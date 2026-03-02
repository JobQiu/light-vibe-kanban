mod protocol;
mod types;

use std::{collections::HashMap, convert::Infallible, process::Stdio, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    response::sse::{Event, Sse},
    routing::post,
};
use futures::stream::Stream;
use tokio_stream::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::{process::Command, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::services::ServeDir;

use crate::{
    protocol::{ProtocolPeer, AUTO_APPROVE_CALLBACK_ID},
    types::*,
};

/// Claude Code package — override with CLAUDE_CODE_PKG env var.
fn claude_code_pkg() -> String {
    std::env::var("CLAUDE_CODE_PKG").unwrap_or_else(|_| "@anthropic-ai/claude-code".to_string())
}

// ─── Shared state ─────────────────────────────────────────────────────────────

/// Active sessions — stores ProtocolPeer for approval responses / interrupt.
type Sessions = Arc<tokio::sync::Mutex<HashMap<String, ProtocolPeer>>>;

#[derive(Clone)]
struct AppState {
    sessions: Sessions,
}

// ─── CLI argument builder (mirrors build_command_builder) ─────────────────────

fn build_args(
    session_id: Option<&str>,
    resume_at: Option<&str>,
    model: Option<&str>,
    mode: ExecutionMode,
) -> Vec<String> {
    let mut args = vec![
        "-y".to_string(),
        claude_code_pkg(),
        "-p".to_string(),
    ];

    // Follow-up: --resume <id> [--resume-session-at <uuid>]
    if let Some(sid) = session_id {
        args.push("--resume".to_string());
        args.push(sid.to_string());
        if let Some(uuid) = resume_at {
            args.push("--resume-session-at".to_string());
            args.push(uuid.to_string());
        }
    }

    // Permission handling — match vibe-kanban exactly
    if mode.needs_stdio_permissions() {
        args.push("--permission-prompt-tool=stdio".to_string());
        args.push("--permission-mode=bypassPermissions".to_string());
    } else {
        args.push("--disallowedTools=AskUserQuestion".to_string());
    }

    if let Some(m) = model {
        args.push("--model".to_string());
        args.push(m.to_string());
    }

    // Core flags — identical to vibe-kanban
    args.extend([
        "--verbose".to_string(),
        "--output-format=stream-json".to_string(),
        "--input-format=stream-json".to_string(),
        "--include-partial-messages".to_string(),
        "--replay-user-messages".to_string(),
    ]);

    args
}

// ─── Hooks builder (mirrors get_hooks) ────────────────────────────────────────

fn build_hooks(mode: ExecutionMode) -> Value {
    let pre_tool_use = match mode {
        ExecutionMode::Plan => serde_json::json!([
            {
                "matcher": "^(ExitPlanMode|AskUserQuestion)$",
                "hookCallbackIds": ["tool_approval"]
            },
            {
                "matcher": "^(?!(ExitPlanMode|AskUserQuestion)$).*",
                "hookCallbackIds": [AUTO_APPROVE_CALLBACK_ID]
            }
        ]),
        ExecutionMode::Supervised => serde_json::json!([
            {
                "matcher": "^(?!(Glob|Grep|NotebookRead|Read|Task|TodoWrite)$).*",
                "hookCallbackIds": ["tool_approval"]
            }
        ]),
        ExecutionMode::Auto => serde_json::json!([
            {
                "matcher": "^AskUserQuestion$",
                "hookCallbackIds": ["tool_approval"]
            }
        ]),
    };

    serde_json::json!({ "PreToolUse": pre_tool_use })
}

// ─── POST /api/chat ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ChatRequest {
    prompt: String,
    #[serde(default, rename = "sessionId")]
    session_id: Option<String>,
    #[serde(default, rename = "resumeAt")]
    resume_at: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    mode: ExecutionMode,
}

async fn chat_handler(
    State(state): State<AppState>,
    Json(req): Json<ChatRequest>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel::<serde_json::Value>(256);
    let internal_id = uuid::Uuid::new_v4().to_string();

    // Send internal ID to browser
    let _ = tx
        .send(serde_json::json!({ "type": "_internal", "internalId": internal_id }))
        .await;

    let sessions = state.sessions.clone();
    let internal_id_clone = internal_id.clone();

    tokio::spawn(async move {
        if let Err(e) = run_claude_session(
            &req,
            tx.clone(),
            sessions.clone(),
            &internal_id_clone,
        )
        .await
        {
            let _ = tx
                .send(serde_json::json!({ "type": "error", "content": e.to_string() }))
                .await;
        }

        // Cleanup
        sessions.lock().await.remove(&internal_id_clone);
    });

    let stream = ReceiverStream::new(rx).map(|value| {
        Ok(Event::default().data(serde_json::to_string(&value).unwrap_or_default()))
    });

    Sse::new(stream)
}

async fn run_claude_session(
    req: &ChatRequest,
    tx: mpsc::Sender<serde_json::Value>,
    sessions: Sessions,
    internal_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = build_args(
        req.session_id.as_deref(),
        req.resume_at.as_deref(),
        req.model.as_deref(),
        req.mode,
    );

    let work_dir = req
        .cwd
        .clone()
        .or_else(|| std::env::var("CLAUDE_CWD").ok())
        .or_else(|| std::env::var("HOME").ok())
        .unwrap_or_else(|| ".".to_string());

    let mut child = Command::new("npx")
        .args(&args)
        .current_dir(&work_dir)
        .env("NPM_CONFIG_LOGLEVEL", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let child_stdin = child.stdin.take().ok_or("missing stdin")?;
    let child_stdout = child.stdout.take().ok_or("missing stdout")?;

    // Spawn protocol peer (starts reading stdout in background)
    let peer = ProtocolPeer::spawn(child_stdin, child_stdout, tx.clone(), req.mode);

    // Store peer for approval responses / interrupt
    sessions
        .lock()
        .await
        .insert(internal_id.to_string(), peer.clone());

    // ── Initialize protocol (same sequence as vibe-kanban spawn_internal) ──

    // 1. Initialize with hooks
    let hooks = build_hooks(req.mode);
    peer.initialize(Some(hooks)).await?;

    // 2. Set permission mode
    peer.set_permission_mode(req.mode.permission_mode()).await?;

    // 3. Send user message
    peer.send_user_message(req.prompt.clone()).await?;

    // Wait for process to exit
    let status = child.wait().await?;

    // Send session tracking info
    let _ = tx
        .send(serde_json::json!({
            "type": "done",
            "exit_code": status.code(),
        }))
        .await;

    Ok(())
}

// ─── POST /api/respond — browser sends approval decision ─────────────────────

#[derive(Deserialize)]
struct RespondRequest {
    #[serde(rename = "internalId")]
    internal_id: String,
    #[serde(rename = "requestId")]
    request_id: String,
    approved: bool,
    #[serde(default)]
    reason: Option<String>,
}

async fn respond_handler(
    State(state): State<AppState>,
    Json(req): Json<RespondRequest>,
) -> Json<Value> {
    let sessions = state.sessions.lock().await;
    let Some(peer) = sessions.get(&req.internal_id) else {
        return Json(serde_json::json!({ "error": "session not found" }));
    };

    let result = if req.approved {
        let result = PermissionResult::Allow {
            updated_input: serde_json::json!({}),
            updated_permissions: None,
        };
        peer.send_control_response(
            req.request_id,
            serde_json::to_value(result).unwrap(),
        )
        .await
    } else {
        let reason = req.reason.as_deref().unwrap_or("Denied by user");
        peer.deny_tool(req.request_id, reason).await
    };

    match result {
        Ok(()) => Json(serde_json::json!({ "ok": true })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

// ─── POST /api/interrupt — cancel current turn ───────────────────────────────

#[derive(Deserialize)]
struct InterruptRequest {
    #[serde(rename = "internalId")]
    internal_id: String,
}

async fn interrupt_handler(
    State(state): State<AppState>,
    Json(req): Json<InterruptRequest>,
) -> Json<Value> {
    let sessions = state.sessions.lock().await;
    let Some(peer) = sessions.get(&req.internal_id) else {
        return Json(serde_json::json!({ "error": "session not found" }));
    };

    match peer.interrupt().await {
        Ok(()) => Json(serde_json::json!({ "ok": true })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let state = AppState {
        sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3456);

    let app = Router::new()
        .route("/api/chat", post(chat_handler))
        .route("/api/respond", post(respond_handler))
        .route("/api/interrupt", post(interrupt_handler))
        .fallback_service(ServeDir::new("public"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind");

    println!("\n  Claude Web Chat running at http://localhost:{port}\n");

    axum::serve(listener, app).await.expect("server error");
}
