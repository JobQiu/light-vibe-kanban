//! Bidirectional control protocol communication with Claude Code.
//!
//! Extracted from vibe-kanban: crates/executors/src/executors/claude/protocol.rs
//!
//! Reads stdout line-by-line, handles control requests inline,
//! forwards everything else to the SSE channel.

use std::sync::Arc;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout},
    sync::{mpsc, Mutex},
};

use crate::types::*;

pub const AUTO_APPROVE_CALLBACK_ID: &str = "AUTO_APPROVE_CALLBACK_ID";

const TOOL_DENY_PREFIX: &str = "The user doesn't want to proceed with this tool use. The tool use was rejected (eg. if it was a file edit, the new_string was NOT written to the file). To tell you how to proceed, the user said: ";

/// Handles bidirectional control protocol communication with Claude Code.
#[derive(Clone)]
pub struct ProtocolPeer {
    stdin: Arc<Mutex<ChildStdin>>,
}

impl ProtocolPeer {
    /// Spawn the protocol peer: starts a background task reading stdout.
    ///
    /// - `stdout`: Claude Code's stdout
    /// - `tx`: channel to send events to the SSE stream
    /// - `mode`: execution mode (auto/supervised/plan)
    /// - `approval_tx`: channel for supervised mode — sends approval requests to the browser
    ///
    /// Returns the ProtocolPeer (for sending messages via stdin).
    pub fn spawn(
        stdin: ChildStdin,
        stdout: ChildStdout,
        tx: mpsc::Sender<serde_json::Value>,
        mode: ExecutionMode,
    ) -> Self {
        let peer = Self {
            stdin: Arc::new(Mutex::new(stdin)),
        };

        let reader_peer = peer.clone();
        tokio::spawn(async move {
            if let Err(e) = reader_peer.read_loop(stdout, tx, mode).await {
                eprintln!("Protocol reader loop error: {e}");
            }
        });

        peer
    }

    /// Main read loop — reads stdout line-by-line.
    async fn read_loop(
        &self,
        stdout: ChildStdout,
        tx: mpsc::Sender<serde_json::Value>,
        mode: ExecutionMode,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut reader = BufReader::new(stdout);
        let mut buffer = String::new();

        loop {
            buffer.clear();
            let bytes_read = reader.read_line(&mut buffer).await?;
            if bytes_read == 0 {
                break; // EOF
            }

            let line = buffer.trim();
            if line.is_empty() {
                continue;
            }

            // Filter out claude-code-router service messages
            if line.starts_with("Service not running, starting service")
                || line.contains("claude code router service has been successfully stopped")
            {
                continue;
            }

            // Try to parse as JSON
            let value: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue, // non-JSON line, skip
            };

            // Check if this is a control message that needs handling
            match serde_json::from_str::<CLIMessage>(line) {
                Ok(CLIMessage::ControlRequest {
                    request_id,
                    request,
                }) => {
                    self.handle_control_request(request_id, request, mode, &tx)
                        .await;
                    continue; // don't forward control messages to browser
                }
                Ok(CLIMessage::Result(_)) => {
                    // Forward the result, then break
                    let _ = tx.send(value).await;
                    break;
                }
                Ok(CLIMessage::ControlResponse { .. })
                | Ok(CLIMessage::ControlCancelRequest { .. }) => {
                    continue; // don't forward
                }
                _ => {
                    // Forward to browser
                    let _ = tx.send(value).await;
                }
            }
        }

        Ok(())
    }

    /// Handle a control request from Claude Code.
    async fn handle_control_request(
        &self,
        request_id: String,
        request: ControlRequestType,
        mode: ExecutionMode,
        tx: &mpsc::Sender<serde_json::Value>,
    ) {
        match request {
            ControlRequestType::CanUseTool {
                tool_name,
                input,
                tool_use_id,
                ..
            } => {
                if mode == ExecutionMode::Supervised || mode == ExecutionMode::Plan {
                    // Forward approval request to browser via SSE
                    let approval_event = serde_json::json!({
                        "type": "approval_request",
                        "request_id": request_id,
                        "tool_name": tool_name,
                        "input": input,
                        "tool_use_id": tool_use_id,
                    });
                    let _ = tx.send(approval_event).await;
                    // Response will come from POST /api/respond
                } else {
                    // Auto-approve
                    let result = PermissionResult::Allow {
                        updated_input: input,
                        updated_permissions: None,
                    };
                    if let Err(e) = self
                        .send_control_response(
                            request_id,
                            serde_json::to_value(result).unwrap(),
                        )
                        .await
                    {
                        eprintln!("Failed to send permission result: {e}");
                    }
                }
            }
            ControlRequestType::HookCallback {
                callback_id,
                input: _,
                ..
            } => {
                let response = if callback_id == AUTO_APPROVE_CALLBACK_ID {
                    serde_json::json!({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "allow",
                            "permissionDecisionReason": "Approved by SDK"
                        }
                    })
                } else if mode == ExecutionMode::Supervised || mode == ExecutionMode::Plan {
                    // Forward to can_use_tool flow
                    serde_json::json!({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "ask",
                            "permissionDecisionReason": "Forwarding to approval service"
                        }
                    })
                } else {
                    serde_json::json!({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "allow",
                            "permissionDecisionReason": "Auto-approved by web service"
                        }
                    })
                };

                if let Err(e) = self.send_control_response(request_id, response).await {
                    eprintln!("Failed to send hook callback result: {e}");
                }
            }
        }
    }

    // ─── Stdin write methods ──────────────────────────────────────────────────

    async fn send_json<T: serde::Serialize>(
        &self,
        message: &T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json = serde_json::to_string(message)?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(json.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    pub async fn initialize(
        &self,
        hooks: Option<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&SDKControlRequest::new(
            SDKControlRequestType::Initialize { hooks },
        ))
        .await
    }

    pub async fn set_permission_mode(
        &self,
        mode: PermissionMode,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&SDKControlRequest::new(
            SDKControlRequestType::SetPermissionMode { mode },
        ))
        .await
    }

    pub async fn send_user_message(
        &self,
        content: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&Message::new_user(content)).await
    }

    pub async fn interrupt(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&SDKControlRequest::new(
            SDKControlRequestType::Interrupt {},
        ))
        .await
    }

    /// Send a control response (success) to Claude Code.
    pub async fn send_control_response(
        &self,
        request_id: String,
        response: serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&ControlResponseMessage::new(ControlResponseType::Success {
            request_id,
            response: Some(response),
        }))
        .await
    }

    /// Send a control error response to Claude Code.
    pub async fn send_control_error(
        &self,
        request_id: String,
        error: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(&ControlResponseMessage::new(ControlResponseType::Error {
            request_id,
            error: Some(error),
        }))
        .await
    }

    /// Send a deny response to a CanUseTool request.
    pub async fn deny_tool(
        &self,
        request_id: String,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = PermissionResult::Deny {
            message: format!("{TOOL_DENY_PREFIX}{reason}"),
            interrupt: Some(false),
        };
        self.send_control_response(request_id, serde_json::to_value(result).unwrap())
            .await
    }
}
