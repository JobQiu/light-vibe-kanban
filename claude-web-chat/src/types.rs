//! Protocol types for Claude Code stdin/stdout communication.
//!
//! Extracted from vibe-kanban: crates/executors/src/executors/claude/types.rs

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Messages FROM Claude Code (stdout) ───────────────────────────────────────

/// Top-level message types from CLI stdout.
/// We only fully parse control messages; everything else is forwarded as raw JSON.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CLIMessage {
    ControlRequest {
        request_id: String,
        request: ControlRequestType,
    },
    ControlResponse {
        response: ControlResponseType,
    },
    ControlCancelRequest {
        request_id: String,
    },
    Result(Value),
    #[serde(untagged)]
    Other(Value),
}

/// Control request types that Claude Code sends to us.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subtype", rename_all = "snake_case")]
pub enum ControlRequestType {
    CanUseTool {
        tool_name: String,
        input: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        permission_suggestions: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blocked_paths: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_use_id: Option<String>,
    },
    HookCallback {
        #[serde(rename = "callback_id")]
        callback_id: String,
        input: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_use_id: Option<String>,
    },
}

// ─── Messages TO Claude Code (stdin) ──────────────────────────────────────────

/// SDK control request — sent to Claude Code for initialization, permission, interrupt.
#[derive(Debug, Clone, Serialize)]
pub struct SDKControlRequest {
    #[serde(rename = "type")]
    pub message_type: String,
    pub request_id: String,
    pub request: SDKControlRequestType,
}

impl SDKControlRequest {
    pub fn new(request: SDKControlRequestType) -> Self {
        Self {
            message_type: "control_request".to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            request,
        }
    }
}

/// SDK control request subtypes.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "subtype", rename_all = "snake_case")]
pub enum SDKControlRequestType {
    Initialize {
        #[serde(skip_serializing_if = "Option::is_none")]
        hooks: Option<Value>,
    },
    SetPermissionMode {
        mode: PermissionMode,
    },
    Interrupt {},
}

/// Control response message — sent to Claude Code to answer control requests.
#[derive(Debug, Clone, Serialize)]
pub struct ControlResponseMessage {
    #[serde(rename = "type")]
    pub message_type: String,
    pub response: ControlResponseType,
}

impl ControlResponseMessage {
    pub fn new(response: ControlResponseType) -> Self {
        Self {
            message_type: "control_response".to_string(),
            response,
        }
    }
}

/// Control response types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subtype", rename_all = "snake_case")]
pub enum ControlResponseType {
    Success {
        request_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        response: Option<Value>,
    },
    Error {
        request_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// User message — sent to Claude Code as conversation input.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    User { message: ClaudeUserMessage },
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaudeUserMessage {
    pub role: String,
    pub content: String,
}

impl Message {
    pub fn new_user(content: String) -> Self {
        Self::User {
            message: ClaudeUserMessage {
                role: "user".to_string(),
                content,
            },
        }
    }
}

/// Permission modes (camelCase to match Claude Code CLI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    Plan,
    BypassPermissions,
}

impl std::fmt::Display for PermissionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "default"),
            Self::AcceptEdits => write!(f, "acceptEdits"),
            Self::Plan => write!(f, "plan"),
            Self::BypassPermissions => write!(f, "bypassPermissions"),
        }
    }
}

// ─── Permission result (response to CanUseTool) ──────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "behavior", rename_all = "camelCase")]
pub enum PermissionResult {
    Allow {
        #[serde(rename = "updatedInput")]
        updated_input: Value,
        #[serde(skip_serializing_if = "Option::is_none", rename = "updatedPermissions")]
        updated_permissions: Option<Value>,
    },
    Deny {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        interrupt: Option<bool>,
    },
}

// ─── Execution mode ──────────────────────────────────────────────────────────

/// Maps to vibe-kanban's plan/approvals/auto modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    Auto,
    Supervised,
    Plan,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Auto
    }
}

impl ExecutionMode {
    /// The PermissionMode to set via the control protocol after initialization.
    pub fn permission_mode(&self) -> PermissionMode {
        match self {
            Self::Plan => PermissionMode::Plan,
            Self::Supervised => PermissionMode::Default,
            Self::Auto => PermissionMode::BypassPermissions,
        }
    }

    pub fn needs_stdio_permissions(&self) -> bool {
        matches!(self, Self::Plan | Self::Supervised)
    }
}
