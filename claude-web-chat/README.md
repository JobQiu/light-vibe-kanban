# Claude Code Web Chat

A standalone **Rust** web service that wraps Claude Code CLI using the exact same protocol as vibe-kanban — bidirectional stdin/stdout JSON communication, control protocol, session resumption, and SSE streaming.

## Architecture

```
Browser (Chat UI)
  ↕ SSE + POST (axum)
Rust Server (tokio + axum)
  ↕ stdin/stdout (stream-json protocol)
Claude Code CLI (npx @anthropic-ai/claude-code)
```

### Source layout

```
claude-web-chat/
├── Cargo.toml
├── src/
│   ├── main.rs          # axum server, SSE endpoints, CLI arg builder, hooks
│   ├── types.rs         # Protocol types (extracted from vibe-kanban)
│   └── protocol.rs      # ProtocolPeer — stdin/stdout communication
└── public/
    └── index.html       # Chat UI (SSE client, tool display, approval UI)
```

### Protocol Flow (identical to vibe-kanban)

1. **Spawn** `npx @anthropic-ai/claude-code -p` with `--input-format=stream-json --output-format=stream-json`
2. **Initialize** — send `SDKControlRequest { subtype: "initialize", hooks }` via stdin
3. **Set Permission Mode** — send `SDKControlRequest { subtype: "set_permission_mode", mode }` via stdin
4. **Send User Message** — send `{ type: "user", message: { role: "user", content } }` via stdin
5. **Stream** — read NDJSON from stdout, forward to browser via SSE
6. **Handle Control Requests** — respond to `can_use_tool` / `hook_callback` via stdin
7. **Follow-up** — spawn new process with `--resume <sessionId>`

### Permission Modes

| Mode | Behavior | CLI Flags |
|------|----------|-----------|
| `auto` | Auto-approve all tools | `--disallowedTools=AskUserQuestion` |
| `supervised` | Browser approval for destructive tools | `--permission-prompt-tool=stdio` |
| `plan` | Approve plan before execution | `--permission-prompt-tool=stdio` |

## Prerequisites

- Rust toolchain (cargo)
- Claude Code CLI accessible via `npx @anthropic-ai/claude-code`
- Valid Anthropic API key configured for Claude Code

## Quick Start

```bash
cd claude-web-chat
cargo run
```

Open http://localhost:3456 in your browser.

For release build:

```bash
cargo build --release
./target/release/claude-web-chat
```

## Configuration

| Env Variable | Default | Description |
|--------------|---------|-------------|
| `PORT` | `3456` | Server port |
| `CLAUDE_CWD` | `$HOME` | Default working directory |
| `CLAUDE_CODE_PKG` | `@anthropic-ai/claude-code` | Claude Code package |

## API

### `POST /api/chat` — SSE stream

```json
{
  "prompt": "message",
  "sessionId": "optional — for follow-ups",
  "resumeAt": "optional — message UUID",
  "cwd": "/optional/path",
  "model": "optional-model-id",
  "mode": "auto | supervised | plan"
}
```

### `POST /api/respond` — Approve/deny tool (supervised mode)

```json
{
  "internalId": "from _internal event",
  "requestId": "from approval_request event",
  "approved": true,
  "reason": "optional denial reason"
}
```

### `POST /api/interrupt` — Cancel current turn

```json
{ "internalId": "from _internal event" }
```

## Extracting from the repo

Fully standalone — copy `claude-web-chat/` anywhere:

```bash
cp -r claude-web-chat /wherever
cd /wherever
cargo run
```
