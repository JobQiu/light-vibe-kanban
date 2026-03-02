# Claude Code Web Chat

A standalone web service that wraps Claude Code CLI using the **exact same protocol as vibe-kanban** — bidirectional stdin/stdout JSON communication, control protocol, session resumption, and streaming.

## Architecture

```
Browser (Chat UI)
  ↕ SSE (Server-Sent Events) + POST for approvals
Express Server
  ↕ stdin/stdout (stream-json protocol)
Claude Code CLI (npx @anthropic-ai/claude-code)
```

### Protocol Flow (identical to vibe-kanban)

1. **Spawn** `npx @anthropic-ai/claude-code -p` with `--input-format=stream-json --output-format=stream-json`
2. **Initialize** — send `SDKControlRequest { subtype: "initialize", hooks }` via stdin
3. **Set Permission Mode** — send `SDKControlRequest { subtype: "set_permission_mode", mode }` via stdin
4. **Send User Message** — send `{ type: "user", message: { role: "user", content } }` via stdin
5. **Stream** — read NDJSON from stdout, forward to browser via SSE
6. **Handle Control Requests** — when Claude Code asks for tool approval (`can_use_tool`) or hook callbacks, respond via stdin
7. **Follow-up** — spawn new process with `--resume <sessionId>`, repeat steps 2-6

### Permission Modes

| Mode | Behavior | CLI Flags |
|------|----------|-----------|
| `auto` | Auto-approve all tools | `--disallowedTools=AskUserQuestion` |
| `supervised` | Browser approval for destructive tools | `--permission-prompt-tool=stdio` |
| `plan` | Approve plan before execution | `--permission-prompt-tool=stdio` |

## Prerequisites

- Node.js >= 18
- Claude Code CLI accessible via `npx @anthropic-ai/claude-code`
- Valid Anthropic API key configured for Claude Code

## Quick Start

```bash
cd claude-web-chat
npm install
npm start
```

Open http://localhost:3456 in your browser.

## Configuration

| Env Variable | Default | Description |
|--------------|---------|-------------|
| `PORT` | `3456` | Server port |
| `CLAUDE_CWD` | `$HOME` | Default working directory |
| `CLAUDE_CODE_PKG` | `@anthropic-ai/claude-code` | Claude Code package (pin version if needed) |

## API

### `POST /api/chat` — Send message (SSE response)

```json
{
  "prompt": "your message",
  "sessionId": "optional — from previous 'done' event",
  "resumeAt": "optional — message UUID for --resume-session-at",
  "cwd": "/optional/working/directory",
  "model": "optional-model-id",
  "mode": "auto | supervised | plan"
}
```

Response: SSE stream with Claude Code JSON events (`system`, `assistant`, `tool_use`, `tool_result`, `result`, `done`, etc.)

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
npm install
npm start
```
