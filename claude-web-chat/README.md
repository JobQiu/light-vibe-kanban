# Claude Code Web Chat

A minimal standalone web service that wraps Claude Code CLI, giving you a continuous conversation experience in the browser — just like using Claude Code in the terminal.

## How it works

```
Browser (Chat UI)
  ↕ SSE streaming
Express Server
  ↕ stdin/stdout (stream-json)
Claude Code CLI (npx @anthropic-ai/claude-code)
```

Each conversation maintains a `session_id`. Follow-up messages use `--resume` to continue the same Claude Code session, giving you true continuous conversation.

## Prerequisites

- Node.js >= 18
- Claude Code CLI accessible via `npx @anthropic-ai/claude-code`
- Valid Anthropic API key (configured in your environment for Claude Code)

## Quick Start

```bash
cd claude-web-chat
npm install
npm start
```

Open http://localhost:3456 in your browser.

## Configuration

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3456` | Server port |
| `CLAUDE_CWD` | `$HOME` | Default working directory for Claude Code |

**UI options:**

- **Working Dir** — the project directory Claude Code operates in
- **Model** — choose between Sonnet, Opus, or Haiku

## API

### `POST /api/chat`

Send a message and receive a streaming response (SSE).

**Request body:**
```json
{
  "prompt": "your message",
  "sessionId": "optional — from previous response for follow-ups",
  "cwd": "/optional/working/directory",
  "model": "optional-model-id"
}
```

**Response:** Server-Sent Events stream. Each event is `data: <json>\n\n` with these types:

| Type | Description |
|------|-------------|
| `system` | Init info (session_id, model) |
| `assistant` | Assistant message with content blocks |
| `tool_use` | Tool invocation (Read, Write, Bash, etc.) |
| `tool_result` | Tool execution result |
| `result` | Turn complete |
| `done` | Process finished, includes `session_id` for follow-ups |

## Extracting from the repo

This service is fully standalone. Just copy the `claude-web-chat/` directory:

```bash
cp -r claude-web-chat /wherever/you/want
cd /wherever/you/want
npm install
npm start
```

No dependency on vibe-kanban or any other part of this repo.
