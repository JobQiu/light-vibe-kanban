/**
 * Claude Code Web Chat — Server
 *
 * Mirrors the exact same protocol that vibe-kanban uses to communicate with
 * Claude Code CLI:
 *
 *   1. Spawn `npx @anthropic-ai/claude-code -p` with
 *      --input-format=stream-json --output-format=stream-json
 *   2. Send Initialize control request via stdin
 *   3. Send SetPermissionMode control request via stdin
 *   4. Send user message via stdin
 *   5. Read streaming JSON from stdout, handle ControlRequests inline
 *   6. For follow-ups: spawn new process with --resume <sessionId>
 */

import express from 'express';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import { createInterface } from 'readline';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Claude Code package — pin the same version vibe-kanban uses
const CLAUDE_CODE_PKG = process.env.CLAUDE_CODE_PKG || '@anthropic-ai/claude-code';

const AUTO_APPROVE_CALLBACK_ID = 'AUTO_APPROVE_CALLBACK_ID';

// ─── Stdin protocol helpers ───────────────────────────────────────────────────

function makeControlRequest(request) {
  return {
    type: 'control_request',
    request_id: randomUUID(),
    request,
  };
}

function makeControlResponse(requestId, response) {
  return {
    type: 'control_response',
    response: {
      subtype: 'success',
      request_id: requestId,
      response,
    },
  };
}

function makeUserMessage(content) {
  return {
    type: 'user',
    message: { role: 'user', content },
  };
}

function writeJson(stdin, obj) {
  return new Promise((resolve, reject) => {
    const line = JSON.stringify(obj) + '\n';
    stdin.write(line, (err) => (err ? reject(err) : resolve()));
  });
}

// ─── CLI argument builder (mirrors build_command_builder) ─────────────────────

function buildArgs({ sessionId, resumeAt, model, mode }) {
  const args = ['-y', CLAUDE_CODE_PKG, '-p'];

  // Follow-up: --resume <id> [--resume-session-at <uuid>]
  if (sessionId) {
    args.push('--resume', sessionId);
    if (resumeAt) {
      args.push('--resume-session-at', resumeAt);
    }
  }

  // Permission handling — match vibe-kanban exactly:
  // plan/supervised → --permission-prompt-tool=stdio --permission-mode=bypassPermissions
  // auto           → --disallowedTools=AskUserQuestion
  if (mode === 'plan' || mode === 'supervised') {
    args.push('--permission-prompt-tool=stdio');
    args.push('--permission-mode=bypassPermissions');
  } else {
    args.push('--disallowedTools=AskUserQuestion');
  }

  if (model) {
    args.push('--model', model);
  }

  // Core flags — identical to vibe-kanban
  args.push(
    '--verbose',
    '--output-format=stream-json',
    '--input-format=stream-json',
    '--include-partial-messages',
    '--replay-user-messages',
  );

  return args;
}

// ─── Hooks builder (mirrors get_hooks) ────────────────────────────────────────

function buildHooks(mode) {
  const hooks = {};

  if (mode === 'plan') {
    hooks.PreToolUse = [
      { matcher: '^(ExitPlanMode|AskUserQuestion)$', hookCallbackIds: ['tool_approval'] },
      {
        matcher: '^(?!(ExitPlanMode|AskUserQuestion)$).*',
        hookCallbackIds: [AUTO_APPROVE_CALLBACK_ID],
      },
    ];
  } else if (mode === 'supervised') {
    hooks.PreToolUse = [
      {
        matcher: '^(?!(Glob|Grep|NotebookRead|Read|Task|TodoWrite)$).*',
        hookCallbackIds: ['tool_approval'],
      },
    ];
  } else {
    // auto mode
    hooks.PreToolUse = [
      { matcher: '^AskUserQuestion$', hookCallbackIds: ['tool_approval'] },
    ];
  }

  return hooks;
}

// ─── Permission mode mapping ──────────────────────────────────────────────────

function permissionModeValue(mode) {
  if (mode === 'plan') return 'plan';
  if (mode === 'supervised') return 'default';
  return 'bypassPermissions';
}

// ─── Control request handler (mirrors on_can_use_tool / on_hook_callback) ─────

async function handleControlRequest(stdin, requestId, request, { mode, sendEvent }) {
  if (request.subtype === 'can_use_tool') {
    if (mode === 'supervised' || mode === 'plan') {
      // Forward to browser for approval
      sendEvent({
        type: 'approval_request',
        request_id: requestId,
        tool_name: request.tool_name,
        input: request.input,
        tool_use_id: request.tool_use_id,
      });
      // Response will come from POST /api/respond
      return;
    }
    // Auto-approve
    const response = { behavior: 'allow', updatedInput: request.input };
    await writeJson(stdin, makeControlResponse(requestId, response));
  } else if (request.subtype === 'hook_callback') {
    const callbackId = request.callback_id;

    if (callbackId === AUTO_APPROVE_CALLBACK_ID) {
      await writeJson(
        stdin,
        makeControlResponse(requestId, {
          hookSpecificOutput: {
            hookEventName: 'PreToolUse',
            permissionDecision: 'allow',
            permissionDecisionReason: 'Approved by SDK',
          },
        }),
      );
    } else if (mode === 'supervised' || mode === 'plan') {
      // Forward hook to can_use_tool flow — respond with "ask"
      await writeJson(
        stdin,
        makeControlResponse(requestId, {
          hookSpecificOutput: {
            hookEventName: 'PreToolUse',
            permissionDecision: 'ask',
            permissionDecisionReason: 'Forwarding to approval service',
          },
        }),
      );
    } else {
      await writeJson(
        stdin,
        makeControlResponse(requestId, {
          hookSpecificOutput: {
            hookEventName: 'PreToolUse',
            permissionDecision: 'allow',
            permissionDecisionReason: 'Auto-approved by web service',
          },
        }),
      );
    }
  }
}

// ─── Active sessions (for supervised-mode approval responses) ─────────────────

const activeSessions = new Map(); // internalId → { stdin }

// ─── POST /api/respond — browser sends approval decision ─────────────────────

app.post('/api/respond', async (req, res) => {
  const { internalId, requestId, approved, reason, updatedInput } = req.body;

  const session = activeSessions.get(internalId);
  if (!session) {
    return res.status(404).json({ error: 'session not found' });
  }

  try {
    if (approved) {
      const response = {
        behavior: 'allow',
        updatedInput: updatedInput || {},
      };
      await writeJson(session.stdin, makeControlResponse(requestId, response));
    } else {
      const response = {
        behavior: 'deny',
        message:
          "The user doesn't want to proceed with this tool use. The tool use was rejected (eg. if it was a file edit, the new_string was NOT written to the file). To tell you how to proceed, the user said: " +
          (reason || 'Denied by user'),
        interrupt: false,
      };
      await writeJson(session.stdin, makeControlResponse(requestId, response));
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/interrupt — cancel current turn ───────────────────────────────

app.post('/api/interrupt', async (req, res) => {
  const { internalId } = req.body;
  const session = activeSessions.get(internalId);
  if (!session) {
    return res.status(404).json({ error: 'session not found' });
  }

  try {
    await writeJson(
      session.stdin,
      makeControlRequest({ subtype: 'interrupt' }),
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/chat — main conversation endpoint ─────────────────────────────

app.post('/api/chat', async (req, res) => {
  const {
    prompt,
    sessionId,
    resumeAt,
    cwd,
    model,
    mode = 'auto', // 'auto' | 'plan' | 'supervised'
  } = req.body;

  if (!prompt?.trim()) {
    return res.status(400).json({ error: 'prompt is required' });
  }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const args = buildArgs({ sessionId, resumeAt, model, mode });
  const workDir = cwd || process.env.CLAUDE_CWD || process.env.HOME || process.cwd();

  const child = spawn('npx', args, {
    cwd: workDir,
    env: { ...process.env, NPM_CONFIG_LOGLEVEL: 'error' },
  });

  // Internal tracking
  const internalId = randomUUID();
  let extractedSessionId = sessionId || null;
  const messageIds = [];
  let pendingAssistantUuid = null;
  let buffer = '';

  const sendEvent = (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  // Register session for approval responses
  activeSessions.set(internalId, { stdin: child.stdin, process: child });
  sendEvent({ type: '_internal', internalId });

  // ── Read stdout — line-by-line JSON parsing ──

  child.stdout.on('data', (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete last line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      // Filter out claude-code-router service messages
      if (
        trimmed.startsWith('Service not running, starting service') ||
        trimmed.includes('claude code router service has been successfully stopped')
      ) {
        continue;
      }

      let json;
      try {
        json = JSON.parse(trimmed);
      } catch {
        continue; // non-JSON line
      }

      // Extract session_id (skip system messages, same as vibe-kanban)
      if (!extractedSessionId && json.type !== 'system' && json.session_id) {
        extractedSessionId = json.session_id;
      }

      // Track message UUIDs for --resume-session-at
      if (json.type === 'user' && json.uuid) {
        pendingAssistantUuid = null;
        messageIds.push(json.uuid);
      } else if (json.type === 'assistant' && json.uuid) {
        pendingAssistantUuid = json.uuid;
      } else if (json.type === 'result') {
        if (pendingAssistantUuid) {
          messageIds.push(pendingAssistantUuid);
          pendingAssistantUuid = null;
        }
      }

      // Handle control requests from Claude Code (stdin response needed)
      if (json.type === 'control_request' && json.request) {
        handleControlRequest(child.stdin, json.request_id, json.request, {
          mode,
          sendEvent,
        });
        continue; // don't forward raw control messages to browser
      }

      // Don't forward control responses either
      if (json.type === 'control_response' || json.type === 'control_cancel_request') {
        continue;
      }

      // Forward everything else to the browser
      sendEvent(json);
    }
  });

  // ── Stderr ──

  child.stderr.on('data', (chunk) => {
    const text = chunk.toString().trim();
    if (text) {
      sendEvent({ type: 'stderr', content: text });
    }
  });

  // ── Process exit ──

  child.on('close', (code) => {
    // Flush remaining buffer
    if (buffer.trim()) {
      try {
        const json = JSON.parse(buffer.trim());
        if (!extractedSessionId && json.type !== 'system' && json.session_id) {
          extractedSessionId = json.session_id;
        }
        sendEvent(json);
      } catch {
        // ignore
      }
    }

    activeSessions.delete(internalId);
    sendEvent({
      type: 'done',
      session_id: extractedSessionId,
      message_ids: messageIds,
      exit_code: code,
    });
    res.end();
  });

  child.on('error', (err) => {
    activeSessions.delete(internalId);
    sendEvent({ type: 'error', content: err.message });
    res.end();
  });

  // Kill on disconnect
  req.on('close', () => {
    if (!child.killed) child.kill('SIGTERM');
    activeSessions.delete(internalId);
  });

  // ── Initialize protocol (same sequence as vibe-kanban spawn_internal) ──

  try {
    const hooks = buildHooks(mode);

    // 1. Initialize
    await writeJson(
      child.stdin,
      makeControlRequest({ subtype: 'initialize', hooks }),
    );

    // 2. Set permission mode
    await writeJson(
      child.stdin,
      makeControlRequest({
        subtype: 'set_permission_mode',
        mode: permissionModeValue(mode),
      }),
    );

    // 3. Send user message
    await writeJson(child.stdin, makeUserMessage(prompt));
  } catch (err) {
    sendEvent({ type: 'error', content: 'Failed to initialize: ' + err.message });
    child.kill('SIGTERM');
    activeSessions.delete(internalId);
    res.end();
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3456;
app.listen(PORT, () => {
  console.log(`\n  Claude Web Chat running at http://localhost:${PORT}\n`);
});
