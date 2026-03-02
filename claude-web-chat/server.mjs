import express from 'express';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

/**
 * POST /api/chat
 * Body: { prompt, sessionId?, cwd?, model? }
 * Response: SSE stream of Claude Code JSON events
 *
 * For the first message, omit sessionId.
 * For follow-ups, include the sessionId returned in the 'done' event.
 */
app.post('/api/chat', (req, res) => {
  const { prompt, sessionId, cwd, model, skipPermissions } = req.body;

  if (!prompt || !prompt.trim()) {
    return res.status(400).json({ error: 'prompt is required' });
  }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  // Build CLI arguments
  const args = ['-y', '@anthropic-ai/claude-code'];

  if (sessionId) {
    args.push('--resume', sessionId);
  }

  args.push('-p', prompt);
  args.push('--output-format', 'stream-json');
  args.push('--verbose');

  if (skipPermissions !== false) {
    args.push('--dangerously-skip-permissions');
  }

  if (model) {
    args.push('--model', model);
  }

  const workDir = cwd || process.env.CLAUDE_CWD || process.env.HOME || process.cwd();

  const child = spawn('npx', args, {
    cwd: workDir,
    env: { ...process.env },
    shell: true,
  });

  let buffer = '';
  let extractedSessionId = sessionId || null;

  const sendEvent = (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  child.stdout.on('data', (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete last line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      try {
        const json = JSON.parse(trimmed);

        // Extract session_id from first eligible message
        if (!extractedSessionId && json.session_id) {
          extractedSessionId = json.session_id;
        }

        sendEvent(json);
      } catch {
        // Non-JSON line — skip
      }
    }
  });

  child.stderr.on('data', (chunk) => {
    const text = chunk.toString().trim();
    if (text) {
      sendEvent({ type: 'stderr', content: text });
    }
  });

  child.on('close', (code) => {
    // Flush remaining buffer
    if (buffer.trim()) {
      try {
        const json = JSON.parse(buffer.trim());
        if (!extractedSessionId && json.session_id) {
          extractedSessionId = json.session_id;
        }
        sendEvent(json);
      } catch {
        // ignore
      }
    }

    sendEvent({
      type: 'done',
      session_id: extractedSessionId,
      exit_code: code,
    });
    res.end();
  });

  child.on('error', (err) => {
    sendEvent({ type: 'error', content: err.message });
    res.end();
  });

  // Kill process if client disconnects
  req.on('close', () => {
    if (!child.killed) {
      child.kill('SIGTERM');
    }
  });
});

const PORT = process.env.PORT || 3456;
app.listen(PORT, () => {
  console.log(`\n  Claude Web Chat running at http://localhost:${PORT}\n`);
});
