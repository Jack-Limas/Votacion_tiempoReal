import { createConnection } from './topics.js';

const SLOW_CLOSE_GRACE_MS = 5_000;
const HEARTBEAT_FRAME = 'data: {"t":"hb"}\n\n';

// Public transport: Server-Sent Events. The fan-out only sees the connection
// interface (topics.js), so the transport never leaks into topics or fanout.
export function openSseConnection(req, res, instanceId) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
    'X-Instance-Id': instanceId
  });
  req.socket.setNoDelay(true);
  req.socket.setKeepAlive(true);
  res.write(': connected\n\n');

  const write = (text) => {
    if (!res.destroyed && !res.writableEnded) res.write(text);
  };
  const conn = createConnection((frame) => write(frame.sse), {
    transport: 'sse',
    bufferedBytes: () => res.writableLength,
    // A data frame, not an SSE comment: EventSource hides comments from the
    // page, and the client watchdog needs to see traffic to detect a dead
    // instance whose socket never closes.
    heartbeat: () => write(HEARTBEAT_FRAME),
    close: () => res.end(),
    disconnectSlow: () => {
      // The signal is queued behind the backlog; if the client never drains
      // it, the socket is destroyed so its memory is released anyway.
      write(`data: ${JSON.stringify({ t: 'reconnect', reason: 'slow_consumer' })}\n\n`);
      res.end();
      setTimeout(() => res.destroy(), SLOW_CLOSE_GRACE_MS).unref();
    }
  });
  res.on('close', () => {
    conn.closed = true;
  });
  return conn;
}
