import { createConnection } from './topics.js';

// Public transport: Server-Sent Events. The fan-out only sees send(string), so
// changing the transport does not touch topics or fanout.
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

  const conn = createConnection((frame) => {
    if (!res.destroyed) res.write(frame);
  }, { bufferedBytes: () => res.writableLength });
  conn.close = () => res.end();
  return conn;
}
