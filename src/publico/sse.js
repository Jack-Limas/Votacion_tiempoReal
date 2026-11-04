import { createConnection } from './topics.js';

// Transporte público: Server-Sent Events. El fan-out solo ve send(string), así
// que cambiar de transporte no toca topics ni fanout.
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
  res.write(': conectado\n\n');

  const conn = createConnection((frame) => {
    if (!res.destroyed) res.write(frame);
  });
  conn.close = () => res.end();
  return conn;
}
