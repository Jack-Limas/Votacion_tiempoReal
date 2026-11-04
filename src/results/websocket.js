import { createConnection } from './topics.js';

// Close code that tells the client to reconnect with its cursors (RF-6/RF-9).
export const SLOW_CONSUMER_CLOSE = 4008;

// Public results over WebSocket. Same frames as SSE (plain JSON), same
// connection interface, so topics, fan-out and back-pressure are shared.
export function openWebSocketConnection(ws, instanceId) {
  ws.send(JSON.stringify({ t: 'hello', instance: instanceId }));
  const conn = createConnection((frame) => {
    if (ws.readyState === ws.OPEN) ws.send(frame.json);
  }, {
    transport: 'ws',
    bufferedBytes: () => ws.bufferedAmount,
    heartbeat: () => {
      if (ws.readyState === ws.OPEN) ws.ping();
    },
    close: () => ws.close(1001, 'server shutting down'),
    disconnectSlow: () => {
      ws.close(SLOW_CONSUMER_CLOSE, 'slow consumer: reconnect with your cursors');
      setTimeout(() => ws.terminate(), 5_000).unref();
    }
  });
  return conn;
}
