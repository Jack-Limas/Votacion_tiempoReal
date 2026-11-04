import { WebSocketServer } from 'ws';
import { config } from '../config.js';

// Several WebSocket endpoints share one HTTP server. Attaching multiple
// WebSocketServer instances with { server, path } makes each of them answer
// (and abort) upgrades meant for the others, so every endpoint uses
// noServer and a single 'upgrade' listener routes by pathname.
export function createWebSocketServer(httpServer, { routes = [] } = {}) {
  const webSocketServer = new WebSocketServer({
    noServer: true,
    maxPayload: 64 * 1024,
    perMessageDeflate: false
  });

  const table = new Map([
    [config.wsPath, (request, socket, head) => {
      webSocketServer.handleUpgrade(request, socket, head, (ws) => {
        webSocketServer.emit('connection', ws, request);
      });
    }],
    ...routes.map((route) => [route.path, route.handleUpgrade])
  ]);

  httpServer.on('upgrade', (request, socket, head) => {
    socket.on('error', (error) => console.error('Upgrade socket error:', error.message));
    let pathname;
    try {
      ({ pathname } = new URL(request.url, 'http://localhost'));
    } catch {
      pathname = null;
    }
    const handler = pathname && table.get(pathname);
    if (!handler) {
      socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      socket.destroy();
      return;
    }
    handler(request, socket, head);
  });

  webSocketServer.on('connection', (socket) => {
    socket.on('error', (error) => {
      console.error('Error en una conexión WebSocket:', error);
    });
  });

  webSocketServer.on('error', (error) => {
    console.error('Error del servidor WebSocket:', error);
  });

  return webSocketServer;
}
