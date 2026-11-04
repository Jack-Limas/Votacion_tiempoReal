import { WebSocketServer } from 'ws';
import { config } from '../config.js';

export function createWebSocketServer(httpServer) {
  const webSocketServer = new WebSocketServer({
    server: httpServer,
    path: config.wsPath,
    maxPayload: 64 * 1024,
    perMessageDeflate: false
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
