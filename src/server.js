import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';
import { createApp } from './app.js';
import { config } from './config.js';
import {
  closeRedisClients,
  connectRedisClients,
  redisClient
} from './config/redis.js';
import { createIdempotencyService } from './services/idempotencyService.js';
import { createWebSocketServer } from './sockets/index.js';

export const app = createApp(createIdempotencyService(redisClient));
export const server = createServer(app);
export const webSocketServer = createWebSocketServer(server);

export async function startServer() {
  await connectRedisClients();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.port, config.host, resolve);
  });
  console.log(`API y WebSocket listos en ${config.host}:${config.port}.`);
}

export async function stopServer() {
  for (const socket of webSocketServer.clients) {
    socket.terminate();
  }

  const closeHttpServer = server.listening
    ? new Promise((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      })
    : Promise.resolve();
  const closeWebSocketServer = new Promise((resolve, reject) => {
    webSocketServer.close((error) => error ? reject(error) : resolve());
  });

  const results = await Promise.allSettled([
    closeHttpServer,
    closeWebSocketServer,
    closeRedisClients()
  ]);
  const errors = results
    .filter((result) => result.status === 'rejected')
    .map((result) => result.reason);

  if (errors.length > 0) {
    throw new AggregateError(errors, 'Ocurrieron errores al detener el servidor.');
  }
}

async function run() {
  await startServer();

  const shutdown = async (signal) => {
    console.log(`Recibida señal ${signal}; cerrando el servidor.`);
    try {
      await stopServer();
    } catch (error) {
      console.error('Error durante el cierre:', error);
      process.exitCode = 1;
    }
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(async (error) => {
    console.error('No se pudo iniciar el servicio:', error);
    try {
      await stopServer();
    } catch (shutdownError) {
      console.error('También falló el cierre del servicio:', shutdownError);
    }
    process.exitCode = 1;
  });
}
