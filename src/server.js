import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';
import { createApp } from './app.js';
import { config } from './config/env.js';
import {
  closeRedisClients,
  connectRedisClients,
  redisClient,
  redisSubClient
} from './config/redis.js';
import { createIdempotencyService } from './services/idempotencyService.js';
import { createPublico } from './publico/index.js';
import { createPublicoRoutes } from './routes/publicoRoutes.js';
import { createWebSocketServer } from './sockets/index.js';
import { createVerificationModule } from './verification/index.js';

export const verification = createVerificationModule({
  redis: redisClient,
  subscriber: redisSubClient
});
export const publico = createPublico({ redis: redisClient });
export const app = createApp(createIdempotencyService(redisClient), {
  routers: [verification.router, createPublicoRoutes(publico)],
  actaMiddleware: [verification.actaRateLimiter]
});
export const server = createServer(app);
export const webSocketServer = createWebSocketServer(server, { routes: [verification.upgradeRoute] });

export async function startServer() {
  await connectRedisClients();
  await verification.start();
  await publico.start();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.port, config.host, resolve);
  });
  console.log(`API, WebSocket y SSE listos en ${config.host}:${config.port} (instancia ${config.instanceId}).`);
}

export async function stopServer() {
  const stopServices = await Promise.allSettled([
    verification.stop(),
    publico.stop()
  ]);

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

  const closeResults = await Promise.allSettled([
    closeHttpServer,
    closeWebSocketServer,
    closeRedisClients()
  ]);
  const errors = [...stopServices, ...closeResults]
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