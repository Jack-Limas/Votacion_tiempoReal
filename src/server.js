import { createServer } from 'node:http';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createApp } from './app.js';
import { config } from './config/env.js';
import {
  closeRedisClients,
  connectRedisClients,
  redisClient,
  redisSubClient
} from './config/redis.js';
import { createIdempotencyService } from './services/idempotencyService.js';
import { createDecisionApplier } from './services/decisionApplier.js';
import { createResults } from './results/index.js';
import { createResultsRoutes } from './routes/resultsRoutes.js';
import { createWebSocketServer } from './sockets/index.js';
import { createVerificationModule } from './verification/index.js';

export const verification = createVerificationModule({
  redis: redisClient,
  subscriber: redisSubClient
});
export const results = createResults({ redis: redisClient });
export const idempotencyService = createIdempotencyService(redisClient);
const decisionClient = redisClient.duplicate();
decisionClient.on('error', (error) => console.error('Decision applier Redis client error:', error.message));
export const decisionApplier = createDecisionApplier({
  client: decisionClient,
  service: idempotencyService,
  streamKey: `${verification.config.keyPrefix}:resolutions`,
  consumer: config.instanceId
});
export const app = createApp(idempotencyService, {
  routers: [verification.router, createResultsRoutes(results)],
  actaMiddleware: [verification.actaRateLimiter],
  publicDir: fileURLToPath(new URL('../public/', import.meta.url))
});
export const server = createServer(app);
export const webSocketServer = createWebSocketServer(server, { routes: [verification.upgradeRoute] });

export async function startServer() {
  await connectRedisClients();
  await verification.start();
  await results.start();
  decisionApplier.start();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.port, config.host, resolve);
  });
  console.log(`API, WebSocket and SSE ready on ${config.host}:${config.port} (instance ${config.instanceId}).`);
}

export async function stopServer() {
  const stopServices = await Promise.allSettled([
    verification.stop(),
    results.stop(),
    decisionApplier.stop()
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
    throw new AggregateError(errors, 'Errors while stopping the server.');
  }
}

async function run() {
  await startServer();

  const shutdown = async (signal) => {
    console.log(`Received ${signal}; shutting down.`);
    try {
      await stopServer();
    } catch (error) {
      console.error('Error during shutdown:', error);
      process.exitCode = 1;
    }
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(async (error) => {
    console.error('Could not start the service:', error);
    try {
      await stopServer();
    } catch (shutdownError) {
      console.error('Shutdown also failed:', shutdownError);
    }
    process.exitCode = 1;
  });
}