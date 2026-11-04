import { createServer } from 'node:http';
import { createApp } from './app.js';
import { config } from './config.js';
import { redisClient } from './redis.js';
import { createIdempotencyService } from './services/idempotencyService.js';

async function startServer() {
  await redisClient.connect();
  const app = createApp(createIdempotencyService(redisClient));
  const server = createServer(app);

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.port, resolve);
  });
  console.log(`API lista en el puerto ${config.port}.`);

  const shutdown = async (signal) => {
    console.log(`Recibida señal ${signal}; cerrando el servidor.`);
    server.close(async (error) => {
      if (error) {
        console.error('Error al cerrar el servidor HTTP:', error);
        process.exitCode = 1;
      }
      if (redisClient.isOpen) {
        await redisClient.quit();
      }
    });
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

startServer().catch(async (error) => {
  console.error('No se pudo iniciar el servicio:', error);
  if (redisClient.isOpen) {
    await redisClient.quit();
  }
  process.exitCode = 1;
});
