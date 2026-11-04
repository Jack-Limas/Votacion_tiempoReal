import { createClient } from 'redis';
import { config } from './config.js';

export const redisClient = createClient({ url: config.redisUrl });

redisClient.on('error', (error) => {
  console.error('Error de conexión con Redis:', error);
});
