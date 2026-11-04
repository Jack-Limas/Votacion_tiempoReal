import { createClient } from 'redis';
import { config } from './env.js';

function createRedisClient(name, options) {
  const client = createClient({
    ...options,
    socket: {
      ...options.socket,
      reconnectStrategy(retries) {
        return Math.min(100 * (2 ** Math.min(retries, 5)), 3000);
      }
    }
  });
  client.on('error', (error) => {
    console.error(`Redis client ${name} error:`, error);
  });
  return client;
}

// Commands fail fast while Redis is unreachable instead of waiting in an
// offline queue, so callers can answer 503 instead of hanging silently.
export const redisClient = createRedisClient('commands', {
  url: config.redisUrl,
  disableOfflineQueue: true
});
export const redisSubClient = createRedisClient('subscriber', { url: config.redisUrl });
export const redisPubClient = createRedisClient('publisher', { url: config.redisUrl });

const redisClients = [redisClient, redisSubClient, redisPubClient];

export async function connectRedisClients() {
  try {
    await Promise.all(redisClients.map((client) => (
      client.isOpen ? Promise.resolve() : client.connect()
    )));
  } catch (error) {
    try {
      await closeRedisClients();
    } catch (closeError) {
      throw new AggregateError(
        [error, closeError],
        'The Redis connection failed and so did its cleanup.'
      );
    }
    throw new Error('Could not connect every Redis client.', { cause: error });
  }
}

export async function closeRedisClients() {
  const results = await Promise.allSettled(
    redisClients
      .filter((client) => client.isOpen)
      .map((client) => client.quit())
  );
  const errors = results
    .filter((result) => result.status === 'rejected')
    .map((result) => result.reason);

  if (errors.length > 0) {
    throw new AggregateError(errors, 'One or more Redis clients could not be closed.');
  }
}
