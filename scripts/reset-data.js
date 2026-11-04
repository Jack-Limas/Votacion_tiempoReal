// Deletes the local environment data (FLUSHDB) and seeds the catalog again.
// Usage: REDIS_URL=redis://localhost:6379 node scripts/reset-data.js
import { createClient } from 'redis';
import { config } from '../src/config/env.js';
import { seedCatalog } from '../src/results/catalog.js';
import { createKeys } from '../src/results/keys.js';

const redis = createClient({ url: config.redisUrl });
await redis.connect();
await redis.flushDb();
await seedCatalog(redis, createKeys(config.redisKeyPrefix).catalog);
await redis.quit();
console.log(`Data reset on ${config.redisUrl}; catalog seeded.`);
