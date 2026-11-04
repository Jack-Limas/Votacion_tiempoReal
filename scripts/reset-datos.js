// Borra los datos del entorno local (FLUSHDB) y vuelve a sembrar el catálogo.
// Uso: REDIS_URL=redis://localhost:6379 node scripts/reset-datos.js
import { createClient } from 'redis';
import { config } from '../src/config.js';
import { sembrarCatalogo } from '../src/publico/catalogo.js';
import { crearClaves } from '../src/publico/claves.js';

const redis = createClient({ url: config.redisUrl });
await redis.connect();
await redis.flushDb();
await sembrarCatalogo(redis, crearClaves(config.redisKeyPrefix).catalogo);
await redis.quit();
console.log(`Datos reiniciados en ${config.redisUrl}; catálogo sembrado.`);
