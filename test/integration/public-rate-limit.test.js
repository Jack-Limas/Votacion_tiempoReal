// Rate limit of new public connections per client IP, against a real
// Redis/Valkey, for both public transports.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import express from 'express';
import { createClient } from 'redis';
import WebSocket, { WebSocketServer } from 'ws';
import { config as baseConfig } from '../../src/config/env.js';
import { createResults } from '../../src/results/index.js';
import { createResultsRoutes } from '../../src/routes/resultsRoutes.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const silent = { log() {}, error() {} };

async function connectOrNull() {
  const client = createClient({ url: REDIS_URL, socket: { connectTimeout: 1000, reconnectStrategy: false } });
  client.on('error', () => {});
  try {
    await client.connect();
    return client;
  } catch {
    return null;
  }
}
const probe = await connectOrNull();
await probe?.quit();
const options = { skip: probe ? false : `Redis not reachable at ${REDIS_URL}` };

test('new public connections are limited per IP with a clear answer on SSE and WebSocket', options, async (t) => {
  const redis = await connectOrNull();
  const prefix = `test:{rl-${randomUUID().slice(0, 8)}}`;
  const results = createResults({
    redis,
    logger: silent,
    config: { ...baseConfig, redisKeyPrefix: prefix, instanceId: 'rl', rateLimitPublicConnects: 3, rateLimitWindowMs: 10_000, heartbeatMs: 60_000 }
  });
  await results.start();
  const app = express();
  app.use(createResultsRoutes(results));
  const server = createServer(app);
  const wss = new WebSocketServer({ server });
  wss.on('connection', (ws, req) => results.handleWebSocket(ws, req));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const open = [];
  t.after(async () => {
    for (const c of open) c.destroy?.() ?? c.terminate?.();
    await results.stop();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for await (const key of redis.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) await redis.del(key);
    await redis.quit();
  });

  const sse = async () => {
    const controller = new AbortController();
    const response = await fetch(`${base}/stream?sub=dep`, { signal: controller.signal });
    open.push({ destroy: () => controller.abort() });
    return response;
  };
  for (let i = 0; i < 3; i += 1) assert.equal((await sse()).status, 200);
  const limited = await sse();
  assert.equal(limited.status, 429);
  assert.ok(Number(limited.headers.get('retry-after')) >= 1);
  assert.equal((await limited.json()).status, 'rate_limited');

  // Same IP, same window: the WebSocket transport closes with 4429.
  const ws = new WebSocket(`${base.replace('http', 'ws')}/?sub=dep`);
  open.push(ws);
  const code = await new Promise((resolve) => ws.on('close', (c) => resolve(c)));
  assert.equal(code, 4429);
});
