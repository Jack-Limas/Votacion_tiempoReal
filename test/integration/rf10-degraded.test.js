// RF-10, first scenario: Redis stops answering. Nobody may be left in a
// silent state. A full application instance runs in-process against a TCP
// proxy in front of the real Redis/Valkey; the test cuts and restores the proxy.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { createServer as createTcpServer, connect as tcpConnect } from 'node:net';
import { createClient } from 'redis';
import WebSocket from 'ws';
import { createApp } from '../../src/app.js';
import { config as baseConfig } from '../../src/config/env.js';
import { createResults } from '../../src/results/index.js';
import { createResultsRoutes } from '../../src/routes/resultsRoutes.js';
import { createIdempotencyService } from '../../src/services/idempotencyService.js';
import { createWebSocketServer } from '../../src/sockets/index.js';
import { issueToken } from '../../src/verification/auth.js';
import { loadVerificationConfig } from '../../src/verification/config.js';
import { createVerificationModule } from '../../src/verification/index.js';
import { connect as sseConnect } from '../../scripts/lib/sse-client.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const SECRET = 'rf10-secret-rf10-secret-rf10-secret-0000';
const silent = { log() {}, error() {} };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function reachable() {
  const client = createClient({ url: REDIS_URL, socket: { connectTimeout: 1000, reconnectStrategy: false } });
  client.on('error', () => {});
  try {
    await client.connect();
    await client.quit();
    return true;
  } catch {
    return false;
  }
}
const options = { skip: (await reachable()) ? false : `Redis not reachable at ${REDIS_URL}` };

function createProxy() {
  const target = new URL(REDIS_URL);
  const sockets = new Set();
  const server = createTcpServer((client) => {
    const upstream = tcpConnect(Number(target.port || 6379), target.hostname);
    for (const s of [client, upstream]) {
      sockets.add(s);
      s.on('error', () => {});
      s.on('close', () => { client.destroy(); upstream.destroy(); });
    }
    client.pipe(upstream).pipe(client);
  });
  let port;
  return {
    async listen() {
      await new Promise((resolve) => server.listen(port ?? 0, '127.0.0.1', resolve));
      port = server.address().port;
      return `redis://127.0.0.1:${port}`;
    },
    async cut() {
      await new Promise((resolve) => {
        server.close(resolve);
        for (const s of sockets) s.destroy();
      });
    }
  };
}

function waitForMessage(messages, predicate, timeoutMs = 6000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      const found = messages.find(predicate);
      if (found) {
        clearInterval(timer);
        resolve(found);
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error(`timeout; got ${JSON.stringify(messages.slice(-5))}`));
      }
    }, 50);
  });
}

const acta = (n, overrides = {}) => ({
  acta: `E14-1-${n}-03`, mesa: n, puesto: 1, municipio: 1, version: 1, jurado: `JUR-${n}`,
  ts_cierre: 1, votos: { C1: 10 }, sufragantes: 10, hash_foto: 'f', idem: randomUUID(), ...overrides
});

test('RF-10: jurados, public and auditors all see the outage, and everything recovers', options, async (t) => {
  const proxy = createProxy();
  const url = await proxy.listen();
  const id = randomUUID().slice(0, 8);
  const config = { ...baseConfig, redisKeyPrefix: `test:{rf10-${id}}`, instanceId: 'rf10', flushMs: 100, metricsTtlSeconds: 5 };

  const clientOptions = { url, socket: { reconnectStrategy: () => 200, connectTimeout: 1000 } };
  const redis = createClient({ ...clientOptions, disableOfflineQueue: true });
  const subscriber = createClient(clientOptions);
  for (const c of [redis, subscriber]) c.on('error', () => {});
  await redis.connect();
  await subscriber.connect();

  const service = createIdempotencyService(redis, { keyPrefix: config.redisKeyPrefix, ttlSeconds: 600 });
  const results = createResults({ redis, config, logger: silent });
  const verification = createVerificationModule({
    redis,
    subscriber,
    logger: silent,
    config: loadVerificationConfig({
      keyPrefix: `test:{rf10v-${id}}`, streamKey: `${config.redisKeyPrefix}:verification`,
      tokenSecret: SECRET, instanceId: 'rf10', healthIntervalMs: 200, heartbeatMs: 200
    })
  });
  const app = createApp(service, { routers: [verification.router, createResultsRoutes(results)] });
  const server = createServer(app);
  createWebSocketServer(server, { routes: [verification.upgradeRoute] });
  await results.start();
  await verification.start();
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;

  const publicMessages = [];
  const sse = await sseConnect(base, [{ topic: 'dep' }], { onMessage: (m) => publicMessages.push(m) });
  const consoleMessages = [];
  const auditor = new WebSocket(`${base.replace('http', 'ws')}/ws/verification`,
    ['verification.v1', `auth.${issueToken(SECRET, { sub: 'AUD-1', role: 'auditor' }, 600)}`]);
  auditor.on('message', (raw) => consoleMessages.push(JSON.parse(raw)));
  await new Promise((resolve) => auditor.once('open', resolve));

  t.after(async () => {
    sse.close();
    auditor.terminate();
    await verification.stop();
    await results.stop();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for (const c of [redis, subscriber]) await c.disconnect().catch(() => {});
    await proxy.cut().catch(() => {});
    const admin = createClient({ url: REDIS_URL });
    await admin.connect();
    for (const prefix of [config.redisKeyPrefix, `test:{rf10v-${id}}`]) {
      for await (const key of admin.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) await admin.del(key);
    }
    await admin.quit();
  });

  const post = (body) => fetch(`${base}/api/actas`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  assert.equal((await post(acta(1))).status, 201);
  await waitForMessage(publicMessages, (m) => m.t === 'delta' || m.t === 'snapshot');

  // ---- Redis stops answering ----
  await proxy.cut();
  const started = Date.now();
  const pending = acta(2);
  const outage = await post(pending);
  const elapsed = Date.now() - started;
  assert.equal(outage.status, 503, 'jurado gets an explicit, retryable failure');
  assert.equal((await outage.json()).retryable, true);
  assert.ok(elapsed < 1500, `fails fast (${elapsed} ms), no silent hang`);

  const publicDown = await waitForMessage(publicMessages, (m) => m.t === 'status' && m.redis === 'down');
  assert.ok(publicDown.since, 'public page is told its data is stale');
  await waitForMessage(consoleMessages, (m) => m.type === 'system' && m.redis === 'down');
  const health = await fetch(`${base}/api/verification/health`);
  assert.equal(health.status, 503);
  assert.equal((await fetch(`${base}/api/results/dep`)).status, 503, 'polling answers 503, not stale 200');
  console.log(`  outage: jury 503 in ${elapsed} ms; public status=down; auditor console degraded`);

  // ---- Redis is back ----
  await proxy.listen();
  await waitForMessage(publicMessages, (m) => m.t === 'status' && m.redis === 'up', 8000);
  await waitForMessage(consoleMessages, (m) => m.type === 'system' && m.redis === 'up', 8000);
  // The jury outbox retries the same acta (same idem): it is accepted once.
  await sleep(300);
  assert.equal((await post(pending)).status, 201);
  assert.equal((await post(pending)).status, 200, 'the retry after that is a duplicate');
  await waitForMessage(publicMessages, (m) => m.t === 'delta' && m.changes['v:C1'] === 20, 8000);
  console.log('  recovery: status=up on both consoles, retried acta applied once, public updated');
});
