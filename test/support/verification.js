// Shared helpers for the verification tests (no tests in this file).
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createClient } from 'redis';
import WebSocket from 'ws';
import { createApp } from '../../src/app.js';
import { createWebSocketServer } from '../../src/sockets/index.js';
import { createVerificationModule } from '../../src/verification/index.js';
import { loadVerificationConfig } from '../../src/verification/config.js';
import { createVerificationStore } from '../../src/verification/store.js';
import { issueToken } from '../../src/verification/auth.js';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
export const SECRET = 'test-secret-test-secret-test-secret-0123456789';
export const silentLogger = { log() {}, error() {} };

export async function redisAvailable() {
  const client = createClient({ url: REDIS_URL, socket: { connectTimeout: 1000, reconnectStrategy: false } });
  client.on('error', () => {});
  try {
    await client.connect();
    await client.ping();
    await client.quit();
    return true;
  } catch {
    return false;
  }
}

export function uniquePrefix() {
  return `test:{verif-${randomUUID().slice(0, 8)}}`;
}

export async function connectClient(url = REDIS_URL, options = {}) {
  const client = createClient({ url, disableOfflineQueue: true, ...options });
  client.on('error', () => {});
  await client.connect();
  return client;
}

export async function cleanup(client, prefix) {
  for await (const key of client.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) {
    await client.del(key);
  }
}

export function testConfig(prefix, overrides = {}) {
  return loadVerificationConfig({
    keyPrefix: prefix,
    streamKey: `${prefix}:incoming`,
    tokenSecret: SECRET,
    auditorAccessCode: 'audit-code',
    coordinatorAccessCode: 'coord-code',
    leaseTtlMs: 600,
    idleLimitMs: 60_000,
    heartbeatMs: 150,
    presenceTtlMs: 600,
    sweepIntervalMs: 100,
    healthIntervalMs: 200,
    ...overrides
  });
}

export function makeStore(client, prefix, overrides = {}) {
  return createVerificationStore(client, { keyPrefix: prefix, leaseTtlMs: 400, presenceTtlMs: 400, ...overrides });
}

export function sampleActa(n, overrides = {}) {
  return {
    acta: `E14-176-${String(n).padStart(4, '0')}-03`,
    mesa: 4000 + n,
    puesto: 176,
    municipio: 11,
    version: 1,
    jurado: `JUR-${n}`,
    votos: { C1: 100, C2: 50, NULO: 2 },
    sufragantes: 160,
    hash_foto: 'abc',
    idem: `idem-${n}`,
    ...overrides
  };
}

export const tokenFor = (sub, role = 'auditor', name = sub) => issueToken(SECRET, { sub, role, name }, 3600);

// Starts one full application instance (HTTP + /ws/verification) against Redis.
export async function startInstance({ prefix, instanceId, redisUrl = REDIS_URL, overrides = {} }) {
  const redis = await connectClient(redisUrl, { socket: { reconnectStrategy: () => 100 } });
  const subscriber = createClient({ url: redisUrl, socket: { reconnectStrategy: () => 100 } });
  subscriber.on('error', () => {});
  await subscriber.connect();
  const config = testConfig(prefix, { instanceId, ...overrides });
  const verification = createVerificationModule({ redis, subscriber, config, logger: silentLogger });
  const app = createApp({ async registerActa() { return { status: 'accepted', accepted: true, duplicate: false }; } }, {
    routers: [verification.router],
    actaMiddleware: [verification.actaRateLimiter]
  });
  const server = createServer(app);
  createWebSocketServer(server, { routes: [verification.upgradeRoute] });
  await verification.start();
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    verification,
    redis,
    url: `http://127.0.0.1:${port}`,
    wsUrl: `ws://127.0.0.1:${port}${config.wsPath}`,
    async stop() {
      await verification.stop();
      server.closeAllConnections?.();
      await new Promise((resolve) => server.close(resolve));
      for (const client of [redis, subscriber]) {
        try {
          await client.disconnect();
        } catch {
          // already closed
        }
      }
    }
  };
}

// Minimal console client that records every message.
export async function openConsole(wsUrl, token) {
  const ws = new WebSocket(wsUrl, ['verification.v1', `auth.${token}`]);
  const messages = [];
  const waiters = [];
  ws.on('message', (raw) => {
    const message = JSON.parse(raw.toString());
    messages.push(message);
    for (const waiter of [...waiters]) {
      if (waiter.predicate(message)) {
        waiters.splice(waiters.indexOf(waiter), 1);
        waiter.resolve(message);
      }
    }
  });
  await new Promise((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', reject);
  });
  return {
    ws,
    messages,
    send: (payload) => ws.send(JSON.stringify(payload)),
    waitFor(predicate, timeoutMs = 4000) {
      const found = messages.find(predicate);
      if (found) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        const waiter = { predicate, resolve };
        waiters.push(waiter);
        setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) {
            waiters.splice(index, 1);
            reject(new Error(`Timed out waiting for message; got: ${messages.map((m) => m.type).join(',')}`));
          }
        }, timeoutMs);
      });
    },
    close: () => ws.terminate()
  };
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
