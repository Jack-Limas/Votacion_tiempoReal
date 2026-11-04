// RF-9 demonstration with a deliberately slow client, against a real
// Redis/Valkey. The results module runs in-process with small thresholds.
//
// Determinism: a client that merely stops reading only makes Node's output
// buffer grow after the kernel buffers are full, and their size depends on
// the OS (Linux with net.ipv4.tcp_rmem up to 33 MB absorbed more than this
// test pushes). So the test corks the SERVER side of the slow connection
// (socket.cork()): every byte the app writes stays in Node's own buffer, which
// is exactly what res.writableLength measures, i.e. the state the server is in
// once the kernel buffers of a slow client are full. Uncorking is the client
// reading again. No production code or threshold is changed for the test.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { connect as tcpConnect } from 'node:net';
import express from 'express';
import { createClient } from 'redis';
import { config as baseConfig } from '../../src/config/env.js';
import { createResults } from '../../src/results/index.js';
import { createKeys } from '../../src/results/keys.js';
import { createResultsRoutes } from '../../src/routes/resultsRoutes.js';
import { connect as sseConnect, createClientState } from '../../scripts/lib/sse-client.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const silent = { log() {}, error() {} };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
const options = { skip: probe ? false : `Redis not reachable at ${REDIS_URL} (run: docker compose up -d valkey)` };

async function startServer(t, overrides) {
  const redis = await connectOrNull();
  const prefix = `test:{rf9-${randomUUID().slice(0, 8)}}`;
  const keys = createKeys(prefix);
  const results = createResults({
    redis,
    logger: silent,
    config: {
      ...baseConfig,
      redisKeyPrefix: prefix,
      instanceId: 'rf9-test',
      fanoutMode: 'batched',
      flushMs: 20,
      heartbeatMs: 60_000,
      metricsTtlSeconds: 5,
      ...overrides
    }
  });
  await results.start();
  const app = express();
  app.use(createResultsRoutes(results));
  const server = createServer(app);
  const serverSockets = new Set();
  server.on('connection', (socket) => {
    serverSockets.add(socket);
    socket.on('close', () => serverSockets.delete(socket));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    await results.stop();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for await (const key of redis.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) await redis.del(key);
    await redis.quit();
  });
  return { redis, keys, results, serverSockets, port: server.address().port, url: `http://127.0.0.1:${server.address().port}` };
}

// A client whose connection stops draining: its server-side socket is corked
// (see the note at the top), and uncorked when it "reads again".
async function slowClient(srv) {
  const socket = tcpConnect(srv.port, '127.0.0.1');
  let text = '';
  let closed = false;
  socket.on('data', (chunk) => {
    text += chunk.toString('utf8');
  });
  socket.on('close', () => {
    closed = true;
  });
  socket.on('error', () => {});
  await new Promise((resolve) => socket.once('connect', resolve));
  socket.write('GET /stream?sub=dep HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n');
  // Wait for the response headers so the SSE connection is registered.
  await waitFor(() => text.includes('\r\n\r\n'), 5000);
  const serverSide = [...srv.serverSockets].find((s) => s.remotePort === socket.localPort);
  assert.ok(serverSide, 'server side of the slow connection found');
  return {
    socket,
    stopReading: () => serverSide.cork(),
    readAgain: () => serverSide.uncork(),
    get text() {
      return text;
    },
    get closed() {
      return closed;
    }
  };
}

// Writes the same absolute values to acc:dep and to the stream, like the apply
// script does, with ~4 KB per event so buffers fill quickly.
let counter = 0;
async function pushEvents(redis, keys, count) {
  for (let i = 0; i < count; i += 200) {
    const multi = redis.multi();
    for (let j = i; j < Math.min(count, i + 200); j += 1) {
      counter += 1;
      const changes = {};
      for (let f = 0; f < 200; f += 1) changes[`v:C${f}`] = counter * 1000 + f;
      multi.hSet(keys.total('dep'), Object.fromEntries(Object.entries(changes).map(([k, v]) => [k, String(v)])));
      multi.xAdd(keys.stream, '*', { e: JSON.stringify({ acta: `A${counter}`, version: 1, type: 'new', receivedAt: Date.now(), changes: { dep: changes } }) });
    }
    await multi.exec();
  }
}

async function waitFor(condition, timeoutMs) {
  const limit = Date.now() + timeoutMs;
  while (Date.now() < limit) {
    if (await condition()) return true;
    await sleep(50);
  }
  return false;
}

test('RF-9: a slow client stops getting detail, the fast one does not, and it is caught up with a snapshot', options, async (t) => {
  const srv = await startServer(t, { slowSoftBytes: 64 * 1024, slowHardBytes: 512 * 1024 * 1024, slowMaxDegradedMs: 600_000 });
  const fast = createClientState();
  let fastMessages = 0;
  const fastConn = await sseConnect(srv.url, [{ topic: 'dep' }], {
    onMessage: (m) => {
      fastMessages += 1;
      fast.apply(m);
    }
  });
  t.after(() => fastConn.close());
  const slow = await slowClient(srv);
  t.after(() => slow.socket.destroy());
  slow.stopReading();

  let pushed = 0;
  const degraded = await waitFor(async () => {
    await pushEvents(srv.redis, srv.keys, 400);
    pushed += 400;
    return srv.results.metrics().slow.degraded >= 1;
  }, 60_000);
  assert.ok(degraded, `the slow client was never degraded after ${pushed} events`);
  console.log(`  slow client degraded after ${pushed} events (folded into window frames)`);

  await pushEvents(srv.redis, srv.keys, 200);
  assert.ok(await waitFor(() => fast.state('dep')?.['v:C0'] === counter * 1000, 10_000), 'the fast client keeps receiving everything');
  assert.equal(srv.results.metrics().slow.disconnected, 0);
  assert.equal(srv.results.metrics().slow.degraded, 1, 'only the slow client was degraded');

  // The slow client starts reading again: its buffer drains and the recovery
  // loop sends one snapshot that equals the stored totals.
  slow.readAgain();
  assert.ok(await waitFor(() => srv.results.metrics().slow.recovered >= 1, 20_000), 'never recovered');
  assert.ok(await waitFor(() => slow.text.includes('"t":"snapshot"', slow.text.indexOf('"t":"snapshot"') + 1), 5000), 'no recovery snapshot received');
  const lastSnapshot = slow.text.slice(slow.text.lastIndexOf('{"t":"snapshot"'));
  const snapshot = JSON.parse(lastSnapshot.slice(0, lastSnapshot.indexOf('\n')));
  assert.equal(snapshot.state['v:C0'], counter * 1000, 'the snapshot carries the latest totals');
  assert.deepEqual(snapshot.state, fast.state('dep'), 'slow client converges to the fast client state');
  console.log(`  recovered with one snapshot; fast client got ${fastMessages} messages meanwhile`);
});

test('RF-9: a slow client that never drains is disconnected with a reconnect signal', options, async (t) => {
  const srv = await startServer(t, { slowSoftBytes: 64 * 1024, slowHardBytes: 256 * 1024, slowMaxDegradedMs: 2000 });
  const slow = await slowClient(srv);
  t.after(() => slow.socket.destroy());
  slow.stopReading();

  const disconnected = await waitFor(async () => {
    await pushEvents(srv.redis, srv.keys, 400);
    return srv.results.metrics().slow.disconnected >= 1;
  }, 60_000);
  assert.ok(disconnected, 'the slow client was never disconnected');
  // The client reads again: it receives the queued reconnect signal and the end.
  slow.readAgain();
  assert.ok(await waitFor(() => slow.closed, 10_000), 'socket closed');
  assert.ok(await waitFor(() => srv.results.registry.connections.size === 0, 10_000), 'the connection left the registry');
  assert.ok(slow.text.includes('"t":"reconnect","reason":"slow_consumer"'), 'the client got the reconnect signal');
  console.log('  disconnected after staying degraded past the time limit');
});
