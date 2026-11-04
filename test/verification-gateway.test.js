// End-to-end tests: two full instances share one Redis and never talk to
// each other directly (RT-3). Requires a real Redis (docker compose up -d).
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer as createTcpServer, connect as tcpConnect } from 'node:net';
import WebSocket from 'ws';
import {
  REDIS_URL,
  cleanup,
  connectClient,
  openConsole,
  redisAvailable,
  sampleActa,
  sleep,
  startInstance,
  tokenFor,
  uniquePrefix
} from './support/verification.js';
import { caseIdFor } from '../src/verification/ingestor.js';

const available = await redisAvailable();
const options = { skip: available ? false : 'Redis not reachable at REDIS_URL (run: docker compose up -d)' };

async function twoInstances(t, overrides) {
  const prefix = uniquePrefix();
  const admin = await connectClient();
  const a = await startInstance({ prefix, instanceId: 'instance-A', overrides });
  const b = await startInstance({ prefix, instanceId: 'instance-B', overrides });
  t.after(async () => {
    await a.stop();
    await b.stop();
    await cleanup(admin, prefix);
    await admin.quit();
  });
  return { prefix, admin, a, b };
}

async function pushDoubtful(admin, prefix, n, overrides) {
  const acta = sampleActa(n, overrides);
  await admin.xAdd(`${prefix}:incoming`, '*', { idem: acta.idem, acta: JSON.stringify(acta) });
  return caseIdFor(acta);
}

test('a claim on instance A is reflected live on instance B and B cannot take the same case', options, async (t) => {
  const { prefix, admin, a, b } = await twoInstances(t);
  const ana = await openConsole(a.wsUrl, tokenFor('AUD-ANA', 'auditor', 'Ana'));
  const bea = await openConsole(b.wsUrl, tokenFor('AUD-BEA', 'auditor', 'Bea'));
  t.after(() => { ana.close(); bea.close(); });

  assert.equal((await ana.waitFor((m) => m.type === 'hello')).instance, 'instance-A');
  assert.equal((await bea.waitFor((m) => m.type === 'hello')).instance, 'instance-B');
  await ana.waitFor((m) => m.type === 'snapshot');

  const id = await pushDoubtful(admin, prefix, 7);
  await bea.waitFor((m) => m.type === 'event' && m.case?.id === id && m.case.status === 'queued');

  ana.send({ type: 'claim', caseId: id, reqId: 'r1' });
  const claimed = await ana.waitFor((m) => m.type === 'result' && m.reqId === 'r1');
  assert.equal(claimed.ok, true);
  const seenOnB = await bea.waitFor((m) => m.type === 'event' && m.case?.id === id && m.case.status === 'claimed');
  assert.equal(seenOnB.case.owner, 'AUD-ANA');

  bea.send({ type: 'claim', caseId: id, reqId: 'r2' });
  const denied = await bea.waitFor((m) => m.type === 'result' && m.reqId === 'r2');
  assert.equal(denied.ok, false);
  assert.equal(denied.code, 'not_available');

  // B tries to resolve with A's token: identity comes from B's signed token, so it fails.
  bea.send({ type: 'resolve', caseId: id, token: claimed.case.token, decision: 'approve', reqId: 'r3' });
  assert.equal((await bea.waitFor((m) => m.reqId === 'r3')).code, 'not_owner');

  ana.send({ type: 'resolve', caseId: id, token: claimed.case.token, decision: 'approve', reqId: 'r4' });
  assert.equal((await ana.waitFor((m) => m.reqId === 'r4')).ok, true);
  await bea.waitFor((m) => m.type === 'event' && m.case?.status === 'resolved');
});

test('the lease survives while the console beats and expires when it disappears', options, async (t) => {
  const { prefix, admin, a, b } = await twoInstances(t);
  const ana = await openConsole(a.wsUrl, tokenFor('AUD-ANA'));
  const watcher = await openConsole(b.wsUrl, tokenFor('AUD-W'));
  t.after(() => watcher.close());
  const id = await pushDoubtful(admin, prefix, 1);
  await ana.waitFor((m) => m.type === 'event' && m.case?.id === id);
  ana.send({ type: 'claim', reqId: 'c' });
  await ana.waitFor((m) => m.reqId === 'c' && m.ok);

  // leaseTtlMs is 600ms in tests; staying connected for 3x that keeps the case.
  await sleep(1800);
  assert.ok(ana.messages.some((m) => m.type === 'lease'), 'renewals were sent');
  assert.ok(!watcher.messages.some((m) => m.type === 'event' && m.event === 'case.expired'));
  assert.equal(watcher.messages.filter((m) => m.type === 'event' && m.case?.status === 'queued' && m.case.id === id).length, 1);

  // Browser closed abruptly: no release message, the lease must expire by itself.
  ana.ws.terminate();
  const expired = await watcher.waitFor((m) => m.type === 'event' && m.case?.id === id && m.case.status === 'queued' && m.previousOwner === 'AUD-ANA', 3000);
  assert.ok(expired);
});

test('an idle auditor loses the case even though the socket stays open', options, async (t) => {
  const { prefix, admin, a } = await twoInstances(t, { idleLimitMs: 700 });
  const ana = await openConsole(a.wsUrl, tokenFor('AUD-ANA'));
  t.after(() => ana.close());
  const id = await pushDoubtful(admin, prefix, 1);
  await ana.waitFor((m) => m.type === 'event' && m.case?.id === id);
  ana.send({ type: 'claim', reqId: 'c' });
  await ana.waitFor((m) => m.reqId === 'c' && m.ok);
  const lost = await ana.waitFor((m) => m.type === 'lease_lost', 3000);
  assert.ok(['idle', 'expired'].includes(lost.reason));
  await ana.waitFor((m) => m.type === 'event' && m.case?.id === id && m.case.status === 'queued' && m.previousOwner, 3000);
});

test('reconnecting through the other instance gets a full versioned snapshot and keeps the lease', options, async (t) => {
  const { prefix, admin, a, b } = await twoInstances(t);
  const first = await openConsole(a.wsUrl, tokenFor('AUD-ANA'));
  const id = await pushDoubtful(admin, prefix, 3);
  await first.waitFor((m) => m.type === 'event' && m.case?.id === id);
  first.send({ type: 'claim', reqId: 'c' });
  const claim = await first.waitFor((m) => m.reqId === 'c');
  first.ws.terminate();

  const second = await openConsole(b.wsUrl, tokenFor('AUD-ANA'));
  t.after(() => second.close());
  const snap = await second.waitFor((m) => m.type === 'snapshot');
  assert.equal(snap.instance, 'instance-B');
  assert.ok(snap.version >= 2);
  assert.equal(snap.claimed[0].id, id);
  assert.equal(snap.claimed[0].owner, 'AUD-ANA');
  // B adopted the lease and keeps renewing it beyond the TTL.
  await sleep(1500);
  assert.equal(second.messages.filter((m) => m.type === 'lease').length > 0, true);
  second.send({ type: 'resolve', caseId: id, token: claim.case.token, decision: 'reject', reqId: 'r' });
  assert.equal((await second.waitFor((m) => m.reqId === 'r')).ok, true);
});

test('coordinator sees auditors from every instance, their case and multiple tabs', options, async (t) => {
  const { prefix, admin, a, b } = await twoInstances(t);
  const tab1 = await openConsole(a.wsUrl, tokenFor('AUD-ANA', 'auditor', 'Ana'));
  const tab2 = await openConsole(b.wsUrl, tokenFor('AUD-ANA', 'auditor', 'Ana'));
  const bea = await openConsole(b.wsUrl, tokenFor('AUD-BEA', 'auditor', 'Bea'));
  const boss = await openConsole(a.wsUrl, tokenFor('COORD-1', 'coordinator', 'Boss'));
  t.after(() => [tab1, tab2, bea, boss].forEach((c) => c.close()));
  const id = await pushDoubtful(admin, prefix, 9);
  await tab1.waitFor((m) => m.type === 'event' && m.case?.id === id);
  tab1.send({ type: 'claim', reqId: 'c' });
  await tab1.waitFor((m) => m.reqId === 'c' && m.ok);

  const view = await boss.waitFor((m) => m.type === 'presence'
    && m.auditors.length === 2
    && m.auditors.find((p) => p.auditorId === 'AUD-ANA')?.currentCase?.caseId === id
    && m.auditors.find((p) => p.auditorId === 'AUD-ANA')?.connections.length === 2, 4000);
  const anaView = view.auditors.find((p) => p.auditorId === 'AUD-ANA');
  assert.deepEqual(anaView.connections.map((c) => c.instance).sort(), ['instance-A', 'instance-B']);
  assert.ok(anaView.currentCaseMs >= 0);

  // Coordinators cannot claim.
  boss.send({ type: 'claim', reqId: 'x' });
  assert.equal((await boss.waitFor((m) => m.type === 'error')).code, 'forbidden');

  // A ghost (no close frame, process gone) disappears from the list.
  bea.ws.terminate();
  await boss.waitFor((m) => m.type === 'presence' && m.auditors.length === 1, 4000);
});

test('upgrade without a valid token is rejected; /ws keeps working', options, async (t) => {
  const { a } = await twoInstances(t);
  const status = await new Promise((resolve) => {
    const ws = new WebSocket(a.wsUrl, ['verification.v1', 'auth.forged.token']);
    ws.on('unexpected-response', (_req, res) => resolve(res.statusCode));
    ws.on('error', () => {});
  });
  assert.equal(status, 401);
  const notFound = await new Promise((resolve) => {
    const ws = new WebSocket(a.wsUrl.replace('/ws/verification', '/ws/unknown'));
    ws.on('unexpected-response', (_req, res) => resolve(res.statusCode));
    ws.on('error', () => {});
  });
  assert.equal(notFound, 404);
});

test('invalid and excessive WebSocket messages get explicit errors', options, async (t) => {
  const { a } = await twoInstances(t, { rateLimitWsMessages: 5, rateLimitWindowMs: 5000 });
  const ana = await openConsole(a.wsUrl, tokenFor('AUD-ANA'));
  t.after(() => ana.close());
  await ana.waitFor((m) => m.type === 'snapshot');
  ana.send({ type: 'resolve', caseId: 'x', token: 'abc', decision: 'approve' });
  assert.equal((await ana.waitFor((m) => m.type === 'error')).code, 'invalid_message');
  for (let i = 0; i < 6; i += 1) ana.send({ type: 'activity' });
  const limited = await ana.waitFor((m) => m.code === 'rate_limited');
  assert.ok(limited.retryAfterMs > 0);
});

test('HTTP login issues tokens and answers 429 once the credential exceeds its limit', options, async (t) => {
  const { a, b } = await twoInstances(t, { rateLimitHttpRequests: 3 });
  const login = (base, body) => fetch(`${base}/api/verification/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  const ok = await login(a.url, { id: 'AUD-ANA', name: 'Ana', role: 'auditor', accessCode: 'audit-code' });
  assert.equal(ok.status, 200);
  assert.ok((await ok.json()).token);
  assert.equal((await login(b.url, { id: 'AUD-ANA', role: 'auditor', accessCode: 'wrong' })).status, 401);
  assert.equal((await login(a.url, { id: 'AUD-ANA', role: 'coordinator', accessCode: 'audit-code' })).status, 401);
  // Fourth request for the same credential, counted across both instances.
  const limited = await login(b.url, { id: 'AUD-ANA', role: 'auditor', accessCode: 'audit-code' });
  assert.equal(limited.status, 429);
  assert.ok(Number(limited.headers.get('retry-after')) >= 1);
  assert.equal((await limited.json()).status, 'rate_limited');
  // Other credentials are unaffected.
  assert.equal((await login(a.url, { id: 'AUD-BEA', role: 'auditor', accessCode: 'audit-code' })).status, 200);
});

test('acta transmission is rate limited per jurado credential', options, async (t) => {
  const { a } = await twoInstances(t, { rateLimitActas: 2 });
  const post = () => fetch(`${a.url}/api/actas`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...sampleActa(1), ts_cierre: 1, votos: { C1: 160 }, sufragantes: 160 })
  });
  assert.equal((await post()).status, 201);
  assert.equal((await post()).status, 201);
  assert.equal((await post()).status, 429);
});

test('Redis outage: consoles see degraded mode, actions fail fast, state recovers', options, async (t) => {
  // TCP proxy in front of Redis that the test can cut and restore.
  const target = new URL(REDIS_URL);
  const sockets = new Set();
  const proxy = createTcpServer((client) => {
    const upstream = tcpConnect(Number(target.port || 6379), target.hostname);
    sockets.add(client);
    sockets.add(upstream);
    client.pipe(upstream).pipe(client);
    const drop = () => { client.destroy(); upstream.destroy(); };
    client.on('error', drop);
    upstream.on('error', drop);
    client.on('close', drop);
    upstream.on('close', drop);
  });
  await new Promise((resolve) => proxy.listen(0, '127.0.0.1', resolve));
  const port = proxy.address().port;
  const proxyUrl = `redis://127.0.0.1:${port}`;

  const prefix = uniquePrefix();
  const admin = await connectClient();
  const inst = await startInstance({ prefix, instanceId: 'instance-P', redisUrl: proxyUrl });
  t.after(async () => {
    await inst.stop();
    proxy.close();
    await cleanup(admin, prefix);
    await admin.quit();
  });
  const ana = await openConsole(inst.wsUrl, tokenFor('AUD-ANA'));
  t.after(() => ana.close());
  await ana.waitFor((m) => m.type === 'snapshot');

  // Cut Redis.
  await new Promise((resolve) => { proxy.close(resolve); for (const s of sockets) s.destroy(); });
  const down = await ana.waitFor((m) => m.type === 'system' && m.redis === 'down', 4000);
  assert.ok(down.since);
  const health = await fetch(`${inst.url}/api/verification/health`);
  assert.equal(health.status, 503);

  const started = Date.now();
  ana.send({ type: 'claim', reqId: 'during-outage' });
  const failed = await ana.waitFor((m) => m.reqId === 'during-outage', 3000);
  assert.equal(failed.ok, false);
  assert.equal(failed.code, 'redis_unavailable');
  assert.ok(Date.now() - started < 1500, 'fails fast instead of hanging');

  // Something happens while this instance is cut off (published events are lost).
  const id = await pushDoubtful(admin, prefix, 1);

  // Restore Redis on the same port.
  await new Promise((resolve) => proxy.listen(port, '127.0.0.1', resolve));
  await ana.waitFor((m) => m.type === 'system' && m.redis === 'up', 6000);
  // Recovery: a fresh snapshot is pushed, and the doubtful acta written while
  // the instance was cut off is ingested from the stream (nothing was lost).
  await ana.waitFor((m) => m.type === 'snapshot' && m.since === undefined && ana.messages.indexOf(m) > ana.messages.indexOf(down), 6000);
  await ana.waitFor((m) => (m.type === 'snapshot' && m.queue.some((c) => c.id === id))
    || (m.type === 'event' && m.case?.id === id), 6000);
});
