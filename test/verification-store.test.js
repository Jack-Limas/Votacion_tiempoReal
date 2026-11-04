// Integration tests against a real Redis (docker compose up -d).
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  cleanup,
  connectClient,
  makeStore,
  redisAvailable,
  sampleActa,
  silentLogger,
  sleep,
  uniquePrefix
} from './support/verification.js';
import { caseIdFor, createIngestor } from '../src/verification/ingestor.js';

const available = await redisAvailable();
const options = { skip: available ? false : 'Redis not reachable at REDIS_URL (run: docker compose up -d)' };

async function setup(t, overrides) {
  const prefix = uniquePrefix();
  const clientA = await connectClient();
  const clientB = await connectClient();
  t.after(async () => {
    await cleanup(clientA, prefix);
    await clientA.quit();
    await clientB.quit();
  });
  // Two stores on two connections behave like two application instances.
  return { prefix, clientA, clientB, a: makeStore(clientA, prefix, overrides), b: makeStore(clientB, prefix, overrides) };
}

async function ingest(store, n, overrides) {
  const acta = sampleActa(n, overrides);
  await store.ingest({ caseId: caseIdFor(acta), acta, idem: acta.idem, sufragantes: acta.sufragantes, totalVotos: 152 });
  return caseIdFor(acta);
}

test('concurrent claims from two instances: exactly one auditor gets the case', options, async (t) => {
  const { a, b } = await setup(t);
  const id = await ingest(a, 1);
  const attempts = await Promise.all(
    Array.from({ length: 40 }, (_, i) => (i % 2 ? a : b).claim(`AUD-${i}`, `Auditor ${i}`, i % 3 ? id : ''))
  );
  const winners = attempts.filter((r) => r.ok);
  assert.equal(winners.length, 1);
  assert.equal(winners[0].case.id, id);
  assert.ok(attempts.filter((r) => !r.ok).every((r) => ['not_available', 'queue_empty'].includes(r.code)));
  const snap = await a.snapshot();
  assert.equal(snap.queueLength, 0);
  assert.equal(snap.claimed.length, 1);
});

test('queue order follows priority and an auditor holds one case at a time', options, async (t) => {
  const { a } = await setup(t);
  const small = await ingest(a, 1, { sufragantes: 20 });
  const big = await ingest(a, 2, { sufragantes: 700 });
  const snap = await a.snapshot();
  assert.deepEqual(snap.queue.map((c) => c.id), [big, small]);
  const first = await a.claim('AUD-1', 'Ana');
  assert.equal(first.case.id, big);
  const second = await a.claim('AUD-1', 'Ana');
  assert.equal(second.ok, false);
  assert.equal(second.code, 'already_holding');
  assert.equal(second.case.id, big);
});

test('fencing tokens are monotonic across claims', options, async (t) => {
  const { a } = await setup(t);
  const id1 = await ingest(a, 1);
  const id2 = await ingest(a, 2);
  const r1 = await a.claim('AUD-1', 'Ana', id1);
  const r2 = await a.claim('AUD-2', 'Bea', id2);
  assert.ok(Number(r2.case.token) > Number(r1.case.token));
});

test('an expired lease is returned to the queue by the sweeper', options, async (t) => {
  const { a } = await setup(t, { leaseTtlMs: 200 });
  const id = await ingest(a, 1);
  await a.claim('AUD-1', 'Ana');
  assert.equal((await a.sweep()).expired, 0);
  await sleep(300);
  assert.equal((await a.sweep()).expired, 1);
  const snap = await a.snapshot();
  assert.deepEqual(snap.queue.map((c) => c.id), [id]);
  assert.equal(snap.claimed.length, 0);
  assert.equal(snap.stats.expired, 1);
});

test('renewing keeps the lease alive past its TTL', options, async (t) => {
  const { a } = await setup(t, { leaseTtlMs: 200 });
  await ingest(a, 1);
  const { case: c } = await a.claim('AUD-1', 'Ana');
  for (let i = 0; i < 6; i += 1) {
    await sleep(100);
    assert.equal((await a.renew(c.id, 'AUD-1', c.token)).ok, true);
    assert.equal((await a.sweep()).expired, 0);
  }
  assert.equal((await a.snapshot()).claimed[0].owner, 'AUD-1');
});

test('a late owner with an expired lease cannot release, renew or resolve the new owner\'s case', options, async (t) => {
  const { a, b } = await setup(t, { leaseTtlMs: 150 });
  const id = await ingest(a, 1);
  const old = (await a.claim('AUD-OLD', 'Old')).case;
  await sleep(250);
  // Not swept yet: the stale owner still cannot act because the lease time passed.
  assert.equal((await a.resolve(id, 'AUD-OLD', old.token, 'approve')).code, 'lease_expired');
  await b.sweep();
  const fresh = (await b.claim('AUD-NEW', 'New', id)).case;
  assert.notEqual(fresh.token, old.token);

  assert.equal((await a.release(id, 'AUD-OLD', old.token)).code, 'not_owner');
  assert.equal((await a.renew(id, 'AUD-OLD', old.token)).code, 'not_owner');
  assert.equal((await a.resolve(id, 'AUD-OLD', old.token, 'approve')).code, 'not_owner');
  // Right owner, stale token (e.g. a second tab with an old token) is also refused.
  assert.equal((await a.resolve(id, 'AUD-NEW', old.token, 'approve')).code, 'not_owner');

  const snap = await a.snapshot();
  assert.equal(snap.claimed[0].owner, 'AUD-NEW');
  assert.equal(snap.claimed[0].token, fresh.token);
  assert.equal((await b.resolve(id, 'AUD-NEW', fresh.token, 'reject', 'photo unreadable')).ok, true);
});

test('release returns the case to its original position; resolve removes it', options, async (t) => {
  const { a, clientA, prefix } = await setup(t);
  const id1 = await ingest(a, 1, { sufragantes: 500 });
  const id2 = await ingest(a, 2, { sufragantes: 10 });
  const c = (await a.claim('AUD-1', 'Ana')).case;
  assert.equal(c.id, id1);
  assert.equal((await a.release(id1, 'AUD-1', c.token)).ok, true);
  assert.deepEqual((await a.snapshot()).queue.map((q) => q.id), [id1, id2]);
  const again = (await a.claim('AUD-2', 'Bea')).case;
  assert.equal((await a.resolve(id1, 'AUD-2', again.token, 'approve')).ok, true);
  const snap = await a.snapshot();
  assert.deepEqual(snap.queue.map((q) => q.id), [id2]);
  assert.equal(snap.stats.resolved, 1);
  const resolutions = await clientA.xRange(`${prefix}:resolutions`, '-', '+');
  assert.equal(resolutions.length, 1);
  assert.equal(resolutions[0].message.token, again.token);
});

test('sweeping concurrently from two instances requeues each case exactly once', options, async (t) => {
  const { a, b, clientB, prefix } = await setup(t, { leaseTtlMs: 100 });
  const events = [];
  const sub = clientB.duplicate();
  sub.on('error', () => {});
  await sub.connect();
  t.after(() => sub.quit());
  await sub.subscribe(`${prefix}:events`, (raw) => events.push(JSON.parse(raw)));
  for (let i = 1; i <= 5; i += 1) {
    await ingest(a, i);
    await a.claim(`AUD-${i}`, `A${i}`);
  }
  await sleep(200);
  const results = await Promise.all([a.sweep(), b.sweep(), a.sweep(), b.sweep()]);
  assert.equal(results.reduce((sum, r) => sum + r.expired, 0), 5);
  await sleep(100);
  assert.equal(events.filter((e) => e.type === 'case.expired').length, 5);
  const versions = events.map((e) => e.version).filter(Boolean);
  assert.deepEqual(versions, [...versions].sort((x, y) => x - y), 'event versions are monotonic');
  assert.equal((await a.snapshot()).queueLength, 5);
});

test('ingest is idempotent and the consumer group splits the stream between instances', options, async (t) => {
  const { a, clientA, prefix } = await setup(t);
  const streamKey = `${prefix}:incoming`;
  for (let i = 1; i <= 12; i += 1) {
    const acta = sampleActa(i);
    // Same field layout the acta registration script writes.
    await clientA.xAdd(streamKey, '*', { idem: acta.idem, acta: JSON.stringify(acta) });
  }
  // A re-sent doubtful acta (new idem, same acta/version) must not create a second case.
  await clientA.xAdd(streamKey, '*', { idem: 'other-idem', acta: JSON.stringify(sampleActa(1, { idem: 'other-idem' })) });
  await clientA.xAdd(streamKey, '*', { idem: 'bad', acta: '{not json' });

  const ingestors = ['inst-1', 'inst-2'].map((consumer) => createIngestor({
    client: clientA.duplicate(),
    store: a,
    streamKey,
    group: 'verification-ingest',
    consumer,
    logger: silentLogger
  }));
  t.after(() => Promise.all(ingestors.map((i) => i.stop())));
  await Promise.all(ingestors.map((i) => i.drainOnce()));
  await Promise.all(ingestors.map((i) => i.drainOnce()));

  const snap = await a.snapshot();
  assert.equal(snap.queueLength, 12);
  const totals = ingestors.reduce((acc, i) => ({
    ingested: acc.ingested + i.stats.ingested,
    duplicates: acc.duplicates + i.stats.duplicates,
    malformed: acc.malformed + i.stats.malformed
  }), { ingested: 0, duplicates: 0, malformed: 0 });
  assert.deepEqual(totals, { ingested: 12, duplicates: 1, malformed: 1 });
  const pending = await clientA.xPending(streamKey, 'verification-ingest');
  assert.equal(pending.pending, 0);
});

test('presence groups tabs per auditor and ghost connections expire on their own', options, async (t) => {
  const { a, b } = await setup(t, { presenceTtlMs: 250 });
  const base = { auditorId: 'AUD-1', name: 'Ana', role: 'auditor', connectedAt: Date.now() };
  await a.touchPresence('conn-1', { ...base, connId: 'conn-1', instance: 'i-1' });
  await b.touchPresence('conn-2', { ...base, connId: 'conn-2', instance: 'i-2' });
  await b.touchPresence('conn-3', { auditorId: 'AUD-2', name: 'Bea', role: 'auditor', connId: 'conn-3', instance: 'i-2', connectedAt: Date.now() });
  await a.ingest({ caseId: 'X#v1', acta: sampleActa(1), sufragantes: 1 });
  await a.claim('AUD-1', 'Ana');

  let presence = await a.listPresence();
  assert.equal(presence.auditors.length, 2);
  const ana = presence.auditors.find((p) => p.auditorId === 'AUD-1');
  assert.deepEqual(ana.connections.map((c) => c.instance).sort(), ['i-1', 'i-2']);
  assert.equal(ana.currentCase.caseId, 'X#v1');

  // conn-3 keeps beating; conn-1 and conn-2 "lose their battery".
  await sleep(150);
  await b.touchPresence('conn-3', { auditorId: 'AUD-2', name: 'Bea', role: 'auditor', connId: 'conn-3', instance: 'i-2', connectedAt: Date.now() });
  await sleep(150);
  presence = await a.listPresence();
  assert.deepEqual(presence.auditors.map((p) => p.auditorId), ['AUD-2']);
});

test('rate limit counts per credential and window', options, async (t) => {
  const { a, b } = await setup(t);
  const results = [];
  for (let i = 0; i < 4; i += 1) results.push(await (i % 2 ? a : b).rateLimit('ws', 'AUD-1', 1000, 3));
  assert.deepEqual(results.map((r) => r.allowed), [true, true, true, false]);
  assert.ok(results[3].retryAfterMs > 0 && results[3].retryAfterMs <= 1000);
  assert.equal((await a.rateLimit('ws', 'AUD-2', 1000, 3)).allowed, true);
});
