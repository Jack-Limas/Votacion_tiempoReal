// Doubtful acta -> verification queue -> auditor decision -> results, against
// a real Redis/Valkey. Every piece runs in-process with unique key prefixes.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createClient } from 'redis';
import { createIdempotencyService } from '../../src/services/idempotencyService.js';
import { createDecisionApplier } from '../../src/services/decisionApplier.js';
import { createKeys } from '../../src/results/keys.js';
import { seedCatalog } from '../../src/results/catalog.js';
import { validateActa } from '../../src/validation/acta.js';
import { createVerificationStore } from '../../src/verification/store.js';
import { createIngestor } from '../../src/verification/ingestor.js';

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
const options = { skip: probe ? false : `Redis not reachable at ${REDIS_URL} (run: docker compose up -d valkey)` };

async function setup(t) {
  const client = await connectOrNull();
  const id = randomUUID().slice(0, 8);
  const resultsPrefix = `test:{res-${id}}`;
  const verificationPrefix = `test:{ver-${id}}`;
  const keys = createKeys(resultsPrefix);
  await seedCatalog(client, keys.catalog);
  const service = createIdempotencyService(client, { keyPrefix: resultsPrefix, ttlSeconds: 600 });
  const store = createVerificationStore(client, { keyPrefix: verificationPrefix, leaseTtlMs: 60_000, presenceTtlMs: 60_000 });
  const ingestor = createIngestor({
    client: client.duplicate(),
    store,
    streamKey: `${resultsPrefix}:verification`,
    group: 'verification-ingest',
    consumer: 'test',
    logger: silent
  });
  const applier = createDecisionApplier({
    client: client.duplicate(),
    service,
    streamKey: `${verificationPrefix}:resolutions`,
    consumer: 'test',
    logger: silent
  });
  t.after(async () => {
    await ingestor.stop();
    await applier.stop();
    for (const prefix of [resultsPrefix, verificationPrefix]) {
      for await (const key of client.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) await client.del(key);
    }
    await client.quit();
  });
  return { client, keys, service, store, ingestor, applier };
}

function doubtfulActa(overrides = {}) {
  return {
    acta: 'E14-176-0042-03',
    mesa: 4201,
    puesto: 176,
    municipio: 11,
    version: 1,
    jurado: 'JUR-1',
    ts_cierre: 1,
    votos: { C1: 184, C2: 97 },
    sufragantes: 300,
    hash_foto: 'f',
    idem: randomUUID(),
    ...overrides
  };
}

async function queueAndResolve(ctx, acta, decision) {
  const first = await ctx.service.registerActa(acta, validateActa(acta));
  assert.equal(first.status, 'verification_required');
  await ctx.ingestor.drainOnce();
  const claim = await ctx.store.claim('AUD-1', 'Ana');
  assert.equal(claim.ok, true);
  const resolved = await ctx.store.resolve(claim.case.id, 'AUD-1', claim.case.token, decision, 'checked photo');
  assert.equal(resolved.ok, true);
  await ctx.applier.drainOnce();
  return claim.case;
}

test('an approved doubtful acta is added exactly once and published with its reason', options, async (t) => {
  const ctx = await setup(t);
  const acta = doubtfulActa();
  const c = await queueAndResolve(ctx, acta, 'approve');
  assert.equal(await ctx.client.hGet(ctx.keys.total('dep'), 'v:C1'), '184');
  assert.equal(await ctx.client.hGet(ctx.keys.total('mun:11'), 'mesas_reported'), '1');
  assert.equal(ctx.applier.stats.applied, 1);

  const [entry] = await ctx.client.xRevRange(ctx.keys.stream, '+', '-', { COUNT: 1 });
  const event = JSON.parse(entry.message.e);
  assert.equal(event.origin, 'approve');
  assert.equal(event.acta, acta.acta);

  // The jurado sees the outcome through the original idem.
  assert.equal((await ctx.service.getSubmission(acta.idem)).verification, 'approve');

  // Re-delivery of the same resolution (crash before XACK) adds nothing.
  const again = await ctx.service.applyVerificationDecision({
    caseId: c.id, acta, decision: 'approve', token: c.token, idem: acta.idem
  });
  assert.equal(again.duplicate, true);
  assert.equal(await ctx.client.hGet(ctx.keys.total('dep'), 'v:C1'), '184');
  assert.equal(await ctx.client.xLen(ctx.keys.stream), 1, 'one public event');
});

test('a decision with an older fencing token is refused', options, async (t) => {
  const ctx = await setup(t);
  const acta = doubtfulActa();
  const c = await queueAndResolve(ctx, acta, 'approve');
  const stale = await ctx.service.applyVerificationDecision({
    caseId: c.id, acta, decision: 'reject', token: String(Number(c.token) - 1), idem: acta.idem
  });
  assert.equal(stale.status, 'stale_token');
  assert.equal(await ctx.client.hGet(ctx.keys.decision(c.id), 'decision'), 'approve');
  assert.equal(await ctx.client.hGet(ctx.keys.total('dep'), 'v:C1'), '184');
});

test('reject and recount record the decision and never touch the totals', options, async (t) => {
  for (const decision of ['reject', 'recount']) {
    const ctx = await setup(t);
    const acta = doubtfulActa();
    const c = await queueAndResolve(ctx, acta, decision);
    assert.equal(await ctx.client.exists(ctx.keys.total('dep')), 0);
    assert.equal(await ctx.client.xLen(ctx.keys.stream), 0, 'no public event');
    assert.equal(await ctx.client.hGet(ctx.keys.decision(c.id), 'decision'), decision);
    assert.equal((await ctx.service.getSubmission(acta.idem)).verification, decision);
    assert.equal(ctx.applier.stats.recorded, 1);
  }
});

test('an approved acta older than an applied correction is superseded', options, async (t) => {
  const ctx = await setup(t);
  const v2 = doubtfulActa({ version: 2, votos: { C1: 200, C2: 100 } });
  await ctx.service.registerActa(v2, validateActa(v2));
  await queueAndResolve(ctx, doubtfulActa(), 'approve');
  assert.equal(await ctx.client.hGet(ctx.keys.total('dep'), 'v:C1'), '200');
});
