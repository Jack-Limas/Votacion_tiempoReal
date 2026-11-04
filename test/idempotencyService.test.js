import test from 'node:test';
import assert from 'node:assert/strict';
import { createIdempotencyService, RedisUnavailableError } from '../src/services/idempotencyService.js';

const acta = { acta: 'E14-001', mesa: 1, puesto: 1, municipio: 1, version: 1, idem: 'idem-001', votos: { C1: 1 } };

test('uses the already connected client and never connects lazily', async () => {
  let evaluated = 0;
  const redis = {
    isOpen: true,
    async connect() {
      assert.fail('the service must not open connections on its own');
    },
    async eval() {
      evaluated += 1;
      return JSON.stringify({ status: 'accepted', accepted: true });
    }
  };
  const result = await createIdempotencyService(redis).registerActa(acta, { valid: true });
  assert.equal(evaluated, 1);
  assert.deepEqual(result, { status: 'accepted', accepted: true });
});

test('wraps Redis failures so the controller can answer 503', async () => {
  const redis = {
    isOpen: false,
    async eval() {
      throw new Error('The client is closed');
    }
  };
  await assert.rejects(
    createIdempotencyService(redis).registerActa(acta, { valid: true }),
    RedisUnavailableError
  );
});
