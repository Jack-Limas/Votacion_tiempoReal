import test from 'node:test';
import assert from 'node:assert/strict';
import { createIdempotencyService } from '../src/services/idempotencyService.js';

test('conecta Redis de forma diferida al registrar un acta', async () => {
  const redis = {
    isOpen: false,
    async connect() {
      this.isOpen = true;
    },
    async eval() {
      return JSON.stringify({ status: 'accepted', accepted: true });
    }
  };
  const service = createIdempotencyService(redis);

  const result = await service.registerActa(
    { acta: 'E14-001', idem: 'idem-001', votos: {} },
    { valid: true }
  );

  assert.equal(redis.isOpen, true);
  assert.deepEqual(result, { status: 'accepted', accepted: true });
});
