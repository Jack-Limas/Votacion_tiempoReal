import test from 'node:test';
import assert from 'node:assert/strict';
import { validateActa } from '../src/validation/acta.js';

const sampleActa = {
  acta: 'E14-176-0042-03',
  mesa: 4201,
  puesto: 176,
  municipio: 11,
  version: 1,
  jurado: 'JUR-88134',
  ts_cierre: 1793577600,
  votos: { C1: 184, C2: 97, C3: 311, BLANCO: 22, NULO: 9, 'NO MARCADO': 4 },
  sufragantes: 627,
  hash_foto: '9f2c...',
  idem: '8f14e45fceea167a'
};

test('accepts an acta whose sum matches sufragantes', () => {
  assert.deepEqual(validateActa(sampleActa), {
    valid: true,
    totalVotos: 627,
    errors: []
  });
});

test('flags as doubtful an acta with different totals', () => {
  const result = validateActa({ ...sampleActa, sufragantes: 628 });
  assert.equal(result.valid, false);
  assert.equal(result.doubtful, true);
  assert.equal(result.totalVotos, 627);
  assert.equal(result.sufragantes, 628);
});

test('rejects negative votes and an invalid idem key', () => {
  const result = validateActa({
    ...sampleActa,
    idem: 'key with spaces',
    votos: { C1: -1 }
  });
  assert.equal(result.valid, false);
  assert.equal(result.errors.length, 2);
});
