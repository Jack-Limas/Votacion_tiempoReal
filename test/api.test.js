import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createApp } from '../src/app.js';

const validActa = {
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

async function listen(server) {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  return `http://127.0.0.1:${address.port}`;
}

test('POST /api/actas confirma un acta y comunica cuando requiere verificación', async (t) => {
  const idempotencyService = {
    async registerActa(acta, validation) {
      if (validation.doubtful) {
        return {
          status: 'verification_required',
          accepted: false,
          duplicate: false,
          acta: acta.acta
        };
      }
      return {
        status: 'accepted',
        accepted: true,
        duplicate: false,
        acta: acta.acta
      };
    }
  };
  const server = createServer(createApp(idempotencyService));
  const baseUrl = await listen(server);
  t.after(() => new Promise((resolve) => server.close(resolve)));

  const acceptedResponse = await fetch(`${baseUrl}/api/actas`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(validActa)
  });
  assert.equal(acceptedResponse.status, 201);
  assert.equal((await acceptedResponse.json()).status, 'accepted');

  const doubtfulResponse = await fetch(`${baseUrl}/api/actas`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...validActa, sufragantes: 628 })
  });
  assert.equal(doubtfulResponse.status, 202);
  assert.equal((await doubtfulResponse.json()).status, 'verification_required');
});

test('POST /api/actas rechaza campos incompletos sin llamar a Redis', async (t) => {
  const server = createServer(createApp({
    async registerActa() {
      assert.fail('No debe registrar un payload inválido.');
    }
  }));
  const baseUrl = await listen(server);
  t.after(() => new Promise((resolve) => server.close(resolve)));

  const response = await fetch(`${baseUrl}/api/actas`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ idem: 'x' })
  });
  assert.equal(response.status, 422);
  assert.equal((await response.json()).status, 'rejected');
});
