import test from 'node:test';
import assert from 'node:assert/strict';
import { issueToken, verifyToken, tokenFromUpgrade, checkAccessCode } from '../src/verification/auth.js';
import { parseClientMessage } from '../src/verification/messages.js';
import { priorityBonusMs } from '../src/verification/store.js';

const SECRET = 'unit-secret-unit-secret-unit-secret-000';

test('signed tokens round-trip and carry the role', () => {
  const token = issueToken(SECRET, { sub: 'AUD-01', name: 'Ana', role: 'auditor' }, 60);
  assert.deepEqual(
    { ...verifyToken(SECRET, token), exp: undefined },
    { sub: 'AUD-01', name: 'Ana', role: 'auditor', exp: undefined }
  );
});

test('tampered, foreign-secret and expired tokens are rejected', () => {
  const token = issueToken(SECRET, { sub: 'AUD-01', role: 'auditor' }, 60);
  const [payload, signature] = token.split('.');
  const forged = Buffer.from(JSON.stringify({ sub: 'AUD-01', role: 'coordinator', exp: 9999999999 })).toString('base64url');
  assert.equal(verifyToken(SECRET, `${forged}.${signature}`), null);
  assert.equal(verifyToken('another-secret-another-secret-0000', token), null);
  assert.equal(verifyToken(SECRET, `${payload}.${signature}x`), null);
  const expired = issueToken(SECRET, { sub: 'AUD-01', role: 'auditor' }, 1, Date.now() - 10_000);
  assert.equal(verifyToken(SECRET, expired), null);
  assert.equal(verifyToken(SECRET, 'garbage'), null);
});

test('token is read from the WebSocket subprotocol or a bearer header', () => {
  assert.equal(tokenFromUpgrade({ headers: { 'sec-websocket-protocol': 'verification.v1, auth.abc.def' } }), 'abc.def');
  assert.equal(tokenFromUpgrade({ headers: { authorization: 'Bearer xyz' } }), 'xyz');
  assert.equal(tokenFromUpgrade({ headers: {} }), null);
});

test('access codes are compared in constant time and never match when unset', () => {
  assert.equal(checkAccessCode('code', 'code'), true);
  assert.equal(checkAccessCode('code', 'codf'), false);
  assert.equal(checkAccessCode('', ''), false);
});

test('client messages are validated strictly', () => {
  assert.deepEqual(parseClientMessage('{"type":"claim"}', 'auditor'), { message: { type: 'claim', reqId: undefined } });
  assert.equal(parseClientMessage('{"type":"claim"}', 'coordinator').error, 'forbidden');
  assert.equal(parseClientMessage('not json', 'auditor').error, 'invalid_message');
  assert.equal(parseClientMessage('{"type":"drop_db"}', 'auditor').error, 'unknown_type');
  assert.equal(parseClientMessage('{"type":"claim","extra":1}', 'auditor').error, 'invalid_message');
  assert.equal(parseClientMessage('{"type":"resolve","caseId":"A#v1","token":"7","decision":"maybe"}', 'auditor').error, 'invalid_message');
  assert.equal(parseClientMessage('{"type":"release","caseId":"A#v1","token":"x1"}', 'auditor').error, 'invalid_message');
  const ok = parseClientMessage('{"type":"resolve","caseId":"E14-1#v2","token":12,"decision":"approve","note":"ok"}', 'auditor');
  assert.deepEqual(ok.message, { type: 'resolve', reqId: undefined, caseId: 'E14-1#v2', token: '12', decision: 'approve', note: 'ok' });
  assert.equal(parseClientMessage('x'.repeat(5000), 'auditor').error, 'invalid_message');
});

test('priority favours cases that withhold more voters, with a cap', () => {
  assert.equal(priorityBonusMs({ sufragantes: 600 }), 600_000);
  assert.equal(priorityBonusMs({ sufragantes: 50_000 }), 1_000_000);
  assert.equal(priorityBonusMs({ sufragantes: 10, version: 2 }), 130_000);
});
