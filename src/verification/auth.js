import { createHmac, timingSafeEqual } from 'node:crypto';

export const ROLES = Object.freeze(['auditor', 'coordinator']);
const SUBJECT_PATTERN = /^[A-Za-z0-9_-]{2,40}$/;

function base64url(input) {
  return Buffer.from(input).toString('base64url');
}

function sign(secret, data) {
  return createHmac('sha256', secret).update(data).digest('base64url');
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  return left.length === right.length && timingSafeEqual(left, right);
}

// Compact HMAC-SHA256 token: base64url(payload).base64url(signature).
// Only characters valid in a WebSocket subprotocol are used, so the browser
// can send it in Sec-WebSocket-Protocol instead of the URL (URLs end up in
// access logs).
export function issueToken(secret, { sub, name, role }, ttlSeconds, nowMs = Date.now()) {
  if (!SUBJECT_PATTERN.test(sub)) throw new Error('Invalid subject.');
  if (!ROLES.includes(role)) throw new Error('Invalid role.');
  const payload = base64url(JSON.stringify({
    sub,
    name: String(name ?? sub).slice(0, 60),
    role,
    exp: Math.floor(nowMs / 1000) + ttlSeconds
  }));
  return `${payload}.${sign(secret, payload)}`;
}

export function verifyToken(secret, token, nowMs = Date.now()) {
  if (typeof token !== 'string' || token.length > 2048) return null;
  const [payload, signature, extra] = token.split('.');
  if (!payload || !signature || extra !== undefined) return null;
  if (!safeEqual(signature, sign(secret, payload))) return null;
  let claims;
  try {
    claims = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
  if (!claims || !SUBJECT_PATTERN.test(claims.sub) || !ROLES.includes(claims.role)) return null;
  if (!Number.isInteger(claims.exp) || claims.exp * 1000 <= nowMs) return null;
  return { sub: claims.sub, name: claims.name ?? claims.sub, role: claims.role, exp: claims.exp };
}

export function checkAccessCode(expected, provided) {
  if (!expected) return false;
  return safeEqual(expected, provided ?? '');
}

export function isValidSubject(value) {
  return typeof value === 'string' && SUBJECT_PATTERN.test(value);
}

export const TOKEN_PROTOCOL_PREFIX = 'auth.';
export const APP_PROTOCOL = 'verification.v1';

// Extracts the token from the Sec-WebSocket-Protocol offer, falling back to
// an Authorization: Bearer header for non-browser clients (load tests).
export function tokenFromUpgrade(request) {
  const offered = String(request.headers['sec-websocket-protocol'] ?? '')
    .split(',')
    .map((value) => value.trim());
  const fromProtocol = offered.find((value) => value.startsWith(TOKEN_PROTOCOL_PREFIX));
  if (fromProtocol) return fromProtocol.slice(TOKEN_PROTOCOL_PREFIX.length);
  const header = String(request.headers.authorization ?? '');
  return header.startsWith('Bearer ') ? header.slice(7) : null;
}
