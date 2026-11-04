import express, { Router } from 'express';
import { fileURLToPath } from 'node:url';
import { checkAccessCode, issueToken, isValidSubject, verifyToken } from './auth.js';

const PUBLIC_DIR = fileURLToPath(new URL('../../public/verification/', import.meta.url));

// Fixed-window limiter keyed by credential (token subject when present,
// otherwise the body/route-provided id, otherwise the client IP). Fails open
// with a header when Redis is down: the protected endpoints already return 503
// in that case, and blocking every request would hide the real failure.
export function createHttpRateLimiter(store, { scope, windowMs, limit, credential }) {
  return async function rateLimit(req, res, next) {
    const id = String(credential(req) ?? req.ip ?? 'unknown').slice(0, 80);
    try {
      const verdict = await store.rateLimit(scope, id, windowMs, limit);
      res.set('RateLimit-Limit', String(limit));
      res.set('RateLimit-Remaining', String(Math.max(limit - verdict.count, 0)));
      if (!verdict.allowed) {
        const retryAfter = Math.max(Math.ceil(verdict.retryAfterMs / 1000), 1);
        res.set('Retry-After', String(retryAfter));
        return res.status(429).json({
          status: 'rate_limited',
          accepted: false,
          retryable: true,
          retryAfterSeconds: retryAfter,
          message: `Too many requests: limit is ${limit} per ${windowMs / 1000}s for this credential.`
        });
      }
    } catch {
      res.set('RateLimit-Policy', 'degraded');
    }
    return next();
  };
}

function bearerSubject(secret) {
  return (req) => {
    const header = String(req.headers.authorization ?? '');
    if (!header.startsWith('Bearer ')) return null;
    return verifyToken(secret, header.slice(7))?.sub ?? null;
  };
}

export function createVerificationRoutes({ config, store, health, gateway }) {
  const router = Router();

  router.use('/verification', express.static(PUBLIC_DIR, { index: 'index.html', maxAge: '5m' }));

  const api = Router();
  api.use(createHttpRateLimiter(store, {
    scope: 'http',
    windowMs: config.rateLimitWindowMs,
    limit: config.rateLimitHttpRequests,
    credential: (req) => bearerSubject(config.tokenSecret)(req) ?? (isValidSubject(req.body?.id) ? `id:${req.body.id}` : null)
  }));

  // Exchanges a shared access code for a signed, short-lived token. In a real
  // deployment this would sit behind the operator's identity provider.
  api.post('/login', (req, res) => {
    const { id, name, role, accessCode } = req.body ?? {};
    if (!isValidSubject(id) || !['auditor', 'coordinator'].includes(role)) {
      return res.status(422).json({ status: 'rejected', message: 'id (2-40 chars: letters, digits, - or _) and role are required.' });
    }
    if (name !== undefined && (typeof name !== 'string' || name.length > 60)) {
      return res.status(422).json({ status: 'rejected', message: 'name must be a string up to 60 characters.' });
    }
    const expected = role === 'coordinator' ? config.coordinatorAccessCode : config.auditorAccessCode;
    if (!checkAccessCode(expected, accessCode)) {
      return res.status(401).json({ status: 'unauthorized', message: 'Invalid access code.' });
    }
    const token = issueToken(config.tokenSecret, { sub: id, name: name?.trim() || id, role }, config.tokenTtlSeconds);
    return res.status(200).json({ token, role, id, expiresIn: config.tokenTtlSeconds });
  });

  api.get('/health', (_req, res) => {
    const state = health.snapshot();
    res.status(state.redis === 'down' ? 503 : 200).json({
      instance: config.instanceId,
      ...state,
      connections: gateway.connections,
      metrics: gateway.metrics
    });
  });

  router.use('/api/verification', api);
  return router;
}
