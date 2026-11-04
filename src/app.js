import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { config } from './config/env.js';
import { createActaRoutes } from './routes/actaRoutes.js';

export function createApp(idempotencyService, { routers = [], actaMiddleware = [], publicDir = null } = {}) {
  const app = express();
  app.disable('x-powered-by');
  // Behind nginx (and Caddy in production) req.ip must come from
  // X-Forwarded-For, otherwise every client shares the proxy address and the
  // per-IP rate limit would throttle everybody together.
  app.set('trust proxy', config.trustProxy);
  app.use(helmet());
  app.use(cors({
    origin: config.corsOrigins.includes('*') ? '*' : config.corsOrigins,
    credentials: false
  }));
  app.use(express.json({ limit: '64kb', strict: true }));
  app.get('/health', (_req, res) => res.status(200).json({ status: 'ok' }));
  app.use('/api/actas', ...actaMiddleware, createActaRoutes(idempotencyService));
  for (const router of routers) app.use(router);
  if (publicDir) {
    // Static pages: / (public results), /jury/, /ops/ and /verification/.
    app.use(express.static(publicDir, {
      index: 'index.html',
      maxAge: '5m',
      setHeaders(res, path) {
        if (path.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache');
      }
    }));
  }

  app.use((error, _req, res, _next) => {
    if (error instanceof SyntaxError && 'body' in error) {
      return res.status(400).json({
        status: 'rejected',
        accepted: false,
        message: 'The request body is not valid JSON.'
      });
    }
    if (error.type === 'entity.too.large') {
      return res.status(413).json({
        status: 'rejected',
        accepted: false,
        message: 'The acta exceeds the maximum allowed size.'
      });
    }

    console.error('Unexpected API error:', error);
    return res.status(500).json({
      status: 'internal_error',
      accepted: false,
      message: 'Internal error while processing the acta.'
    });
  });

  return app;
}
