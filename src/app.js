import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { config } from './config/env.js';
import { createActaRoutes } from './routes/actaRoutes.js';

export function createApp(idempotencyService, { routers = [], actaMiddleware = [] } = {}) {
  const app = express();
  app.disable('x-powered-by');
  app.use(helmet());
  app.use(cors({
    origin: config.corsOrigins.includes('*') ? '*' : config.corsOrigins,
    credentials: false
  }));
  app.use(express.json({ limit: '64kb', strict: true }));
  app.get('/health', (_req, res) => res.status(200).json({ status: 'ok' }));
  app.use('/api/actas', ...actaMiddleware, createActaRoutes(idempotencyService));
  for (const router of routers) app.use(router);

  app.use((error, _req, res, _next) => {
    if (error instanceof SyntaxError && 'body' in error) {
      return res.status(400).json({
        status: 'rejected',
        accepted: false,
        message: 'El cuerpo de la solicitud no contiene JSON válido.'
      });
    }
    if (error.type === 'entity.too.large') {
      return res.status(413).json({
        status: 'rejected',
        accepted: false,
        message: 'El acta excede el tamaño máximo permitido.'
      });
    }

    console.error('Error inesperado en la API:', error);
    return res.status(500).json({
      status: 'internal_error',
      accepted: false,
      message: 'Error interno al procesar el acta.'
    });
  });

  return app;
}
