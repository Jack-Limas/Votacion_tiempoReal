import { Router } from 'express';

export function createPublicoRoutes(publico) {
  const router = Router();
  router.get('/whoami', (_req, res) => res.json({ instancia: publico.instanceId }));
  router.get('/metrics', (_req, res) => res.json(publico.metrics()));
  router.get('/stream', (req, res) => publico.handleStream(req, res));
  return router;
}
