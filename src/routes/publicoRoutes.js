import { Router } from 'express';

export function createPublicoRoutes(publico) {
  const router = Router();
  router.get('/whoami', (_req, res) => res.json({ instancia: publico.instanceId }));
  router.get('/metrics', (_req, res) => res.json(publico.metrics()));
  // Métricas de todas las instancias, leídas de Redis (no de las otras instancias).
  router.get('/metrics/all', async (_req, res, next) => {
    try {
      res.json(await publico.metricsCluster());
    } catch (error) {
      next(error);
    }
  });
  router.get('/stream', (req, res) => publico.handleStream(req, res));
  return router;
}
