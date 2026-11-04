import { Router } from 'express';

export function createResultsRoutes(results) {
  const router = Router();
  router.get('/whoami', (_req, res) => res.json({ instance: results.instanceId }));
  router.get('/metrics', (_req, res) => res.json(results.metrics()));
  // Metrics of every instance, read from Redis (never from the other instances).
  router.get('/metrics/all', async (_req, res, next) => {
    try {
      res.json(await results.clusterMetrics());
    } catch (error) {
      next(error);
    }
  });
  router.get('/stream', (req, res) => results.handleStream(req, res));
  return router;
}
