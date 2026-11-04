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
  // Polling transport (also handy for clients that cannot keep a connection).
  router.get('/api/results/:topic', async (req, res, next) => {
    if (!results.isTopic(req.params.topic)) {
      return res.status(400).json({ status: 'rejected', message: 'Use dep, mun:{id} or pto:{id}.' });
    }
    try {
      res.set('Cache-Control', 'public, max-age=1');
      return res.json(await results.poll(req.params.topic));
    } catch (error) {
      if (results.status.redis === 'down' || /closed|ECONNREFUSED|offline/i.test(String(error?.message))) {
        return res.status(503).json({ status: 'temporarily_unavailable', retryable: true });
      }
      return next(error);
    }
  });
  return router;
}
