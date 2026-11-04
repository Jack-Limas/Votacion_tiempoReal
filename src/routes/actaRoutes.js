import { Router } from 'express';
import { createActaController } from '../controllers/actaController.js';

export function createActaRoutes(idempotencyService) {
  const router = Router();
  router.post('/', createActaController(idempotencyService));
  return router;
}
