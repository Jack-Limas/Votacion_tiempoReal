import { Router } from 'express';
import { createActaController, createSubmissionStatusController } from '../controllers/actaController.js';

export function createActaRoutes(idempotencyService) {
  const router = Router();
  router.post('/', createActaController(idempotencyService));
  // Outcome of a submission by its idem key (includes the auditor's decision
  // for doubtful actas).
  router.get('/:idem', createSubmissionStatusController(idempotencyService));
  return router;
}
