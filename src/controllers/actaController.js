import { RedisUnavailableError } from '../services/idempotencyService.js';
import { validateActa } from '../validation/acta.js';

export function createActaController(idempotencyService) {
  return async function receiveActa(req, res, next) {
    const validation = validateActa(req.body);
    if (validation.errors.length > 0) {
      return res.status(422).json({
        status: 'rejected',
        accepted: false,
        duplicate: false,
        errors: validation.errors
      });
    }

    try {
      const result = await idempotencyService.registerActa(req.body, validation);

      if (result.status === 'idempotency_conflict') {
        return res.status(409).json(result);
      }
      if (result.status === 'verification_required' && !result.duplicate) {
        return res.status(202).json(result);
      }
      return res.status(result.duplicate ? 200 : 201).json(result);
    } catch (error) {
      if (error instanceof RedisUnavailableError) {
        console.error('No se pudo procesar la recepción del acta:', error.cause);
        return res.status(503).json({
          status: 'temporarily_unavailable',
          accepted: false,
          retryable: true,
          message: 'No se pudo confirmar el acta. Conserve el envío y reintente automáticamente.'
        });
      }
      return next(error);
    }
  };
}
