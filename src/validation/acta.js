const MAX_VOTE_COUNT = 1_000_000_000;
export const IDEM_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

export function validateActa(payload) {
  const errors = [];

  if (!isRecord(payload)) {
    return { valid: false, errors: ['The body must be a JSON object.'] };
  }

  for (const field of ['acta', 'jurado', 'hash_foto']) {
    if (typeof payload[field] !== 'string' || payload[field].trim() === '') {
      errors.push(`${field} must be a non-empty string.`);
    }
  }

  if (
    typeof payload.idem !== 'string' ||
    !IDEM_PATTERN.test(payload.idem)
  ) {
    errors.push('idem must have between 1 and 128 valid characters.');
  }

  for (const field of ['mesa', 'puesto', 'municipio', 'version', 'ts_cierre', 'sufragantes']) {
    if (!isNonNegativeInteger(payload[field])) {
      errors.push(`${field} must be a non-negative integer.`);
    }
  }

  if (!isRecord(payload.votos) || Object.keys(payload.votos).length === 0) {
    errors.push('votos must be an object with at least one entry.');
  } else {
    for (const [candidate, count] of Object.entries(payload.votos)) {
      if (candidate.trim() === '' || candidate.length > 64) {
        errors.push('Each vote key must have between 1 and 64 characters.');
      }
      if (!Number.isSafeInteger(count) || count < 0 || count > MAX_VOTE_COUNT) {
        errors.push(`The vote count for ${candidate} must be an integer between 0 and ${MAX_VOTE_COUNT}.`);
      }
    }
  }

  if (errors.length > 0) {
    return { valid: false, errors };
  }

  const totalVotos = Object.values(payload.votos).reduce((total, count) => total + count, 0);
  if (!Number.isSafeInteger(totalVotos)) {
    return { valid: false, errors: ['The vote sum exceeds the allowed limit.'] };
  }

  if (totalVotos !== payload.sufragantes) {
    return {
      valid: false,
      doubtful: true,
      totalVotos,
      sufragantes: payload.sufragantes,
      errors: []
    };
  }

  return { valid: true, totalVotos, errors: [] };
}
