const MAX_VOTE_COUNT = 1_000_000_000;
const IDEM_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

export function validateActa(payload) {
  const errors = [];

  if (!isRecord(payload)) {
    return { valid: false, errors: ['El cuerpo debe ser un objeto JSON.'] };
  }

  for (const field of ['acta', 'jurado', 'hash_foto']) {
    if (typeof payload[field] !== 'string' || payload[field].trim() === '') {
      errors.push(`${field} debe ser una cadena no vacía.`);
    }
  }

  if (
    typeof payload.idem !== 'string' ||
    !IDEM_PATTERN.test(payload.idem)
  ) {
    errors.push('idem debe tener entre 1 y 128 caracteres válidos.');
  }

  for (const field of ['mesa', 'puesto', 'municipio', 'version', 'ts_cierre', 'sufragantes']) {
    if (!isNonNegativeInteger(payload[field])) {
      errors.push(`${field} debe ser un entero no negativo.`);
    }
  }

  if (!isRecord(payload.votos) || Object.keys(payload.votos).length === 0) {
    errors.push('votos debe ser un objeto con al menos un registro.');
  } else {
    for (const [candidate, count] of Object.entries(payload.votos)) {
      if (candidate.trim() === '' || candidate.length > 64) {
        errors.push('Cada identificador de voto debe tener entre 1 y 64 caracteres.');
      }
      if (!Number.isSafeInteger(count) || count < 0 || count > MAX_VOTE_COUNT) {
        errors.push(`El total de votos para ${candidate} debe ser un entero entre 0 y ${MAX_VOTE_COUNT}.`);
      }
    }
  }

  if (errors.length > 0) {
    return { valid: false, errors };
  }

  const totalVotos = Object.values(payload.votos).reduce((total, count) => total + count, 0);
  if (!Number.isSafeInteger(totalVotos)) {
    return { valid: false, errors: ['La suma de votos excede el límite permitido.'] };
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
