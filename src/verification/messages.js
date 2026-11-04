// Validation for every message a console may send. Anything that does not
// match exactly is rejected before touching Redis.
export const MAX_MESSAGE_BYTES = 4 * 1024;
export const DECISIONS = Object.freeze(['approve', 'reject', 'recount']);
const CASE_ID = /^[A-Za-z0-9._:#-]{1,160}$/;
const TOKEN = /^\d{1,18}$/;
const REQ_ID = /^[A-Za-z0-9_-]{1,40}$/;

const SCHEMAS = {
  claim: { roles: ['auditor'], fields: { caseId: { optional: true, pattern: CASE_ID } } },
  release: { roles: ['auditor'], fields: { caseId: { pattern: CASE_ID }, token: { pattern: TOKEN } } },
  resolve: {
    roles: ['auditor'],
    fields: {
      caseId: { pattern: CASE_ID },
      token: { pattern: TOKEN },
      decision: { oneOf: DECISIONS },
      note: { optional: true, maxLength: 500 }
    }
  },
  activity: { roles: ['auditor'], fields: {} },
  snapshot: { roles: ['auditor', 'coordinator'], fields: {} },
  presence: { roles: ['coordinator'], fields: {} }
};

export function parseClientMessage(raw, role) {
  const text = typeof raw === 'string' ? raw : raw?.toString('utf8');
  if (!text || Buffer.byteLength(text) > MAX_MESSAGE_BYTES) {
    return { error: 'invalid_message', message: 'Empty or oversized message.' };
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    return { error: 'invalid_message', message: 'Message is not valid JSON.' };
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return { error: 'invalid_message', message: 'Message must be a JSON object.' };
  }
  const schema = SCHEMAS[data.type];
  if (!schema) return { error: 'unknown_type', message: 'Unknown message type.' };
  if (!schema.roles.includes(role)) {
    return { error: 'forbidden', message: `Role ${role} cannot send ${data.type}.` };
  }
  if (data.reqId !== undefined && (typeof data.reqId !== 'string' || !REQ_ID.test(data.reqId))) {
    return { error: 'invalid_message', message: 'Invalid reqId.' };
  }

  const allowed = new Set(['type', 'reqId', ...Object.keys(schema.fields)]);
  const unexpected = Object.keys(data).filter((key) => !allowed.has(key));
  if (unexpected.length > 0) {
    return { error: 'invalid_message', message: `Unexpected fields: ${unexpected.join(', ')}.` };
  }

  const message = { type: data.type, reqId: data.reqId };
  for (const [name, rule] of Object.entries(schema.fields)) {
    let value = data[name];
    if (value === undefined || value === null || value === '') {
      if (rule.optional) continue;
      return { error: 'invalid_message', message: `${name} is required.` };
    }
    if (name === 'token' && typeof value === 'number' && Number.isSafeInteger(value)) {
      value = String(value);
    }
    if (typeof value !== 'string') {
      return { error: 'invalid_message', message: `${name} must be a string.` };
    }
    if (rule.pattern && !rule.pattern.test(value)) {
      return { error: 'invalid_message', message: `${name} has an invalid format.` };
    }
    if (rule.oneOf && !rule.oneOf.includes(value)) {
      return { error: 'invalid_message', message: `${name} must be one of ${rule.oneOf.join(', ')}.` };
    }
    if (rule.maxLength && value.length > rule.maxLength) {
      return { error: 'invalid_message', message: `${name} exceeds ${rule.maxLength} characters.` };
    }
    message[name] = value;
  }
  return { message };
}
