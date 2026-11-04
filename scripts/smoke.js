// End-to-end smoke test against a running deployment (local compose or the
// cloud URL). Checks every route through the load balancer and the whole acta
// flow: normal, duplicate, corrected, doubtful -> approved by an auditor, and
// the public client receiving each change.
//
// Usage: node scripts/smoke.js --url=https://20.30.40.50.sslip.io \
//          --auditor-code=... [--insecure=1 for a local self-signed certificate]
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';

const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => a.slice(2).split('=')));
const BASE = (args.url ?? process.env.BASE_URL ?? 'http://localhost:8080').replace(/\/$/, '');
const AUDITOR_CODE = args['auditor-code'] ?? process.env.VERIFICATION_AUDITOR_CODE ?? 'dev-auditor';
if (args.insecure === '1') process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
const WS_BASE = BASE.replace(/^http/, 'ws');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const results = [];

function check(name, ok, detail = '') {
  results.push({ name, ok });
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}${detail ? ` - ${detail}` : ''}`);
}

async function waitFor(predicate, timeoutMs = 15_000) {
  const limit = Date.now() + timeoutMs;
  while (Date.now() < limit) {
    const value = predicate();
    if (value) return value;
    await sleep(100);
  }
  return null;
}

const post = async (body) => {
  const response = await fetch(`${BASE}/api/actas`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  return { status: response.status, body: await response.json() };
};

// ---- Routes through the load balancer ----
for (const path of ['/', '/jury/', '/ops/', '/verification/', '/health', '/whoami', '/metrics/all', '/api/results/dep']) {
  const response = await fetch(`${BASE}${path}`);
  check(`GET ${path}`, response.status === 200, String(response.status));
}
const instances = new Set();
for (let i = 0; i < 24; i += 1) instances.add((await (await fetch(`${BASE}/whoami`)).json()).instance);
check('requests spread over several instances (no sticky sessions)', instances.size >= 2, `${instances.size} distinct instances in 24 requests`);

// ---- Public client over WebSocket, following one test mesa's puesto ----
const mesa = 6000 + Math.floor(Math.random() * 400);
const puesto = 9000 + Math.floor(Math.random() * 900);
const topic = `pto:${puesto}`;
const publicMessages = [];
const publicWs = new WebSocket(`${WS_BASE}/ws?sub=${topic}`);
publicWs.on('message', (raw) => publicMessages.push(JSON.parse(raw)));
await new Promise((resolve, reject) => {
  publicWs.once('open', resolve);
  publicWs.once('error', reject);
});
check('public WebSocket connected', Boolean(await waitFor(() => publicMessages.find((m) => m.t === 'snapshot'))));
const latest = () => publicMessages.filter((m) => m.t === 'delta' || m.t === 'snapshot').reduce((state, m) => ({ ...state, ...(m.changes ?? m.state) }), {});

const acta = (version, votos, overrides = {}) => ({
  acta: `E14-${puesto}-${mesa}-03`, mesa, puesto, municipio: 99, version, jurado: `JUR-SMOKE-${mesa}`,
  ts_cierre: Math.floor(Date.now() / 1000), votos, sufragantes: Object.values(votos).reduce((a, b) => a + b, 0),
  hash_foto: 'smoke', idem: randomUUID(), ...overrides
});

// ---- Normal, duplicate, corrected ----
const v1 = acta(1, { C1: 100, C2: 50 });
const first = await post(v1);
check('normal acta accepted', first.status === 201, `${first.status} ${first.body.status}`);
check('public sees the normal acta', Boolean(await waitFor(() => latest()['v:C1'] === 100)));
const duplicate = await post(v1);
check('resend answered as duplicate, not added', duplicate.status === 200 && duplicate.body.duplicate === true, `${duplicate.status}`);
const v2 = acta(2, { C1: 90, C2: 60 });
const corrected = await post(v2);
check('correction accepted', corrected.status === 201 && corrected.body.type === 'correction', `${corrected.status} ${corrected.body.type}`);
const reason = await waitFor(() => publicMessages.find((m) => m.reason?.type === 'correction' && m.reason.version === 2));
check('public sees the correction with its reason', Boolean(reason) && latest()['v:C1'] === 90);

// ---- Doubtful -> auditor approves -> added once ----
const doubtfulMesa = mesa + 1;
const doubtful = { ...acta(1, { C1: 40, C2: 10 }), mesa: doubtfulMesa, acta: `E14-${puesto}-${doubtfulMesa}-03`, sufragantes: 55 };
const queued = await post(doubtful);
check('doubtful acta sent to verification', queued.status === 202, `${queued.status}`);

const login = await fetch(`${BASE}/api/verification/login`, {
  method: 'POST', headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ id: 'AUD-SMOKE', name: 'Smoke auditor', role: 'auditor', accessCode: AUDITOR_CODE })
});
check('auditor login', login.status === 200, String(login.status));
if (login.status === 200) {
  const { token } = await login.json();
  const consoleMessages = [];
  const auditor = new WebSocket(`${WS_BASE}/ws/verification`, ['verification.v1', `auth.${token}`]);
  auditor.on('message', (raw) => consoleMessages.push(JSON.parse(raw)));
  await new Promise((resolve, reject) => {
    auditor.once('open', resolve);
    auditor.once('error', reject);
  });
  const caseId = `${doubtful.acta}#v1`;
  const queuedCase = await waitFor(() => consoleMessages.find((m) => (m.type === 'snapshot' && m.queue.some((c) => c.id === caseId))
    || (m.type === 'event' && m.case?.id === caseId)));
  check('auditor console sees the doubtful case', Boolean(queuedCase));
  auditor.send(JSON.stringify({ type: 'claim', caseId, reqId: 'c1' }));
  const claim = await waitFor(() => consoleMessages.find((m) => m.reqId === 'c1'));
  check('auditor claims the case', claim?.ok === true, claim?.code ?? '');
  if (claim?.ok) {
    auditor.send(JSON.stringify({ type: 'resolve', caseId, token: claim.case.token, decision: 'approve', reqId: 'r1', note: 'smoke test' }));
    const resolved = await waitFor(() => consoleMessages.find((m) => m.reqId === 'r1'));
    check('auditor approves the case', resolved?.ok === true, resolved?.code ?? '');
    const verified = await waitFor(() => publicMessages.find((m) => m.reason?.type === 'verification' && m.reason.acta === doubtful.acta), 20_000);
    check('public sees the approved acta with a verification reason', Boolean(verified) && latest()['v:C1'] === 130);
    const status = await (await fetch(`${BASE}/api/actas/${doubtful.idem}`)).json();
    check('jurado can read the decision of its doubtful acta', status.verification === 'approve', status.verification ?? '');
  }
  auditor.close();
}
publicWs.close();

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed.`);
process.exitCode = failed.length > 0 ? 1 : 0;
