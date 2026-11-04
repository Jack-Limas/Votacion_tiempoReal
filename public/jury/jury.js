// Jury app (RT-6). Transport: plain HTTP POST protected by an idem key. Each
// attempt is independent, survives reconnections and needs no long-lived
// connection on a poor mobile network. The outbox stores the acta in
// IndexedDB BEFORE sending and retries until the server gives a definitive
// answer, so the jurado never has to remember to resend.
import { createActaOutbox } from '/acta-outbox.js';

const CANDIDATES = ['C1', 'C2', 'C3', 'C4', 'BLANCO', 'NULO', 'NO_MARCADO'];
const DEFAULTS = { C1: 184, C2: 97, C3: 311, C4: 0, BLANCO: 22, NULO: 9, NO_MARCADO: 4 };
const $ = (id) => document.getElementById(id);
const form = $('form');
const items = new Map(); // idem -> { acta, state, detail }

for (const candidate of CANDIDATES) {
  const label = document.createElement('label');
  label.className = 'field';
  label.innerHTML = '<span></span><input type="number" min="0" required>';
  label.querySelector('span').textContent = candidate;
  const input = label.querySelector('input');
  input.name = `v_${candidate}`;
  input.value = DEFAULTS[candidate];
  $('votes').append(label);
}

function votes() {
  return Object.fromEntries(CANDIDATES.map((c) => [c, Number(form.elements[`v_${c}`].value)]));
}

function updateSum() {
  const sum = Object.values(votes()).reduce((a, b) => a + b, 0);
  const sufragantes = Number(form.elements.sufragantes.value);
  const node = $('sum');
  node.className = `sum ${sum === sufragantes ? '' : 'bad'}`;
  node.textContent = sum === sufragantes
    ? `Sum ${sum} matches sufragantes.`
    : `Sum ${sum} ≠ sufragantes ${sufragantes}: it will go to human verification.`;
}
form.addEventListener('input', updateSum);
updateSum();

function setNetwork() {
  const pill = $('net');
  const online = navigator.onLine !== false;
  pill.className = `pill ${online ? 'ok' : 'warn'}`;
  pill.replaceChildren(Object.assign(document.createElement('span'), { className: 'dot' }), online ? 'Online' : 'Offline – stored locally');
}
window.addEventListener('online', setNetwork);
window.addEventListener('offline', setNetwork);
setNetwork();

const LABELS = {
  pending: ['warn', 'Stored, sending…'],
  retrying: ['warn', 'Waiting to retry'],
  accepted: ['ok', 'Registered'],
  duplicate: ['ok', 'Already registered'],
  superseded: ['ok', 'Registered (older version)'],
  verification_required: ['warn', 'In human verification'],
  approve: ['ok', 'Verified and counted'],
  reject: ['bad', 'Rejected by audit'],
  recount: ['bad', 'Physical recount ordered'],
  rejected: ['bad', 'Rejected (invalid data)'],
  idempotency_conflict: ['bad', 'Conflict: resend as new version']
};

function renderQueue() {
  const list = $('queue');
  if (items.size === 0) return;
  list.replaceChildren(...[...items.values()].reverse().map(({ acta, state, detail }) => {
    const [kind, text] = LABELS[state] ?? ['warn', state];
    const li = document.createElement('li');
    li.innerHTML = '<span><b class="mono"></b> <span class="muted"></span></span><span class="tag"></span>';
    li.querySelector('b').textContent = `${acta.acta} v${acta.version}`;
    li.querySelector('.muted').textContent = detail ?? '';
    const tag = li.querySelector('.tag');
    tag.className = `tag ${kind}`;
    tag.textContent = text;
    return li;
  }));
}

function stateOf(result, status) {
  if (status === 422) return 'rejected';
  if (result.verification) return result.verification;
  if (result.status === 'accepted' && result.superseded) return 'superseded';
  if (result.duplicate && result.status === 'accepted') return 'duplicate';
  return result.status;
}

const outbox = createActaOutbox({
  onResult(result, acta, status) {
    items.set(acta.idem, { acta, state: stateOf(result, status), detail: result.message });
    $('banner').className = 'banner hidden';
    renderQueue();
  },
  onError(error, acta) {
    if (acta) {
      items.set(acta.idem, { acta, state: 'retrying', detail: error.message });
      renderQueue();
    }
    $('banner').className = 'banner warn';
    $('banner').textContent = `The server could not confirm yet (${error.message}). Your acta is safe on this device and will be resent automatically.`;
  }
});
outbox.start();

// Doubtful actas: ask the server for the auditor's decision from time to time.
setInterval(async () => {
  for (const [idem, item] of items) {
    if (item.state !== 'verification_required') continue;
    try {
      const response = await fetch(`/api/actas/${encodeURIComponent(idem)}`);
      if (!response.ok) continue;
      const result = await response.json();
      if (result.verification) {
        item.state = result.verification;
        renderQueue();
      }
    } catch {
      // offline: try again later
    }
  }
}, 10_000);

async function photoHash(file) {
  if (!file) return 'no-photo';
  const digest = await crypto.subtle.digest('SHA-256', await file.arrayBuffer());
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const f = form.elements;
  const acta = {
    acta: f.acta.value.trim(),
    mesa: Number(f.mesa.value),
    puesto: Number(f.puesto.value),
    municipio: Number(f.municipio.value),
    version: Number(f.version.value),
    jurado: f.jurado.value.trim(),
    ts_cierre: Math.floor(Date.now() / 1000),
    votos: votes(),
    sufragantes: Number(f.sufragantes.value),
    hash_foto: await photoHash(f.photo.files[0])
  };
  const idem = await outbox.enqueueActa(acta);
  if (!items.has(idem)) items.set(idem, { acta: { ...acta, idem }, state: 'pending' });
  renderQueue();
});
