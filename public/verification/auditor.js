import {
  $, el, connectConsole, formatDuration, parseActa, renderConnection, renderHeaderUser,
  requireSession, toast
} from './console.js';

const session = requireSession('auditor');
const cases = new Map();
let clockOffset = 0;
let settings = { leaseTtlMs: 15000 };
let connection = { socket: 'connecting', redis: 'unknown' };
let currentVersion = 0;
let reqSeq = 0;
let lastActivitySent = 0;
const pending = new Set();

const serverNow = () => Date.now() + clockOffset;
const myCase = () => [...cases.values()].find((c) => c.status === 'claimed' && c.owner === session?.sub);
const canAct = () => connection.socket === 'open' && connection.redis !== 'down';

function request(payload) {
  const reqId = `r${++reqSeq}`;
  if (!client.send({ ...payload, reqId })) {
    toast('Not connected. Try again when the connection is back.', 'warn');
    return;
  }
  pending.add(reqId);
  render();
}

function renderQueue() {
  const list = $('#queue');
  const queued = [...cases.values()].filter((c) => c.status === 'queued').sort((a, b) => Number(a.priority) - Number(b.priority));
  const taken = [...cases.values()].filter((c) => c.status === 'claimed' && c.owner !== session.sub);
  $('#queue-count').textContent = String(queued.length);
  $('#take-next').disabled = !canAct() || queued.length === 0 || Boolean(myCase()) || pending.size > 0;

  if (queued.length === 0 && taken.length === 0) {
    list.replaceChildren(el('li', { class: 'empty', style: 'display:block' }, el('span', { class: 'big' }, '🎉'), 'No doubtful actas waiting.'));
    return;
  }
  const rows = queued.map((c, index) => {
    const acta = parseActa(c);
    const diff = Number(c.totalVotos) - Number(c.sufragantes);
    return el('li', {},
      el('div', { class: 'rank' }, index + 1),
      el('div', {},
        el('div', { class: 'title' }, acta.acta ?? c.id, acta.version > 1 ? el('span', { class: 'tag', style: 'margin-left:6px' }, `v${acta.version}`) : null),
        el('div', { class: 'meta' },
          `Mun ${acta.municipio ?? '–'} · Puesto ${acta.puesto ?? '–'} · Mesa ${acta.mesa ?? '–'} · `,
          el('span', { class: Math.abs(diff) > 0 ? 'diff' : '' }, `${diff > 0 ? '+' : ''}${Number.isFinite(diff) ? diff : '?'} votes`),
          ' · waiting ', el('span', { 'data-since': c.enqueuedAt }, formatDuration(serverNow() - Number(c.enqueuedAt))))),
      el('button', {
        class: 'btn small',
        disabled: !canAct() || Boolean(myCase()) || pending.size > 0,
        onclick: () => request({ type: 'claim', caseId: c.id })
      }, 'Take'));
  });
  const lockedRows = taken.map((c) => el('li', { class: 'claimed' },
    el('div', { class: 'rank' }, '🔒'),
    el('div', {},
      el('div', { class: 'title' }, parseActa(c).acta ?? c.id),
      el('div', { class: 'meta' }, `In review by ${c.ownerName ?? c.owner} · `,
        el('span', { 'data-since': c.claimedAt }, formatDuration(serverNow() - Number(c.claimedAt))))),
    el('span', { class: 'tag lock' }, 'Locked')));
  list.replaceChildren(...rows, ...lockedRows);
}

function leaseRing(c) {
  const remaining = Math.max(0, Number(c.leaseExpiresAt) - serverNow());
  const ratio = Math.min(1, remaining / settings.leaseTtlMs);
  const circumference = 2 * Math.PI * 30;
  const cls = ratio > 0.5 ? '' : ratio > 0.2 ? 'warn' : 'bad';
  const svgNs = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(svgNs, 'svg');
  svg.setAttribute('width', '74');
  svg.setAttribute('height', '74');
  for (const [klass, offset] of [['track', 0], ['value', circumference * (1 - ratio)]]) {
    const circle = document.createElementNS(svgNs, 'circle');
    circle.setAttribute('class', klass);
    circle.setAttribute('cx', '37');
    circle.setAttribute('cy', '37');
    circle.setAttribute('r', '30');
    circle.setAttribute('stroke-dasharray', String(circumference));
    circle.setAttribute('stroke-dashoffset', String(offset));
    svg.append(circle);
  }
  return el('div', { class: `ring ${cls}`, title: 'Lease: renewed automatically while you are connected and active' },
    svg, el('div', { class: 'label' }, el('div', {}, `${Math.ceil(remaining / 1000)}s`, el('small', {}, 'lease'))));
}

function renderCase() {
  const host = $('#case');
  const c = myCase();
  if (!c) {
    host.replaceChildren(el('div', { class: 'empty' },
      el('span', { class: 'big' }, '🗂️'),
      el('div', { style: 'font-weight:600;color:var(--text)' }, 'You are not reviewing a case'),
      el('div', {}, 'Take the highest-priority case or choose one from the queue.')));
    return;
  }
  const acta = parseActa(c);
  const votes = Object.entries(acta.votos ?? {});
  const max = Math.max(1, ...votes.map(([, v]) => v));
  const total = votes.reduce((sum, [, v]) => sum + v, 0);
  const diff = total - Number(acta.sufragantes);
  const note = el('textarea', { id: 'note', maxlength: '500', placeholder: 'Note for the record (optional)…' });
  const busy = !canAct() || pending.size > 0;
  const decide = (decision) => request({ type: 'resolve', caseId: c.id, token: c.token, decision, note: note.value.trim() || undefined });

  host.replaceChildren(
    el('div', { class: 'case-head' },
      el('div', {},
        el('div', { class: 'muted', style: 'font-size:12px' }, `Case ${c.id}`),
        el('h3', { class: 'case-id' }, acta.acta ?? c.id),
        el('div', { class: 'chips', style: 'margin-top:8px' },
          el('span', { class: 'tag hot' }, `Mismatch ${diff > 0 ? '+' : ''}${diff}`),
          el('span', { class: 'tag' }, `Fencing token #${c.token}`),
          el('span', { class: 'tag' }, 'Reviewing ', el('span', { 'data-since': c.claimedAt }, formatDuration(serverNow() - Number(c.claimedAt)))))),
      leaseRing(c)),
    el('div', { class: 'kv' },
      el('div', {}, el('span', {}, 'Municipio'), el('b', {}, acta.municipio ?? '–')),
      el('div', {}, el('span', {}, 'Puesto'), el('b', {}, acta.puesto ?? '–')),
      el('div', {}, el('span', {}, 'Mesa'), el('b', {}, acta.mesa ?? '–')),
      el('div', {}, el('span', {}, 'Version'), el('b', {}, acta.version ?? 1)),
      el('div', {}, el('span', {}, 'Jurado'), el('b', {}, acta.jurado ?? '–'))),
    el('table', { class: 'votes' },
      el('thead', {}, el('tr', {}, el('th', {}, 'Option'), el('th', { style: 'width:45%' }, ''), el('th', { class: 'num' }, 'Votes'))),
      el('tbody', {}, votes.map(([name, value]) => el('tr', {},
        el('td', {}, name),
        el('td', {}, el('div', { class: 'bar', style: `width:${(value / max) * 100}%` })),
        el('td', { class: 'num' }, value)))),
      el('tfoot', {},
        el('tr', {}, el('td', {}, 'Sum'), el('td', {}), el('td', { class: 'num' }, total)),
        el('tr', {}, el('td', {}, 'Sufragantes'), el('td', {}), el('td', { class: 'num' }, acta.sufragantes ?? '–')),
        el('tr', {}, el('td', {}, 'Difference'), el('td', {}), el('td', { class: 'num diff' }, `${diff > 0 ? '+' : ''}${diff}`)))),
    note,
    el('div', { class: 'decision' },
      el('button', { class: 'btn ok', disabled: busy, onclick: () => decide('approve') }, '✓ Approve'),
      el('button', { class: 'btn warn', disabled: busy, onclick: () => decide('recount') }, '↻ Recount'),
      el('button', { class: 'btn bad', disabled: busy, onclick: () => decide('reject') }, '✕ Reject')),
    el('button', {
      class: 'btn ghost block', style: 'margin-top:10px', disabled: busy,
      onclick: () => request({ type: 'release', caseId: c.id, token: c.token })
    }, 'Return case to the queue'));
}

let renderedCaseKey = null;

function render() {
  renderConnection(connection);
  renderQueue();
  // Rebuild the case panel only when the case or the ability to act changes,
  // so the note being typed is never wiped.
  const c = myCase();
  const key = c ? `${c.id}:${c.token}:${canAct()}:${pending.size}` : `none:${canAct()}`;
  if (key !== renderedCaseKey) {
    renderedCaseKey = key;
    renderCase();
  }
  tick();
  $('#version').textContent = currentVersion ? `v${currentVersion}` : '';
}

// Cheap per-second refresh: timers and the lease ring only (no rebuilds, so
// no click is lost while the DOM changes under the pointer).
function tick() {
  for (const node of document.querySelectorAll('[data-since]')) {
    node.textContent = formatDuration(serverNow() - Number(node.dataset.since));
  }
  const ring = $('.ring');
  const c = myCase();
  if (ring && c) ring.replaceWith(leaseRing(c));
}

const client = session && connectConsole({
  session,
  onState(state) { connection = state; render(); },
  onHello(message) { settings = message.settings; },
  onSnapshot(snapshot) {
    clockOffset = snapshot.now - Date.now();
    currentVersion = snapshot.version;
    cases.clear();
    for (const c of [...snapshot.queue, ...snapshot.claimed]) cases.set(c.id, c);
    pending.clear();
    render();
  },
  onEvent(event) {
    currentVersion = event.version;
    if (event.event === 'case.resolved') cases.delete(event.case.id);
    else cases.set(event.case.id, event.case);
    render();
  },
  onMessage(message) {
    if (message.type === 'result') {
      pending.delete(message.reqId);
      if (message.ok) {
        if (message.case) cases.set(message.case.id, message.case);
        if (message.action === 'resolve') { cases.delete(message.caseId); toast('Decision recorded.', 'ok'); }
        if (message.action === 'release') toast('Case returned to the queue.', 'info');
      } else {
        const text = {
          redis_unavailable: 'Redis is unavailable: the action was NOT applied. Try again when the system recovers.',
          not_available: 'Someone else took that case first.',
          queue_empty: 'The queue is empty.',
          already_holding: 'Finish your current case first.',
          not_owner: 'This case is no longer yours (your lease expired and it was reassigned).',
          lease_expired: 'Your lease expired; the case went back to the queue.'
        }[message.code] ?? message.message ?? `Action failed (${message.code}).`;
        toast(text, 'bad', 5000);
        if (message.case) cases.set(message.case.id, message.case);
      }
      render();
    } else if (message.type === 'lease') {
      const c = cases.get(message.caseId);
      if (c) c.leaseExpiresAt = message.leaseExpiresAt;
      $('#idle').className = 'banner warn hidden';
    } else if (message.type === 'lease_lost') {
      toast(message.reason === 'idle'
        ? 'You were inactive for too long: your case went back to the queue.'
        : 'You lost the lease on your case.', 'bad', 6000);
      $('#idle').className = 'banner warn hidden';
      client.requestSnapshot();
    } else if (message.type === 'idle_warning') {
      const banner = $('#idle');
      banner.className = 'banner warn';
      banner.replaceChildren(el('strong', {}, 'Still there?'),
        ` Your case returns to the queue in ${Math.ceil(message.releaseInMs / 1000)}s unless you interact with the page.`);
    } else if (message.type === 'error') {
      toast(message.code === 'rate_limited'
        ? `Slow down: too many actions. Retry in ${Math.ceil(message.retryAfterMs / 1000)}s.`
        : message.message, 'bad');
    }
  }
});

if (session) {
  renderHeaderUser(session);
  $('#take-next').addEventListener('click', () => request({ type: 'claim' }));
  // Activity signal: lets the server tell "still working" from "went to lunch".
  const markActive = () => {
    if (!myCase() || Date.now() - lastActivitySent < 15_000) return;
    lastActivitySent = Date.now();
    client.send({ type: 'activity' });
    $('#idle').className = 'banner warn hidden';
  };
  for (const type of ['pointerdown', 'keydown', 'pointermove', 'wheel', 'touchstart']) {
    window.addEventListener(type, markActive, { passive: true });
  }
  setInterval(tick, 1000);
  render();
}
