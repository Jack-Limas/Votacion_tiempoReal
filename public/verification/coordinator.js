import {
  $, el, connectConsole, formatDuration, initials, parseActa, renderConnection, renderHeaderUser,
  requireSession, toast
} from './console.js';

const session = requireSession('coordinator');
const cases = new Map();
let stats = {};
let presence = { auditors: [], now: Date.now() };
let presenceReceivedAt = Date.now();
let clockOffset = 0;
let connection = { socket: 'connecting', redis: 'unknown' };
let version = 0;

const serverNow = () => Date.now() + clockOffset;

function stat(label, value, accent) {
  return el('div', { class: `card stat ${accent ? 'accent' : ''}` }, el('span', {}, label), el('b', {}, value));
}

function renderStats() {
  const queued = [...cases.values()].filter((c) => c.status === 'queued').length;
  const inReview = [...cases.values()].filter((c) => c.status === 'claimed').length;
  $('#stats').replaceChildren(
    stat('In queue', queued, true),
    stat('In review', inReview),
    stat('Resolved', stats.resolved ?? 0),
    stat('Leases expired', stats.expired ?? 0),
    stat('Auditors online', presence.auditors.length)
  );
}

function renderQueue() {
  const queued = [...cases.values()].filter((c) => c.status === 'queued').sort((a, b) => Number(a.priority) - Number(b.priority));
  $('#queue-count').textContent = String(queued.length);
  if (queued.length === 0) {
    $('#queue').replaceChildren(el('li', { class: 'empty', style: 'display:block' }, el('span', { class: 'big' }, '✨'), 'Queue is empty.'));
    return;
  }
  $('#queue').replaceChildren(...queued.slice(0, 200).map((c, index) => {
    const acta = parseActa(c);
    return el('li', {},
      el('div', { class: 'rank' }, index + 1),
      el('div', {},
        el('div', { class: 'title' }, acta.acta ?? c.id),
        el('div', { class: 'meta' }, `Mun ${acta.municipio ?? '–'} · ${acta.sufragantes ?? '?'} voters · waiting `,
          el('span', { 'data-since': c.enqueuedAt }, formatDuration(serverNow() - Number(c.enqueuedAt))))),
      c.requeuedAt ? el('span', { class: 'tag hot', title: 'Returned after a lease expired or was released' }, 'Requeued') : el('span'));
  }));
}

function renderPresence() {
  $('#online-count').textContent = String(presence.auditors.length);
  const rows = presence.auditors.map((a) => {
    const busy = Boolean(a.currentCase);
    const idleMs = presence.now - (a.lastActivity || a.connectedAt);
    const dotClass = busy ? 'busy' : idleMs > 5 * 60_000 ? 'idle' : '';
    const instances = [...new Set(a.connections.map((c) => c.instance))];
    return el('tr', {},
      el('td', {}, el('div', { class: 'person' },
        el('div', { class: 'avatar' }, initials(a.name)),
        el('div', {},
          el('div', { style: 'font-weight:600' }, el('span', { class: `status-dot ${dotClass}`, style: 'display:inline-block;margin-right:8px' }), a.name),
          el('div', { class: 'muted mono', style: 'font-size:11px' }, a.auditorId)))),
      el('td', {}, el('div', { class: 'chips' },
        el('span', { class: 'tag' }, `${a.connections.length} tab${a.connections.length === 1 ? '' : 's'}`),
        instances.map((instance) => el('span', { class: 'tag mono' }, instance)))),
      el('td', {}, busy
        ? el('span', { class: 'mono' }, parseActa(a.currentCase).acta ?? a.currentCase.caseId)
        : el('span', { class: 'muted' }, 'Available')),
      el('td', { class: 'mono' }, busy
        ? el('span', { 'data-since': a.currentCase.claimedAt }, formatDuration(serverNow() - a.currentCase.claimedAt))
        : '–'),
      el('td', { class: 'mono' }, formatDuration(a.workedMs)),
      el('td', { class: 'mono' }, a.resolved));
  });
  $('#presence').replaceChildren(...(rows.length ? rows : [el('tr', {}, el('td', { colspan: '6', class: 'empty' }, 'No auditors connected.'))]));
  $('#updated').textContent = `Presence updated ${new Date(presenceReceivedAt).toLocaleTimeString()}`;
}

function render() {
  renderConnection(connection);
  renderStats();
  renderQueue();
  renderPresence();
  $('#version').textContent = version ? `State version v${version}` : '';
}

const client = session && connectConsole({
  session,
  onState(state) { connection = state; render(); },
  onSnapshot(snapshot) {
    clockOffset = snapshot.now - Date.now();
    version = snapshot.version;
    stats = snapshot.stats ?? {};
    cases.clear();
    for (const c of [...snapshot.queue, ...snapshot.claimed]) cases.set(c.id, c);
    render();
  },
  onEvent(event) {
    version = event.version;
    if (event.event === 'case.resolved') {
      cases.delete(event.case.id);
      stats.resolved = (stats.resolved ?? 0) + 1;
    } else {
      cases.set(event.case.id, event.case);
      if (event.event === 'case.expired') {
        stats.expired = (stats.expired ?? 0) + 1;
        toast(`Lease expired: ${parseActa(event.case).acta ?? event.case.id} is back in the queue.`, 'warn');
      }
    }
    render();
  },
  onMessage(message) {
    if (message.type === 'presence') {
      presence = message;
      presenceReceivedAt = Date.now();
      render();
    } else if (message.type === 'error') {
      toast(message.message ?? message.code, 'bad');
    }
  }
});

if (session) {
  renderHeaderUser(session);
  setInterval(() => {
    for (const node of document.querySelectorAll('[data-since]')) {
      node.textContent = formatDuration(serverNow() - Number(node.dataset.since));
    }
  }, 1000);
  render();
  void client;
}
