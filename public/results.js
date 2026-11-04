// Public results page. Transport: Server-Sent Events (server -> client only).
// The browser's automatic EventSource reconnection is not used: it cannot
// send our cursors and it retries in lock-step. This client reconnects with
// exponential backoff + full jitter and resumes from its cursor (RF-6), so it
// ends in exactly the same state as a client that never disconnected.
import { backoffDelay } from '/assets/backoff.js';
import { observeInp } from '/assets/inp.js';

const NAMES = { C1: 'Candidate 1', C2: 'Candidate 2', C3: 'Candidate 3', C4: 'Candidate 4', BLANCO: 'Blank vote', NULO: 'Null vote', NO_MARCADO: 'Unmarked' };
const $ = (id) => document.getElementById(id);

let topic = 'dep';
let source = null;
let attempt = 0;
let retryTimer = null;
let stableTimer = null;
const states = new Map(); // topic -> { cursor, state }
let renderQueued = false;

function compareIds(a, b) {
  const [am, as] = a.split('-').map(Number);
  const [bm, bs] = b.split('-').map(Number);
  return am - bm || as - bs;
}

function setConnection(kind, text) {
  const pill = $('conn');
  pill.className = `pill ${kind}`;
  pill.replaceChildren(Object.assign(document.createElement('span'), { className: 'dot' }), text);
}

function banner(kind, text) {
  const node = $('banner');
  if (!text) {
    node.className = 'banner hidden';
    return;
  }
  node.className = `banner ${kind}`;
  node.textContent = text;
}

function title(t) {
  if (t === 'dep') return 'Department';
  const [kind, id] = t.split(':');
  return kind === 'mun' ? `Municipality ${id}` : `Polling station ${id}`;
}

// Rendering is batched on animation frames: a burst of deltas costs one
// layout, which keeps the page responsive on low-end phones (RT-9).
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {
    renderQueued = false;
    render();
  });
}

function render() {
  const entry = states.get(topic);
  $('scope-title').textContent = title(topic);
  const state = entry?.state ?? {};
  const rows = Object.entries(state)
    .filter(([key]) => key.startsWith('v:'))
    .map(([key, votes]) => [key.slice(2), Number(votes)])
    .sort((a, b) => b[1] - a[1]);
  const total = rows.reduce((sum, [, votes]) => sum + votes, 0);
  $('empty').classList.toggle('hidden', rows.length > 0);
  $('reported').textContent = Number(state.mesas_reported ?? 0).toLocaleString('en-US');
  const percent = Number(state.percent_reported ?? 0);
  $('percent').textContent = `${percent.toFixed(2)} %`;
  $('progress-bar').style.width = `${Math.min(100, percent)}%`;
  $('cursor').textContent = entry?.cursor ?? '–';

  const list = $('candidates');
  const items = rows.map(([candidate, votes]) => {
    const share = total > 0 ? (votes / total) * 100 : 0;
    const li = document.createElement('li');
    li.innerHTML = '<span class="name"></span><span class="votes"></span><div class="bar"><div></div></div>';
    li.querySelector('.name').textContent = NAMES[candidate] ?? candidate;
    li.querySelector('.votes').textContent = `${votes.toLocaleString('en-US')} · ${share.toFixed(1)} %`;
    li.querySelector('.bar div').style.width = `${share}%`;
    return li;
  });
  list.replaceChildren(...items);
}

function notice(text) {
  const list = $('notices');
  if (list.firstElementChild?.classList.contains('muted')) list.replaceChildren();
  const li = document.createElement('li');
  li.textContent = `${new Date().toLocaleTimeString()} · ${text}`;
  list.prepend(li);
  while (list.children.length > 6) list.lastElementChild.remove();
}

function explain(reason) {
  if (reason.type === 'correction') {
    notice(`Acta ${reason.acta} was corrected (version ${reason.version}); some numbers went down.`);
  } else if (reason.type === 'verification') {
    notice(`Acta ${reason.acta} was added after an auditor verified it.`);
  }
}

function apply(message) {
  if (message.t === 'status') {
    if (message.redis === 'down') {
      banner('warn', `Live updates are paused since ${new Date(message.since).toLocaleTimeString()}. You are seeing the last confirmed numbers; nothing is lost and the page will catch up automatically.`);
      setConnection('warn', 'Paused');
    } else {
      banner(null);
      setConnection('ok', 'Live');
    }
    return;
  }
  if (message.t === 'reconnect') {
    // Server asked us to come back (slow connection): resume from the cursor.
    reconnect(true);
    return;
  }
  const entry = states.get(message.topic);
  if (message.t === 'snapshot') {
    states.set(message.topic, { cursor: message.upTo, state: { ...message.state } });
  } else if (message.t === 'delta') {
    if (entry?.cursor && compareIds(message.upTo, entry.cursor) <= 0) return;
    const state = { ...(entry?.state ?? {}), ...message.changes };
    states.set(message.topic, { cursor: message.upTo, state });
    if (message.reason) explain(message.reason);
  }
  $('updated').textContent = `updated ${new Date().toLocaleTimeString()}`;
  if (message.topic === topic) scheduleRender();
}

function connect() {
  clearTimeout(retryTimer);
  const entry = states.get(topic);
  const sub = entry?.cursor ? `${topic}@${entry.cursor}` : topic;
  setConnection('warn', attempt === 0 ? 'Connecting…' : 'Reconnecting…');
  source = new EventSource(`/stream?sub=${encodeURIComponent(sub)}`);
  source.onopen = () => {
    setConnection('ok', 'Live');
    banner(null);
    clearTimeout(stableTimer);
    // Reset the backoff only once the connection proved stable.
    stableTimer = setTimeout(() => { attempt = 0; }, 10_000);
  };
  source.onmessage = (event) => {
    try {
      apply(JSON.parse(event.data));
    } catch (error) {
      console.error(error);
    }
  };
  source.onerror = () => reconnect(false);
}

function reconnect(immediate) {
  source?.close();
  source = null;
  clearTimeout(stableTimer);
  const delay = immediate ? backoffDelay(0) : backoffDelay(attempt);
  attempt += 1;
  setConnection('warn', `Reconnecting in ${Math.ceil(delay / 1000)} s`);
  banner('warn', 'Connection lost. Showing the last numbers received; reconnecting without losing updates.');
  retryTimer = setTimeout(connect, delay);
}

function select(newTopic) {
  if (newTopic === topic) return;
  topic = newTopic;
  for (const button of document.querySelectorAll('[data-scope]')) {
    button.classList.toggle('active', button.dataset.scope === topic);
  }
  source?.close();
  attempt = 0;
  render();
  connect();
}

// Controls.
const mun = $('mun');
mun.append(new Option('Municipality…', ''));
for (let i = 1; i <= 40; i += 1) mun.append(new Option(`Municipality ${i}`, `mun:${i}`));
mun.addEventListener('change', () => mun.value && select(mun.value));
document.querySelector('[data-scope="dep"]').addEventListener('click', () => {
  mun.value = '';
  select('dep');
});
const goToStation = () => {
  const id = Number($('pto').value);
  if (Number.isInteger(id) && id >= 1 && id <= 1842) select(`pto:${id}`);
};
$('pto-go').addEventListener('click', goToStation);
$('pto').addEventListener('keydown', (event) => event.key === 'Enter' && goToStation());
window.addEventListener('online', () => {
  if (!source) connect();
});

fetch('/whoami').then((r) => r.json()).then((body) => { $('instance').textContent = body.instance; }).catch(() => {});
observeInp((state) => {
  $('inp').textContent = state.inp === null ? '–' : `${Math.round(state.inp)} ms (${state.interactions})`;
});
render();
connect();
