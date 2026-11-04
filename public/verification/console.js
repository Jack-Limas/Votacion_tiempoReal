// Shared console client: authentication, WebSocket with exponential backoff
// and full jitter, versioned state (snapshot + ordered events) and UI helpers.

const TOKEN_KEY = 'verification.token';
const BACKOFF_BASE_MS = 500;
const BACKOFF_CAP_MS = 30_000;
const STABLE_AFTER_MS = 10_000;

export function getSession() {
  let token;
  try {
    token = sessionStorage.getItem(TOKEN_KEY);
  } catch {
    token = null;
  }
  if (!token) return null;
  try {
    const payload = JSON.parse(atob(token.split('.')[0].replace(/-/g, '+').replace(/_/g, '/')));
    if (payload.exp * 1000 <= Date.now()) return null;
    return { token, ...payload };
  } catch {
    return null;
  }
}

export function saveToken(token) {
  try { sessionStorage.setItem(TOKEN_KEY, token); } catch { /* private mode */ }
}

export function logout() {
  try { sessionStorage.removeItem(TOKEN_KEY); } catch { /* ignore */ }
  location.href = './';
}

export function requireSession(role) {
  const session = getSession();
  if (!session || session.role !== role) {
    location.replace('./');
    return null;
  }
  return session;
}

// Full jitter: uniform in [0, min(cap, base * 2^attempt)]. Spreads a mass
// reconnection (instance death, mobile network blip) over time instead of
// synchronising every client on the same retry instant.
export function backoffDelay(attempt, random = Math.random) {
  return Math.round(random() * Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt));
}

export const $ = (selector, root = document) => root.querySelector(selector);

export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

export function initials(name = '') {
  return name.split(/\s+/).filter(Boolean).slice(0, 2).map((p) => p[0].toUpperCase()).join('') || '?';
}

export function formatDuration(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return h > 0 ? `${h}h ${String(m).padStart(2, '0')}m` : `${m}:${String(s).padStart(2, '0')}`;
}

export function parseActa(c) {
  try {
    return typeof c.acta === 'string' ? JSON.parse(c.acta) : c.acta ?? {};
  } catch {
    return {};
  }
}

export function toast(message, kind = 'info', ms = 3500) {
  let host = $('.toasts');
  if (!host) {
    host = el('div', { class: 'toasts', role: 'status', 'aria-live': 'polite' });
    document.body.append(host);
  }
  const node = el('div', { class: `toast ${kind}` }, message);
  host.append(node);
  setTimeout(() => node.remove(), ms);
}

// Renders the connection pill + banners from the connection state.
export function renderConnection(state) {
  const pill = $('#conn');
  const banner = $('#banner');
  if (!pill || !banner) return;
  let cls = 'warn';
  let text = 'Connecting…';
  if (state.socket === 'open' && state.redis !== 'down') { cls = 'ok'; text = `Live · ${state.instance ?? ''}`; }
  else if (state.socket === 'open') { cls = 'bad'; text = 'Degraded'; }
  else if (state.socket === 'waiting') { cls = 'warn'; text = `Reconnecting in ${Math.ceil(state.retryInMs / 1000)}s`; }
  pill.className = `pill ${cls}`;
  pill.replaceChildren(el('span', { class: 'dot' }), text);

  if (state.socket === 'open' && state.redis === 'down') {
    banner.className = 'banner bad';
    banner.replaceChildren(
      el('strong', {}, 'Degraded mode.'),
      ` Redis is not responding since ${new Date(state.redisSince).toLocaleTimeString()}. `,
      'Taking, releasing and resolving cases are disabled; nothing you click is silently lost. Your current case is kept until the lease expires.'
    );
  } else if (state.socket === 'waiting' || state.socket === 'connecting') {
    banner.className = 'banner warn';
    banner.replaceChildren(
      el('strong', {}, 'Connection lost.'),
      state.socket === 'waiting'
        ? ` Retrying in ${Math.ceil(state.retryInMs / 1000)}s (attempt ${state.attempt}). `
        : ' Connecting… ',
      'The view will be refreshed with a full snapshot when the link is back.'
    );
  } else {
    banner.className = 'banner hidden';
  }
}

export function connectConsole({ session, onHello, onSnapshot, onEvent, onMessage, onState }) {
  const state = { socket: 'connecting', redis: 'unknown', redisSince: null, instance: null, attempt: 0, retryInMs: 0 };
  let ws = null;
  let version = null;
  let buffer = [];
  let stableTimer = null;
  let retryTimer = null;
  let countdown = null;
  let closedByUser = false;

  const emitState = () => onState?.({ ...state });

  function send(payload) {
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }

  function requestSnapshot() {
    version = null;
    buffer = [];
    send({ type: 'snapshot' });
  }

  function applyEvent(event) {
    if (version === null) { buffer.push(event); return; }
    if (event.version <= version) return;
    if (event.version !== version + 1) { requestSnapshot(); return; }
    version = event.version;
    onEvent?.(event);
  }

  function handle(message) {
    switch (message.type) {
      case 'hello':
        state.instance = message.instance;
        state.redis = message.system.redis;
        state.redisSince = message.system.since;
        emitState();
        onHello?.(message);
        break;
      case 'snapshot': {
        version = message.version;
        onSnapshot?.(message);
        const pending = buffer.sort((a, b) => a.version - b.version);
        buffer = [];
        for (const event of pending) applyEvent(event);
        break;
      }
      case 'event':
        if (message.version) applyEvent(message);
        break;
      case 'system':
        state.redis = message.redis;
        state.redisSince = message.since;
        emitState();
        break;
      default:
        onMessage?.(message);
    }
  }

  function scheduleReconnect() {
    const delay = backoffDelay(state.attempt);
    state.attempt += 1;
    state.socket = 'waiting';
    const until = Date.now() + delay;
    state.retryInMs = delay;
    emitState();
    clearInterval(countdown);
    countdown = setInterval(() => {
      state.retryInMs = Math.max(0, until - Date.now());
      emitState();
    }, 250);
    retryTimer = setTimeout(() => { clearInterval(countdown); open(); }, delay);
  }

  function open() {
    if (!getSession()) { logout(); return; }
    state.socket = 'connecting';
    emitState();
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${scheme}://${location.host}/ws/verification`, ['verification.v1', `auth.${session.token}`]);
    ws.onopen = () => {
      state.socket = 'open';
      emitState();
      clearTimeout(stableTimer);
      // Only reset the backoff once the connection proved stable.
      stableTimer = setTimeout(() => { state.attempt = 0; }, STABLE_AFTER_MS);
    };
    ws.onmessage = (event) => {
      try { handle(JSON.parse(event.data)); } catch (error) { console.error(error); }
    };
    ws.onclose = () => {
      clearTimeout(stableTimer);
      version = null;
      buffer = [];
      if (!closedByUser) scheduleReconnect();
    };
    ws.onerror = () => { /* onclose follows */ };
  }

  window.addEventListener('online', () => {
    if (state.socket === 'waiting') {
      clearTimeout(retryTimer);
      clearInterval(countdown);
      // Small jitter even on 'online' so a whole office does not reconnect at once.
      retryTimer = setTimeout(open, backoffDelay(1));
    }
  });

  open();
  return {
    send,
    requestSnapshot,
    close() { closedByUser = true; ws?.close(); },
    get state() { return { ...state }; }
  };
}

export function renderHeaderUser(session) {
  const who = $('#who');
  if (!who) return;
  who.replaceChildren(
    el('div', { style: 'text-align:right' },
      el('div', { style: 'font-weight:600' }, session.name),
      el('div', { class: 'muted mono', style: 'font-size:11px' }, `${session.sub} · ${session.role}`)),
    el('div', { class: 'avatar' }, initials(session.name)),
    el('button', { class: 'btn small ghost', onclick: logout, title: 'Sign out' }, 'Sign out')
  );
}
