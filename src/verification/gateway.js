import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { APP_PROTOCOL, tokenFromUpgrade, verifyToken } from './auth.js';
import { MAX_MESSAGE_BYTES, parseClientMessage } from './messages.js';
import { VerificationUnavailableError } from './store.js';

const SLOW_CONSOLE_BYTES = 512 * 1024;
const PRESENCE_PUSH_MS = 5_000;
const IDLE_WARNING_MS = 60_000;

function rejectUpgrade(socket, status, reason) {
  socket.write(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}

export function createVerificationGateway({ config, store, health, subscriber, logger = console }) {
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: MAX_MESSAGE_BYTES,
    perMessageDeflate: false,
    handleProtocols: (protocols) => (protocols.has(APP_PROTOCOL) ? APP_PROTOCOL : false)
  });

  /** @type {Map<string, {ws: WebSocket, user: any, connId: string, alive: boolean, lastActivity: number, connectedAt: number, stale: boolean, localWindow: {start: number, count: number}}>} */
  const conns = new Map();
  /** auditorId -> { caseId, token } for leases this instance must keep renewing. */
  const held = new Map();
  const metrics = { messagesSent: 0, messagesReceived: 0, rateLimited: 0, leaseRenewals: 0, leasesLost: 0 };
  let heartbeatTimer = null;
  let sweepTimer = null;
  let presenceTimer = null;
  let presenceDebounce = null;
  let unsubscribeHealth = null;

  function send(conn, payload) {
    if (conn.ws.readyState !== WebSocket.OPEN) return;
    // Back-pressure: a console that cannot keep up stops receiving
    // incremental events and gets a full snapshot once it drains.
    if (conn.ws.bufferedAmount > SLOW_CONSOLE_BYTES && payload.type === 'event') {
      conn.stale = true;
      return;
    }
    conn.ws.send(JSON.stringify(payload));
    metrics.messagesSent += 1;
  }

  function broadcast(payload, filter = () => true) {
    for (const conn of conns.values()) if (filter(conn)) send(conn, payload);
  }

  function auditorConns(auditorId) {
    return [...conns.values()].filter((conn) => conn.user.role === 'auditor' && conn.user.sub === auditorId);
  }

  function failure(conn, action, reqId, error) {
    if (error instanceof VerificationUnavailableError) {
      health.reportFailure(error.cause ?? error);
      send(conn, { type: 'result', action, reqId, ok: false, code: 'redis_unavailable', message: error.message });
      return;
    }
    logger.error(`Verification ${action} failed:`, error);
    send(conn, { type: 'result', action, reqId, ok: false, code: 'internal_error', message: 'Unexpected server error.' });
  }

  async function sendSnapshot(conn) {
    try {
      const snapshot = await store.snapshot();
      conn.stale = false;
      send(conn, { type: 'snapshot', instance: config.instanceId, ...snapshot });
    } catch (error) {
      failure(conn, 'snapshot', undefined, error);
    }
  }

  async function sendPresence(targets) {
    if (targets.length === 0) return;
    try {
      const presence = await store.listPresence();
      for (const conn of targets) send(conn, { type: 'presence', ...presence });
    } catch (error) {
      if (!(error instanceof VerificationUnavailableError)) logger.error('Presence push failed:', error);
    }
  }

  const coordinators = () => [...conns.values()].filter((conn) => conn.user.role === 'coordinator');

  function schedulePresencePush() {
    clearTimeout(presenceDebounce);
    presenceDebounce = setTimeout(() => void sendPresence(coordinators()), 250);
  }

  function touchPresence(conn) {
    return store.touchPresence(conn.connId, {
      connId: conn.connId,
      auditorId: conn.user.sub,
      name: conn.user.name,
      role: conn.user.role,
      instance: config.instanceId,
      connectedAt: conn.connectedAt,
      lastActivity: conn.lastActivity
    });
  }

  async function rateLimited(conn) {
    const now = Date.now();
    try {
      const verdict = await store.rateLimit('ws', conn.user.sub, config.rateLimitWindowMs, config.rateLimitWsMessages);
      if (verdict.allowed) return null;
      return verdict.retryAfterMs;
    } catch {
      // Redis is down: fall back to a per-connection local window so a
      // misbehaving client still cannot flood this instance.
      if (now - conn.localWindow.start >= config.rateLimitWindowMs) conn.localWindow = { start: now, count: 0 };
      conn.localWindow.count += 1;
      return conn.localWindow.count > config.rateLimitWsMessages
        ? config.rateLimitWindowMs - (now - conn.localWindow.start)
        : null;
    }
  }

  async function onMessage(conn, raw) {
    metrics.messagesReceived += 1;
    const retryAfterMs = await rateLimited(conn);
    if (retryAfterMs !== null) {
      metrics.rateLimited += 1;
      send(conn, {
        type: 'error',
        code: 'rate_limited',
        retryAfterMs,
        message: `Too many messages. Limit is ${config.rateLimitWsMessages} per ${config.rateLimitWindowMs / 1000}s.`
      });
      return;
    }
    const parsed = parseClientMessage(raw, conn.user.role);
    if (parsed.error) {
      send(conn, { type: 'error', code: parsed.error, message: parsed.message });
      return;
    }
    const { message } = parsed;
    const { reqId } = message;
    const user = conn.user;
    if (user.role === 'auditor') conn.lastActivity = Date.now();

    try {
      switch (message.type) {
        case 'activity':
          return;
        case 'snapshot':
          return void (await sendSnapshot(conn));
        case 'presence':
          return void (await sendPresence([conn]));
        case 'claim': {
          const result = await store.claim(user.sub, user.name, message.caseId ?? '');
          if (result.ok || result.code === 'already_holding') {
            held.set(user.sub, { caseId: result.case.id, token: result.case.token });
          }
          return send(conn, { type: 'result', action: 'claim', reqId, ...result });
        }
        case 'release': {
          const result = await store.release(message.caseId, user.sub, message.token);
          // Success or any ownership failure: either way this lease is no longer ours.
          dropHeld(user.sub, message.caseId);
          return send(conn, { type: 'result', action: 'release', reqId, caseId: message.caseId, ...result });
        }
        case 'resolve': {
          const result = await store.resolve(message.caseId, user.sub, message.token, message.decision, message.note ?? '');
          // Success or any ownership failure: either way this lease is no longer ours.
          dropHeld(user.sub, message.caseId);
          return send(conn, { type: 'result', action: 'resolve', reqId, caseId: message.caseId, ...result });
        }
        default:
          return undefined;
      }
    } catch (err) {
      failure(conn, message.type, reqId, err);
    }
  }

  function dropHeld(auditorId, caseId) {
    if (held.get(auditorId)?.caseId === caseId) held.delete(auditorId);
  }

  function onConnection(ws, user) {
    const conn = {
      ws,
      user,
      connId: randomUUID(),
      alive: true,
      lastActivity: Date.now(),
      connectedAt: Date.now(),
      stale: false,
      localWindow: { start: Date.now(), count: 0 }
    };
    conns.set(conn.connId, conn);

    ws.on('pong', () => {
      conn.alive = true;
    });
    ws.on('message', (raw) => void onMessage(conn, raw));
    ws.on('error', (error) => logger.error('Verification socket error:', error.message));
    ws.on('close', () => {
      conns.delete(conn.connId);
      // The lease is NOT released here: a network blip must not cost the
      // auditor the case. Without renewals it expires after leaseTtlMs and
      // the sweeper returns it to the queue.
      if (user.role === 'auditor' && auditorConns(user.sub).length === 0) held.delete(user.sub);
      store.removePresence(conn.connId).catch(() => {});
    });

    send(conn, {
      type: 'hello',
      instance: config.instanceId,
      connId: conn.connId,
      user: { sub: user.sub, name: user.name, role: user.role },
      settings: { leaseTtlMs: config.leaseTtlMs, idleLimitMs: config.idleLimitMs, heartbeatMs: config.heartbeatMs },
      system: health.snapshot()
    });

    void (async () => {
      await sendSnapshot(conn);
      try {
        await touchPresence(conn);
        if (user.role === 'auditor') {
          // Adopt a lease the auditor already holds (reconnect through any
          // instance: no sticky sessions needed).
          const snap = await store.snapshot(0);
          const mine = snap.claimed.find((c) => c.owner === user.sub);
          if (mine) held.set(user.sub, { caseId: mine.id, token: mine.token });
        }
      } catch {
        // Health monitor already reports the outage to the console.
      }
      if (user.role === 'coordinator') await sendPresence([conn]);
    })();
  }

  async function heartbeat() {
    const now = Date.now();
    for (const conn of conns.values()) {
      if (!conn.alive) {
        // No pong since the previous beat: half-open TCP (laptop without
        // battery, dead mobile link). Terminate so presence is cleaned up.
        conn.ws.terminate();
        continue;
      }
      conn.alive = false;
      conn.ws.ping();
      if (conn.stale && conn.ws.bufferedAmount < SLOW_CONSOLE_BYTES / 4) void sendSnapshot(conn);
    }
    if (health.status === 'down') return;

    await Promise.allSettled([...conns.values()].map((conn) => touchPresence(conn)));

    for (const [auditorId, lease] of held) {
      const local = auditorConns(auditorId);
      const alive = local.filter((conn) => conn.ws.readyState === WebSocket.OPEN);
      if (alive.length === 0) {
        held.delete(auditorId);
        continue;
      }
      const lastActivity = Math.max(...alive.map((conn) => conn.lastActivity));
      const idleMs = now - lastActivity;
      if (idleMs >= config.idleLimitMs) {
        // Connected but inactive (lunch break): stop renewing so the lease
        // expires and the case goes back to the queue.
        held.delete(auditorId);
        for (const conn of alive) {
          send(conn, { type: 'lease_lost', caseId: lease.caseId, reason: 'idle', message: 'Case returned to the queue after inactivity.' });
        }
        continue;
      }
      if (idleMs >= config.idleLimitMs - IDLE_WARNING_MS) {
        for (const conn of alive) {
          send(conn, { type: 'idle_warning', caseId: lease.caseId, releaseInMs: config.idleLimitMs - idleMs });
        }
      }
      try {
        const result = await store.renew(lease.caseId, auditorId, lease.token);
        if (result.ok) {
          metrics.leaseRenewals += 1;
          for (const conn of alive) send(conn, { type: 'lease', caseId: lease.caseId, leaseExpiresAt: result.leaseExpiresAt });
        } else if (held.get(auditorId) === lease) {
          held.delete(auditorId);
          metrics.leasesLost += 1;
          for (const conn of alive) {
            send(conn, { type: 'lease_lost', caseId: lease.caseId, reason: result.code, message: 'Your lease is no longer valid.' });
          }
        }
      } catch {
        // Redis down: keep the entry and retry on the next beat. When Redis
        // is back, the renewal either succeeds or reports lease_expired.
      }
    }
  }

  function onEvent(raw) {
    let event;
    try {
      event = JSON.parse(raw);
    } catch {
      return;
    }
    if (event.type === 'presence.changed') {
      schedulePresencePush();
      return;
    }
    const c = event.case;
    if (c && (event.type === 'case.released' || event.type === 'case.expired' || event.type === 'case.resolved')) {
      const owner = event.previousOwner ?? c.resolvedBy;
      if (owner && held.get(owner)?.caseId === c.id) held.delete(owner);
      if (event.type === 'case.expired' && owner) {
        for (const conn of auditorConns(owner)) {
          send(conn, { type: 'lease_lost', caseId: c.id, reason: 'expired', message: 'Lease expired; the case went back to the queue.' });
        }
      }
    }
    broadcast({ ...event, type: 'event', event: event.type });
    if (event.type.startsWith('case.')) schedulePresencePush();
  }

  return {
    wss,
    metrics,
    get connections() {
      return conns.size;
    },
    handleUpgrade(request, socket, head) {
      const user = verifyToken(config.tokenSecret, tokenFromUpgrade(request));
      if (!user) return rejectUpgrade(socket, 401, 'Unauthorized');
      wss.handleUpgrade(request, socket, head, (ws) => onConnection(ws, user));
      return undefined;
    },
    async start() {
      await subscriber.subscribe(store.keys.events, onEvent);
      unsubscribeHealth = health.onChange((state) => {
        broadcast({ type: 'system', ...state });
        // Pub/Sub has no memory: anything published while we were cut off
        // is lost, so every console gets a fresh snapshot on recovery.
        if (state.redis === 'up') for (const conn of conns.values()) void sendSnapshot(conn);
      });
      heartbeatTimer = setInterval(() => void heartbeat(), config.heartbeatMs);
      sweepTimer = setInterval(() => {
        if (health.status !== 'down') store.sweep().catch(() => {});
      }, config.sweepIntervalMs);
      presenceTimer = setInterval(() => void sendPresence(coordinators()), PRESENCE_PUSH_MS);
    },
    async stop() {
      clearInterval(heartbeatTimer);
      clearInterval(sweepTimer);
      clearInterval(presenceTimer);
      clearTimeout(presenceDebounce);
      unsubscribeHealth?.();
      for (const conn of conns.values()) conn.ws.terminate();
      await Promise.allSettled([...conns.values()].map((conn) => store.removePresence(conn.connId)));
      conns.clear();
      try {
        if (subscriber.isOpen) await subscriber.unsubscribe(store.keys.events, onEvent);
      } catch {
        // subscriber already closed
      }
      await new Promise((resolve) => wss.close(() => resolve()));
    },
    // For tests.
    heartbeat,
    held
  };
}
