import { createHash } from 'node:crypto';
import http from 'node:http';
import { compareIds } from '../../src/results/stream.js';

export function subParam(subs) {
  return subs.map(({ topic, cursor }) => (cursor ? `${topic}@${cursor}` : topic)).join(',');
}

// Opens an SSE connection (no EventSource: the caller handles reconnection).
export function connect(baseUrl, subs, { onMessage = () => {}, onClose = () => {}, parse = true } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(`/stream?sub=${subParam(subs)}`, baseUrl);
    const req = http.get(url, { agent: false }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`SSE answered ${res.statusCode}`));
        return;
      }
      res.setEncoding('utf8');
      let buffer = '';
      res.on('data', (chunk) => {
        buffer += chunk;
        let end = buffer.indexOf('\n\n');
        while (end !== -1) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          // Heartbeats only keep the connection alive; callers never see them.
          if (block.startsWith('data: ') && block !== 'data: {"t":"hb"}') {
            onMessage(parse ? JSON.parse(block.slice(6)) : block);
          }
          end = buffer.indexOf('\n\n');
        }
      });
      res.on('close', onClose);
      resolve({
        instance: res.headers['x-instance-id'],
        response: res,
        close: () => req.destroy()
      });
    });
    req.on('error', reject);
  });
}

// Public client state: snapshot + deltas carrying absolute values.
export function createClientState() {
  const topics = new Map();

  return {
    apply(message) {
      const current = topics.get(message.topic);
      if (message.t === 'snapshot') {
        topics.set(message.topic, { cursor: message.upTo, state: { ...message.state } });
        return true;
      }
      if (message.t !== 'delta') return false;
      if (current?.cursor && compareIds(message.upTo, current.cursor) <= 0) return false;
      const state = current?.state ?? {};
      Object.assign(state, message.changes);
      topics.set(message.topic, { cursor: message.upTo, state });
      return true;
    },
    cursors() {
      return [...topics].map(([topic, { cursor }]) => ({ topic, cursor }));
    },
    state(topic) {
      return topics.get(topic)?.state;
    },
    hash(topic) {
      const state = topics.get(topic)?.state ?? {};
      const canonical = JSON.stringify(Object.keys(state).sort().map((k) => [k, state[k]]));
      return createHash('sha256').update(canonical).digest('hex');
    }
  };
}
