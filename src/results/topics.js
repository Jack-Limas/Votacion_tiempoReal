// Topic registry (RF-5).
//   topics: Map<topic, Set<connection>>, and on each connection a Set<topic>
//   plus one cursor per topic. Subscribe O(1), unsubscribe O(1) per topic,
//   delivery O(k) with k = subscribers of the topic. Delivering an event never
//   walks all the connections of the instance.

const TOPIC_RE = /^(dep|mun:\d{1,6}|pto:\d{1,6})$/;
const CURSOR_RE = /^\d{1,20}-\d{1,20}$/;
const EMPTY = new Set();

// Cursor of a topic whose resume (snapshot or replay) has not finished yet.
export const PENDING = Symbol('pending');

let sequence = 0;

// A connection knows nothing about the transport: it only exposes send(string)
// and, optionally, bufferedBytes() for back-pressure.
export function createConnection(send, { bufferedBytes = () => 0 } = {}) {
  sequence += 1;
  return {
    id: sequence,
    send,
    bufferedBytes,
    topics: new Set(),
    cursors: new Map(),
    pending: null
  };
}

export function createTopicRegistry() {
  const topics = new Map();
  const connections = new Set();

  function unsubscribe(conn, topic) {
    const subscribers = topics.get(topic);
    if (subscribers) {
      subscribers.delete(conn);
      if (subscribers.size === 0) {
        topics.delete(topic);
      }
    }
    conn.topics.delete(topic);
    conn.cursors.delete(topic);
  }

  return {
    connections,
    add(conn) {
      connections.add(conn);
    },
    subscribe(conn, topic, cursor = null) {
      let subscribers = topics.get(topic);
      if (!subscribers) {
        subscribers = new Set();
        topics.set(topic, subscribers);
      }
      subscribers.add(conn);
      conn.topics.add(topic);
      conn.cursors.set(topic, cursor);
    },
    unsubscribe,
    remove(conn) {
      for (const topic of [...conn.topics]) {
        unsubscribe(conn, topic);
      }
      connections.delete(conn);
    },
    subscribers(topic) {
      return topics.get(topic) ?? EMPTY;
    },
    counts() {
      const result = {};
      for (const [topic, subscribers] of topics) {
        result[topic] = subscribers.size;
      }
      return result;
    },
    get topicCount() {
      return topics.size;
    }
  };
}

// "mun:11@1700000000000-0,dep" -> [{ topic: 'mun:11', cursor: '1700000000000-0' }, { topic: 'dep', cursor: null }]
export function parseSubscriptions(raw, { maxTopics = 10 } = {}) {
  if (typeof raw !== 'string' || raw.trim() === '') {
    throw new Error('The sub parameter is required, e.g. sub=mun:11,dep.');
  }
  const result = new Map();
  for (const part of raw.split(',')) {
    const [topic, cursor, ...extra] = part.trim().split('@');
    if (!TOPIC_RE.test(topic) || extra.length > 0) {
      throw new Error(`Invalid topic: ${part}. Use dep, mun:{id} or pto:{id}.`);
    }
    if (cursor !== undefined && !CURSOR_RE.test(cursor)) {
      throw new Error(`Invalid cursor for ${topic}: ${cursor}.`);
    }
    result.set(topic, cursor ?? null);
  }
  if (result.size > maxTopics) {
    throw new Error(`At most ${maxTopics} topics per connection.`);
  }
  return [...result].map(([topic, cursor]) => ({ topic, cursor }));
}
