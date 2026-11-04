// Registro de tópicos (RF-5).
//   topics: Map<topic, Set<conexion>>   y en cada conexión: Set<topic> + cursor por tópico.
//   alta O(1), baja O(1) por tópico, recorrido O(k) con k = suscriptores del tópico.
// Nunca se recorren todas las conexiones de la instancia para entregar un evento.

const TOPIC_RE = /^(dep|mun:\d{1,6}|pto:\d{1,6})$/;
const CURSOR_RE = /^\d{1,20}-\d{1,20}$/;
const VACIO = new Set();

// Cursor de un tópico cuya reanudación (snapshot o replay) todavía no termina.
export const PENDIENTE = Symbol('pendiente');

let secuencia = 0;

// La conexión no sabe de transporte: solo expone send(string).
export function createConnection(send) {
  secuencia += 1;
  return {
    id: secuencia,
    send,
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
      return topics.get(topic) ?? VACIO;
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
    throw new Error('El parámetro sub es obligatorio, p. ej. sub=mun:11,dep.');
  }
  const result = new Map();
  for (const parte of raw.split(',')) {
    const [topic, cursor, ...sobra] = parte.trim().split('@');
    if (!TOPIC_RE.test(topic) || sobra.length > 0) {
      throw new Error(`Tópico inválido: ${parte}. Use dep, mun:{id} o pto:{id}.`);
    }
    if (cursor !== undefined && !CURSOR_RE.test(cursor)) {
      throw new Error(`Cursor inválido para ${topic}: ${cursor}.`);
    }
    result.set(topic, cursor ?? null);
  }
  if (result.size > maxTopics) {
    throw new Error(`Máximo ${maxTopics} tópicos por conexión.`);
  }
  return [...result].map(([topic, cursor]) => ({ topic, cursor }));
}
