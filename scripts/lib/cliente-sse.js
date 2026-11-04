import { createHash } from 'node:crypto';
import http from 'node:http';
import { compareIds } from '../../src/results/stream.js';

export function subParam(subs) {
  return subs.map(({ topic, cursor }) => (cursor ? `${topic}@${cursor}` : topic)).join(',');
}

// Abre una conexión SSE (sin EventSource: la reconexión la maneja quien llama).
export function conectar(baseUrl, subs, { onMessage = () => {}, onClose = () => {}, parse = true } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(`/stream?sub=${subParam(subs)}`, baseUrl);
    const req = http.get(url, { agent: false }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`SSE respondió ${res.statusCode}`));
        return;
      }
      res.setEncoding('utf8');
      let buffer = '';
      res.on('data', (chunk) => {
        buffer += chunk;
        let fin = buffer.indexOf('\n\n');
        while (fin !== -1) {
          const bloque = buffer.slice(0, fin);
          buffer = buffer.slice(fin + 2);
          if (bloque.startsWith('data: ')) {
            onMessage(parse ? JSON.parse(bloque.slice(6)) : bloque);
          }
          fin = buffer.indexOf('\n\n');
        }
      });
      res.on('close', onClose);
      resolve({
        instancia: res.headers['x-instance-id'],
        close: () => req.destroy()
      });
    });
    req.on('error', reject);
  });
}

// Estado del cliente público: snapshot + deltas con valores absolutos.
export function createEstadoCliente() {
  const topics = new Map();

  return {
    aplicar(mensaje) {
      const actual = topics.get(mensaje.topic);
      if (mensaje.t === 'snapshot') {
        topics.set(mensaje.topic, { cursor: mensaje.upTo, estado: { ...mensaje.estado } });
        return true;
      }
      if (mensaje.t !== 'delta') return false;
      if (actual?.cursor && compareIds(mensaje.upTo, actual.cursor) <= 0) return false;
      const estado = actual?.estado ?? {};
      Object.assign(estado, mensaje.cambios);
      topics.set(mensaje.topic, { cursor: mensaje.upTo, estado });
      return true;
    },
    cursores() {
      return [...topics].map(([topic, { cursor }]) => ({ topic, cursor }));
    },
    estado(topic) {
      return topics.get(topic)?.estado;
    },
    hash(topic) {
      const estado = topics.get(topic)?.estado ?? {};
      const canonico = JSON.stringify(Object.keys(estado).sort().map((k) => [k, estado[k]]));
      return createHash('sha256').update(canonico).digest('hex');
    }
  };
}
