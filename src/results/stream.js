// Lector del stream global st:eventos. Cada instancia lee TODOS los eventos
// (sin grupos de consumidores) con XREAD BLOCK en una conexión dedicada.

// Los IDs de stream se comparan de forma numérica (ms y secuencia), nunca como
// texto: '1700000000000-10' es mayor que '1700000000000-9'.
export function parseId(id) {
  const guion = id.indexOf('-');
  return {
    ms: Number(guion === -1 ? id : id.slice(0, guion)),
    seq: guion === -1 ? 0 : Number(id.slice(guion + 1))
  };
}

export function compareIds(a, b) {
  const x = typeof a === 'string' ? parseId(a) : a;
  const y = typeof b === 'string' ? parseId(b) : b;
  if (x.ms !== y.ms) return x.ms < y.ms ? -1 : 1;
  if (x.seq !== y.seq) return x.seq < y.seq ? -1 : 1;
  return 0;
}

export function decodeEntry({ id, message }) {
  return { id, evento: JSON.parse(message.e) };
}

export function createStreamReader({
  redis,
  streamKey,
  onEntries,
  blockMs = 5000,
  count = 200,
  logger = console
}) {
  let client = null;
  let running = false;
  let loop = null;
  let lastId = null;

  async function readLoop() {
    while (running) {
      try {
        // '$' solo en la primera lectura; después, siempre el último ID recibido.
        // La primera lectura bloquea sin límite (BLOCK 0) para no tener que
        // repetir '$' tras un timeout, lo que perdería eventos.
        const reply = await client.xRead(
          { key: streamKey, id: lastId ?? '$' },
          { BLOCK: lastId ? blockMs : 0, COUNT: count }
        );
        if (!reply) continue;
        const entries = [];
        for (const stream of reply) {
          for (const message of stream.messages) {
            entries.push(decodeEntry(message));
          }
        }
        if (entries.length === 0) continue;
        lastId = entries[entries.length - 1].id;
        await onEntries(entries);
      } catch (error) {
        if (!running) break;
        logger.error('Error leyendo el stream de eventos:', error);
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  }

  return {
    get lastId() {
      return lastId;
    },
    async start() {
      client = redis.duplicate();
      client.on('error', (error) => logger.error('Error del cliente Redis del stream:', error));
      await client.connect();
      running = true;
      loop = readLoop();
    },
    async stop() {
      running = false;
      if (client?.isOpen) {
        await client.disconnect();
      }
      await loop;
    }
  };
}
