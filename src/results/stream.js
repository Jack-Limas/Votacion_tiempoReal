// Reader of the global events stream. Every instance reads ALL events (no
// consumer group) with XREAD BLOCK on a dedicated connection.

// Stream IDs are compared numerically (ms and sequence), never as text:
// '1700000000000-10' is greater than '1700000000000-9'.
export function parseId(id) {
  const dash = id.indexOf('-');
  return {
    ms: Number(dash === -1 ? id : id.slice(0, dash)),
    seq: dash === -1 ? 0 : Number(id.slice(dash + 1))
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
  return { id, event: JSON.parse(message.e) };
}

export function createStreamReader({
  redis,
  streamKey,
  onEntries,
  onStatus = () => {},
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
        // '$' only on the first read, the last received ID afterwards. The
        // first read blocks without limit (BLOCK 0) so '$' is never repeated
        // after a timeout, which would skip events.
        const reply = await client.xRead(
          { key: streamKey, id: lastId ?? '$' },
          { BLOCK: lastId ? blockMs : 0, COUNT: count }
        );
        onStatus('up');
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
        onStatus('down');
        logger.error('Error reading the events stream:', error.message);
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
      client.on('error', (error) => logger.error('Events stream Redis client error:', error.message));
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
