// Moves doubtful actas from the registration stream into the priority queue.
//
// Every instance joins the same consumer group, so each stream entry is
// delivered to exactly one instance. An entry is acknowledged only after the
// INGEST script committed it; if an instance dies in between, the entry stays
// pending and another instance takes it over with XAUTOCLAIM. INGEST is
// idempotent per case id, so a re-delivery never creates a second case.

const CLAIM_IDLE_MS = 30_000;
const CLAIM_EVERY_MS = 10_000;

export function caseIdFor(acta) {
  return `${acta.acta}#v${acta.version ?? 1}`;
}

export function createIngestor({ client, store, streamKey, group, consumer, logger = console }) {
  let running = false;
  let loop = null;
  let lastAutoClaim = 0;
  const stats = { ingested: 0, duplicates: 0, malformed: 0 };

  async function ensureGroup() {
    try {
      await client.xGroupCreate(streamKey, group, '0', { MKSTREAM: true });
    } catch (error) {
      if (!String(error?.message).includes('BUSYGROUP')) throw error;
    }
  }

  async function handle(entry) {
    if (!entry) return;
    const { id, message } = entry;
    let acta;
    try {
      acta = JSON.parse(message.acta);
    } catch {
      acta = null;
    }
    if (!acta || typeof acta.acta !== 'string') {
      stats.malformed += 1;
      logger.error(`Verification ingest: malformed stream entry ${id}; acknowledging it.`);
      await client.xAck(streamKey, group, id);
      return;
    }
    const votes = acta.votos && typeof acta.votos === 'object' ? Object.values(acta.votos) : [];
    const result = await store.ingest({
      caseId: caseIdFor(acta),
      acta,
      idem: message.idem,
      sufragantes: acta.sufragantes,
      totalVotos: votes.reduce((sum, value) => sum + (Number(value) || 0), 0)
    });
    if (result.ok) stats.ingested += 1;
    else stats.duplicates += 1;
    await client.xAck(streamKey, group, id);
  }

  async function tick() {
    if (Date.now() - lastAutoClaim > CLAIM_EVERY_MS) {
      lastAutoClaim = Date.now();
      const claimed = await client.xAutoClaim(streamKey, group, consumer, CLAIM_IDLE_MS, '0-0', { COUNT: 50 });
      for (const entry of claimed?.messages ?? []) await handle(entry);
    }
    // '0' re-reads this consumer's own pending entries (after a crash and
    // restart with the same INSTANCE_ID); '>' reads new ones.
    for (const id of ['0', '>']) {
      const response = await client.xReadGroup(
        group,
        consumer,
        { key: streamKey, id },
        id === '>' ? { COUNT: 50, BLOCK: 1000 } : { COUNT: 50 }
      );
      for (const stream of response ?? []) {
        for (const entry of stream.messages) await handle(entry);
      }
    }
  }

  async function run() {
    let backoff = 250;
    while (running) {
      try {
        if (!client.isOpen) await client.connect();
        await ensureGroup();
        while (running) {
          await tick();
          backoff = 250;
        }
      } catch (error) {
        if (!running) break;
        logger.error('Verification ingest error; retrying:', error?.message ?? error);
        await new Promise((resolve) => setTimeout(resolve, backoff));
        backoff = Math.min(backoff * 2, 5_000);
      }
    }
  }

  return {
    stats,
    start() {
      if (running) return;
      running = true;
      loop = run();
    },
    async stop() {
      running = false;
      if (client.isOpen) {
        try {
          await client.disconnect();
        } catch {
          // already closing
        }
      }
      await loop;
    },
    // Exposed for tests: process whatever is available right now.
    async drainOnce() {
      if (!client.isOpen) await client.connect();
      await ensureGroup();
      await tick();
    }
  };
}
