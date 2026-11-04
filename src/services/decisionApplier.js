// Applies auditors' decisions to the results (RF-7 -> RF-3).
//
// The verification RESOLVE script writes every decision to the resolutions
// stream atomically with the lease check, together with its fencing token.
// Every instance joins the same consumer group, so each decision is handled by
// one instance; it is acknowledged only after the apply script committed. If
// an instance dies in between, another one claims the entry (XAUTOCLAIM) and
// applies it again, which is harmless: the apply script is idempotent per
// (case, token) and refuses tokens older than the last one seen.
//
// The two writes live in different hash slots ({verification} and
// {results}), so they cannot be one Lua script on Redis Cluster; the stream
// plus the idempotent apply gives the same exactly-once effect.

const CLAIM_IDLE_MS = 30_000;
const CLAIM_EVERY_MS = 10_000;

export function createDecisionApplier({ client, service, streamKey, group = 'results-apply', consumer, logger = console }) {
  let running = false;
  let loop = null;
  let lastAutoClaim = 0;
  const stats = { applied: 0, recorded: 0, duplicates: 0, stale: 0, malformed: 0 };

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
    let acta = null;
    try {
      acta = JSON.parse(message.acta);
    } catch {
      acta = null;
    }
    if (!acta || !message.case || !/^\d+$/.test(message.token ?? '') || !['approve', 'reject', 'recount'].includes(message.decision)) {
      stats.malformed += 1;
      logger.error(`Decision applier: malformed resolution ${id}; acknowledging it.`);
      await client.xAck(streamKey, group, id);
      return;
    }
    const result = await service.applyVerificationDecision({
      caseId: message.case,
      acta,
      decision: message.decision,
      token: message.token,
      idem: message.idem ?? ''
    });
    if (result.duplicate) stats.duplicates += 1;
    else if (result.status === 'stale_token') stats.stale += 1;
    else if (message.decision === 'approve') stats.applied += 1;
    else stats.recorded += 1;
    await client.xAck(streamKey, group, id);
  }

  async function tick() {
    if (Date.now() - lastAutoClaim > CLAIM_EVERY_MS) {
      lastAutoClaim = Date.now();
      const claimed = await client.xAutoClaim(streamKey, group, consumer, CLAIM_IDLE_MS, '0-0', { COUNT: 50 });
      for (const entry of claimed?.messages ?? []) await handle(entry);
    }
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
        logger.error('Decision applier error; retrying:', error?.message ?? error);
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
    // For tests: process whatever is available now.
    async drainOnce() {
      if (!client.isOpen) await client.connect();
      await ensureGroup();
      await tick();
    }
  };
}
