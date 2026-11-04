// Tracks whether Redis answers, combining client events with an active PING
// that has its own timeout (a hung TCP connection emits no events). Status
// changes are pushed to the consoles so the degraded mode is always visible.
export function createRedisHealth(client, { intervalMs = 2_000, timeoutMs = 1_500, logger = console } = {}) {
  let status = 'unknown';
  let since = Date.now();
  let lastError = null;
  let timer = null;
  const listeners = new Set();

  function set(next, error) {
    if (error) lastError = String(error?.message ?? error);
    if (next === status) return;
    status = next;
    since = Date.now();
    if (next === 'down') logger.error(`Redis health: DOWN (${lastError ?? 'no response'})`);
    else if (next === 'up') logger.log('Redis health: UP');
    for (const listener of listeners) listener(snapshot());
  }

  function snapshot() {
    return { redis: status, since, lastError: status === 'up' ? null : lastError };
  }

  async function probe() {
    let timeout;
    try {
      await Promise.race([
        client.ping(),
        new Promise((_, reject) => {
          timeout = setTimeout(() => reject(new Error('PING timeout')), timeoutMs);
        })
      ]);
      set('up');
    } catch (error) {
      set('down', error);
    } finally {
      clearTimeout(timeout);
    }
  }

  const onError = (error) => set('down', error);
  const onEnd = () => set('down', new Error('connection closed'));
  const onReady = () => void probe();

  return {
    get status() {
      return status;
    },
    snapshot,
    probe,
    onChange(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    // Lets request handlers report failures they observed first.
    reportFailure(error) {
      set('down', error);
    },
    start() {
      client.on('error', onError);
      client.on('end', onEnd);
      client.on('ready', onReady);
      timer = setInterval(() => void probe(), intervalMs);
      timer.unref?.();
      return probe();
    },
    stop() {
      clearInterval(timer);
      client.off('error', onError);
      client.off('end', onEnd);
      client.off('ready', onReady);
    }
  };
}
