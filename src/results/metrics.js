// Per-instance metrics (RT-4 / RT-7). Exposed on /metrics and published to
// Redis every second with a short TTL, so all instances can be read without
// them talking to each other.
import { createHistogram, monitorEventLoopDelay } from 'node:perf_hooks';

const NS_PER_MS = 1e6;
const round = (value) => Math.round(value * 100) / 100;

export function createMetrics({
  instanceId,
  mode,
  registry,
  redis,
  keys,
  resolutionMs = 10,
  publishMs = 1000,
  ttlSeconds = 5,
  live = () => ({}),
  logger = console
}) {
  // monitorEventLoopDelay records the interval between firings of its internal
  // timer, which includes the resolution itself: it is subtracted. reset()
  // drops the next sample, so a second histogram that is never reset keeps the
  // delay since start-up.
  const loopSecond = monitorEventLoopDelay({ resolution: resolutionMs });
  const loopTotal = monitorEventLoopDelay({ resolution: resolutionMs });
  const delaySecond = createHistogram();
  const delayTotal = createHistogram();

  const counters = {
    events: 0,
    messages: 0,
    naive: 0,
    dropped: 0,
    reconnections: { replay: 0, snapshot: 0, initial: 0, recovery: 0 },
    slow: { degraded: 0, recovered: 0, disconnected: 0 }
  };
  let messagesLastSecond = 0;
  let eventsLastMinute = [];
  let cpuLast = process.cpuUsage();
  let cpuLastAt = process.hrtime.bigint();
  let last = null;
  let timer = null;

  // CPU of this process since the previous sample, as % of one core.
  function cpuPercent() {
    const now = process.hrtime.bigint();
    const usage = process.cpuUsage(cpuLast);
    const elapsedUs = Number(now - cpuLastAt) / 1000;
    cpuLast = process.cpuUsage();
    cpuLastAt = now;
    return elapsedUs > 0 ? round(((usage.user + usage.system) / elapsedUs) * 100) : 0;
  }

  function transports() {
    const counts = {};
    for (const conn of registry.connections) counts[conn.transport] = (counts[conn.transport] ?? 0) + 1;
    return counts;
  }

  function loopDelay(histogram) {
    const ms = (value) => round(Math.max(0, value / NS_PER_MS - resolutionMs));
    return {
      p50: ms(histogram.percentile(50)),
      p99: ms(histogram.percentile(99)),
      max: ms(histogram.max)
    };
  }

  function percentiles(histogram) {
    return histogram.count === 0
      ? { p50: null, p99: null }
      : { p50: histogram.percentile(50), p99: histogram.percentile(99) };
  }

  function sample() {
    const memory = process.memoryUsage();
    const connections = registry.connections.size;
    const now = Date.now();
    eventsLastMinute.push([now, counters.events]);
    eventsLastMinute = eventsLastMinute.filter(([ts]) => now - ts <= 60_000);
    const snapshot = {
      instance: instanceId,
      mode,
      ts: now,
      connections,
      transports: transports(),
      topics: registry.topicCount,
      subscribers: registry.counts(),
      eventLoop: loopDelay(loopSecond),
      eventLoopTotal: loopDelay(loopTotal),
      messages: {
        perSecond: counters.messages - messagesLastSecond,
        total: counters.messages,
        naiveTotal: counters.naive,
        dropped: counters.dropped
      },
      events: counters.events,
      // Events applied to the totals in the last 60 s (new actas, corrections
      // and approved actas): the "actas per minute" of the dashboard.
      eventsPerMinute: counters.events - eventsLastMinute[0][1],
      publishDelay: {
        ...percentiles(delaySecond),
        total: percentiles(delayTotal),
        samples: delayTotal.count
      },
      reconnections: { ...counters.reconnections },
      slow: { ...counters.slow },
      cpuPercent: cpuPercent(),
      memory: {
        rssMB: round(memory.rss / 2 ** 20),
        heapMB: round(memory.heapUsed / 2 ** 20),
        // Approximation: whole-process heap divided by open connections.
        heapPerConnectionKB: connections > 0 ? round(memory.heapUsed / 1024 / connections) : null
      },
      ...live()
    };
    messagesLastSecond = counters.messages;
    loopSecond.reset();
    delaySecond.reset();
    return snapshot;
  }

  async function publish() {
    last = sample();
    try {
      await redis.set(keys.metrics(instanceId), JSON.stringify(last), { EX: ttlSeconds });
    } catch (error) {
      logger.error('Could not publish metrics to Redis:', error.message);
    }
  }

  return {
    eventProcessed(connections) {
      counters.events += 1;
      // Naive figure: every acta to every connection of the instance.
      counters.naive += connections;
    },
    messagesSent(n) {
      counters.messages += n;
    },
    dropped(n) {
      counters.dropped += n;
    },
    publishDelay(ms) {
      const value = Math.max(1, Math.round(ms));
      delaySecond.record(value);
      delayTotal.record(value);
    },
    reconnection(kind) {
      counters.reconnections[kind] += 1;
    },
    slow(kind) {
      counters.slow[kind] += 1;
    },
    local() {
      return {
        ...(last ?? {}),
        instance: instanceId,
        mode,
        connections: registry.connections.size,
        topics: registry.topicCount,
        subscribers: registry.counts(),
        messages: { ...last?.messages, total: counters.messages, naiveTotal: counters.naive },
        events: counters.events,
        reconnections: { ...counters.reconnections },
        slow: { ...counters.slow },
        transports: transports(),
        ...live()
      };
    },
    // offsetMs shifts the per-second histogram reset away from the flush, so
    // a flush never starts right after a reset().
    start(offsetMs = 0) {
      loopSecond.enable();
      loopTotal.enable();
      timer = setTimeout(() => {
        timer = setInterval(() => void publish(), publishMs);
      }, offsetMs);
    },
    stop() {
      clearTimeout(timer);
      clearInterval(timer);
      loopSecond.disable();
      loopTotal.disable();
    }
  };
}

// Reads the metrics published by every instance (keys with TTL).
export async function readClusterMetrics(redis, keys) {
  const names = [];
  for await (const key of redis.scanIterator({ MATCH: keys.metricsPattern, COUNT: 100 })) {
    names.push(key);
  }
  const values = names.length > 0 ? await redis.mGet(names) : [];
  const instances = values.filter(Boolean).map((value) => JSON.parse(value))
    .sort((a, b) => a.instance.localeCompare(b.instance));
  const sum = (fn) => instances.reduce((acc, m) => acc + fn(m), 0);
  return {
    instances,
    totals: {
      instances: instances.length,
      connections: sum((m) => m.connections),
      eventsPerInstance: instances.map((m) => m.events),
      messagesSent: sum((m) => m.messages.total),
      naiveMessages: sum((m) => m.messages.naiveTotal),
      messagesPerSecond: sum((m) => m.messages.perSecond),
      reconnections: {
        replay: sum((m) => m.reconnections.replay),
        snapshot: sum((m) => m.reconnections.snapshot),
        initial: sum((m) => m.reconnections.initial)
      },
      slow: {
        degraded: sum((m) => m.slow?.degraded ?? 0),
        recovered: sum((m) => m.slow?.recovered ?? 0),
        disconnected: sum((m) => m.slow?.disconnected ?? 0)
      },
      eventLoopP99Max: Math.max(0, ...instances.map((m) => m.eventLoop.p99))
    }
  };
}
