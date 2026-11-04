// Interaction to Next Paint, measured in the page with the Event Timing API
// (same definition as web-vitals: the worst interaction, or the 98th
// percentile when there are 50 or more). Exposed on window.__inp so the load
// measurement script can read it.
export function observeInp(onChange = () => {}) {
  const byInteraction = new Map();
  const state = { inp: null, interactions: 0, supported: 'PerformanceEventTiming' in globalThis };
  globalThis.__inp = state;
  if (!state.supported) return state;

  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      if (!entry.interactionId) continue;
      byInteraction.set(entry.interactionId, Math.max(byInteraction.get(entry.interactionId) ?? 0, entry.duration));
    }
    const durations = [...byInteraction.values()].sort((a, b) => b - a);
    state.interactions = durations.length;
    state.inp = durations[Math.min(durations.length - 1, Math.floor(durations.length / 50))] ?? null;
    onChange(state);
  });
  observer.observe({ type: 'event', durationThreshold: 16, buffered: true });
  return state;
}
