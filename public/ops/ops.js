// Operations dashboard (RT-7). Transport: polling /metrics/all every second.
// A handful of operators read an aggregate that changes once per second, so
// a long-lived connection would buy nothing; polling keeps it stateless and
// works through any proxy.
const $ = (id) => document.getElementById(id);
const HISTORY = 120;
const loopHistory = new Map(); // instance -> number[]
const rateHistory = [];
const COLORS = ['#7c5cff', '#22d3ee', '#22c55e', '#f59e0b', '#ef4444', '#e879f9', '#38bdf8', '#a3e635'];

function tile(label, value) {
  const div = document.createElement('div');
  div.className = 'card tile';
  div.innerHTML = '<span></span><b></b>';
  div.querySelector('span').textContent = label;
  div.querySelector('b').textContent = value;
  return div;
}

function draw(canvas, series, { max } = {}) {
  const ctx = canvas.getContext('2d');
  const { width, height } = canvas;
  ctx.clearRect(0, 0, width, height);
  const top = Math.max(max ?? 0, ...series.flatMap(({ values }) => values), 1);
  ctx.strokeStyle = '#262e3f';
  ctx.fillStyle = '#8b95a8';
  ctx.font = '11px system-ui';
  for (const fraction of [0.25, 0.5, 0.75, 1]) {
    const y = height - fraction * (height - 14);
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
    ctx.fillText((top * fraction).toFixed(top < 10 ? 1 : 0), 4, y - 2);
  }
  series.forEach(({ values, color }) => {
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    values.forEach((value, i) => {
      const x = (i / (HISTORY - 1)) * width;
      const y = height - (value / top) * (height - 14);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });
}

function push(list, value) {
  list.push(value);
  if (list.length > HISTORY) list.shift();
}

function render({ instances, totals }) {
  const instancesDown = instances.filter((m) => m.status === 'down').length;
  const conn = $('conn');
  conn.className = `pill ${instancesDown ? 'bad' : 'ok'}`;
  conn.replaceChildren(Object.assign(document.createElement('span'), { className: 'dot' }),
    instancesDown ? `${instancesDown} instance(s) without Redis` : `${totals.instances} instances live`);

  const delays = instances.map((m) => m.publishDelay?.p99).filter((v) => v !== null && v !== undefined);
  $('tiles').replaceChildren(
    tile('Instances', totals.instances),
    tile('Public connections', totals.connections.toLocaleString('en-US')),
    tile('Messages / s', totals.messagesPerSecond.toLocaleString('en-US')),
    tile('Event Loop p99 (max)', `${totals.eventLoopP99Max} ms`),
    tile('Actas / min', instances[0]?.eventsPerMinute ?? 0),
    tile('Publish delay p99', delays.length ? `${Math.max(...delays)} ms` : '–'),
    tile('Sent vs naive', totals.naiveMessages ? `${(totals.messagesSent / totals.naiveMessages * 100).toFixed(1)} %` : '–')
  );

  push(rateHistory, totals.messagesPerSecond);
  for (const m of instances) {
    if (!loopHistory.has(m.instance)) loopHistory.set(m.instance, []);
    push(loopHistory.get(m.instance), m.eventLoop.p99);
  }
  draw($('loop'), [...loopHistory.values()].map((values, i) => ({ values, color: COLORS[i % COLORS.length] })));
  draw($('rate'), [{ values: rateHistory, color: '#22d3ee' }]);

  $('rows').replaceChildren(...instances.map((m, i) => {
    const tr = document.createElement('tr');
    const cells = [
      m.instance,
      m.status ?? 'up',
      m.connections,
      Object.entries(m.transports ?? {}).map(([k, v]) => `${k}:${v}`).join(' ') || '–',
      m.messages.perSecond,
      m.eventLoop.p99,
      m.eventLoop.max,
      m.cpuPercent ?? '–',
      m.memory.heapMB,
      m.memory.heapPerConnectionKB ?? '–',
      m.eventsPerMinute ?? '–',
      `${m.publishDelay.p50 ?? '–'} / ${m.publishDelay.p99 ?? '–'}`,
      `${m.slow?.degraded ?? 0}/${m.slow?.recovered ?? 0}/${m.slow?.disconnected ?? 0}`
    ];
    cells.forEach((value, column) => {
      const td = document.createElement('td');
      td.textContent = value;
      if (column >= 2 && column !== 3) td.className = 'num';
      if (column === 0) {
        td.style.borderLeft = `3px solid ${COLORS[i % COLORS.length]}`;
        td.className = 'mono';
      }
      tr.append(td);
    });
    return tr;
  }));
  $('updated').textContent = `updated ${new Date().toLocaleTimeString()}`;
}

async function tick() {
  try {
    const response = await fetch('/metrics/all', { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
  } catch (error) {
    const conn = $('conn');
    conn.className = 'pill bad';
    conn.textContent = `Metrics unavailable (${error.message})`;
  } finally {
    setTimeout(tick, 1000);
  }
}
tick();
