// RT-9: Interaction to Next Paint of the public page on a low-end phone
// profile, measured in a real Chrome while updates keep arriving.
//
// Profile: 360x640 viewport at DPR 2, CPU throttled 6x (Chrome DevTools'
// "low-end mobile" preset), "Slow 4G"-like network (150 ms RTT, 1.6 Mbps
// down, 750 kbps up). Interactions are trusted input events sent through the
// DevTools protocol (clicks and key presses), so the Event Timing API counts
// them exactly like a person's taps.
//
// Usage: CHROME_PATH=... node scripts/measure-inp.js --url=http://localhost:8080 --label=under-load [--rounds=15]
//        [--wait-for-load=1]
// With --wait-for-load=1 the page is opened first and the interactions start
// only once /metrics/all shows messages flowing (start the load generator
// after this script); the interactions then happen during the peak.
import { mkdirSync, writeFileSync } from 'node:fs';
import puppeteer from 'puppeteer-core';

const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => a.slice(2).split('=')));
const BASE = args.url ?? process.env.BASE_URL ?? 'http://localhost:8080';
const LABEL = args.label ?? 'baseline';
const ROUNDS = Number(args.rounds ?? 15);
const CPU_SLOWDOWN = Number(args.cpu ?? 6);
const CHROME = process.env.CHROME_PATH ?? 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 360, height: 640, deviceScaleFactor: 2, isMobile: true, hasTouch: false });
const cdp = await page.createCDPSession();
await cdp.send('Network.enable');
await cdp.send('Network.emulateNetworkConditions', {
  offline: false, latency: 150, downloadThroughput: (1.6 * 1024 * 1024) / 8, uploadThroughput: (750 * 1024) / 8
});
await cdp.send('Emulation.setCPUThrottlingRate', { rate: CPU_SLOWDOWN });

const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
const navigationStart = Date.now();
await page.goto(`${BASE}/`, { waitUntil: 'domcontentloaded', timeout: 120_000 });
await page.waitForFunction(() => document.querySelector('#conn')?.textContent.includes('Live'), { timeout: 120_000 });
const navigationToLiveMs = Date.now() - navigationStart;

let loadAtStart = null;
if (args['wait-for-load'] === '1') {
  // Actas being applied (events growing) means the simulator is running;
  // snapshots of newly opened clients alone do not count.
  const limit = Date.now() + 10 * 60_000;
  let previousEvents = null;
  while (Date.now() < limit) {
    try {
      const { instances, totals } = await (await fetch(`${BASE}/metrics/all`)).json();
      const events = Math.max(...instances.map((m) => m.events));
      if (previousEvents !== null && events > previousEvents + 5) {
        loadAtStart = { connections: totals.connections, messagesPerSecond: totals.messagesPerSecond };
        break;
      }
      previousEvents = events;
    } catch {
      // keep waiting
    }
    await sleep(2000);
  }
  if (!loadAtStart) throw new Error('No load observed within 10 minutes.');
}
const transport = await page.$eval('#transport', (n) => n.textContent);

let updates = 0;
await page.exposeFunction('__countUpdate', () => { updates += 1; });
await page.evaluate(() => {
  new MutationObserver(() => globalThis.__countUpdate()).observe(document.querySelector('#candidates'), { childList: true });
});

for (let round = 0; round < ROUNDS; round += 1) {
  await page.click('[data-scope="dep"]');
  await sleep(400);
  await page.click('#pto', { clickCount: 3 });
  await page.keyboard.type(String(1 + ((round * 97) % 1842)), { delay: 60 });
  await page.click('#pto-go');
  await sleep(600);
  await page.focus('#mun');
  await page.keyboard.press('ArrowDown');
  await sleep(600);
}
await sleep(1500);
const inp = await page.evaluate(() => globalThis.__inp);
await browser.close();

const result = {
  label: LABEL,
  url: BASE,
  transport,
  navigationToLiveMs,
  loadAtStart,
  profile: { viewport: '360x640@2x', cpuSlowdown: CPU_SLOWDOWN, network: '150 ms RTT, 1.6 Mbps down, 750 kbps up' },
  rounds: ROUNDS,
  interactions: inp?.interactions ?? 0,
  inpMs: inp?.inp === null || inp?.inp === undefined ? null : Math.round(inp.inp),
  candidateListRendersDuringTest: updates,
  pageErrors: errors,
  measuredAt: new Date().toISOString()
};
console.log(JSON.stringify(result, null, 2));
mkdirSync('results', { recursive: true });
writeFileSync(`results/inp-${LABEL}.json`, JSON.stringify(result, null, 2));
