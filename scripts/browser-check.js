// Opens every page in a real Chrome and fails on console errors, failed
// requests or missing live data. Usage:
//   CHROME_PATH="C:/Program Files/Google/Chrome/Application/chrome.exe" \
//   node scripts/browser-check.js --url=http://localhost:8080
import puppeteer from 'puppeteer-core';

const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => a.slice(2).split('=')));
const BASE = args.url ?? process.env.BASE_URL ?? 'http://localhost:8080';
const CHROME = process.env.CHROME_PATH ?? 'C:/Program Files/Google/Chrome/Application/chrome.exe';

const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--no-sandbox'] });
const failures = [];

async function check(path, assertion) {
  const page = await browser.newPage();
  const problems = [];
  page.on('console', (message) => {
    if (message.type() === 'error') problems.push(`console: ${message.text()}`);
  });
  page.on('pageerror', (error) => problems.push(`pageerror: ${error.message}`));
  page.on('requestfailed', (request) => {
    // EventSource requests are aborted when the page closes; that is expected.
    if (!request.url().includes('/stream')) problems.push(`requestfailed: ${request.url()}`);
  });
  await page.goto(`${BASE}${path}`, { waitUntil: 'domcontentloaded' });
  try {
    const detail = await assertion(page);
    console.log(`ok   ${path} ${detail ?? ''}`);
  } catch (error) {
    problems.push(error.message);
  }
  if (problems.length > 0) {
    failures.push([path, problems]);
    console.log(`FAIL ${path}\n  ${problems.join('\n  ')}`);
  }
  await page.close();
}

await check('/', async (page) => {
  await page.waitForFunction(() => document.querySelector('#conn')?.textContent.includes('Live'), { timeout: 10_000 });
  await page.waitForFunction(() => document.querySelector('#cursor')?.textContent !== '–', { timeout: 10_000 });
  const transport = await page.$eval('#transport', (n) => n.textContent);
  if (transport !== 'WebSocket') throw new Error(`expected WebSocket, got ${transport}`);
  await page.select('#mun', 'mun:1');
  await page.waitForFunction(() => document.querySelector('#scope-title')?.textContent === 'Municipality 1', { timeout: 5000 });
  return `(cursor ${await page.$eval('#cursor', (n) => n.textContent)})`;
});
await check('/jury/', async (page) => {
  await page.waitForSelector('#votes input');
  const version = Date.now() % 1_000_000;
  await page.$eval('input[name=version]', (n, v) => { n.value = String(v); }, version);
  await page.click('button[type=submit]');
  await page.waitForFunction(() => /Registered|verification/.test(document.querySelector('#queue')?.textContent ?? ''), { timeout: 10_000 });
  return `(${await page.$eval('#queue li .tag', (n) => n.textContent)})`;
});
await check('/ops/', async (page) => {
  await page.waitForFunction(() => document.querySelectorAll('#rows tr').length >= 1, { timeout: 10_000 });
  return `(${await page.$$eval('#rows tr', (rows) => rows.length)} instances)`;
});
await check('/verification/', async (page) => {
  await page.waitForSelector('#login');
});

await browser.close();
if (failures.length > 0) {
  process.exitCode = 1;
} else {
  console.log('All pages loaded without errors.');
}
