import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync, writeFileSync } from 'node:fs';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { Ed25519KeyIdentity } from '@tokostudios/ic-platform/identity';

if (process.argv[2] === '--identity') {
  console.log(Ed25519KeyIdentity.generate(new Uint8Array(32).fill(7)).getPrincipal().toText());
} else {
  const here = dirname(fileURLToPath(import.meta.url)), out = resolve(here, '../../target/wire-browser');
  const [host, canister, report, subject = 'endpoints', integritySql] = process.argv.slice(2);
  assert(['endpoints', 'rejected-migration', 'integrity-findings'].includes(subject), 'unknown installed qualification subject');
  assert(host && canister && report, 'expected host, canister and report path');
  await build({ entryPoints: [resolve(here, 'installed.ts')], bundle: true, format: 'esm', platform: 'browser', outfile: resolve(out, 'installed.js') });
  const server = createServer((request, response) => {
    if (request.url === '/installed.js') {
      response.setHeader('Content-Type', 'application/javascript'); response.end(readFileSync(resolve(out, 'installed.js')));
    } else {
      response.setHeader('Content-Type', 'text/html'); response.end('<!doctype html><title>IcyDB Wire qualification</title>');
    }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    page.on('console', message => console.error(`browser: ${message.text()}`));
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const result = await page.evaluate(async ({ host, canister, subject, integritySql }) => {
      const { qualify, qualifyRejectedMigration, qualifyIntegrityFindings } = await import('/installed.js');
      if (subject === 'integrity-findings') return qualifyIntegrityFindings(host, canister, integritySql);
      return subject === 'rejected-migration'
        ? qualifyRejectedMigration(host, canister) : qualify(host, canister);
    }, { host, canister, subject, integritySql }).catch(async error => {
      const frames = await page.evaluate(() => globalThis.icydbWireFrames ?? []);
      writeFileSync(report, JSON.stringify({ failed: true, frames }, null, 2) + '\n');
      throw error;
    });
    writeFileSync(report, JSON.stringify(result, null, 2) + '\n');
    console.log(`Installed browser: ${result.frames.length} calls across ${new Set(result.frames.map(f => f.method)).size} endpoint methods`);
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
}
