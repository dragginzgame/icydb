import assert from 'node:assert/strict';
import { lstat, readFile, readdir } from 'node:fs/promises';
import { extname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';

// SDK Core and its legacy equivalents, not Auth providers or executable tools.
const sdkPackages = ['@icp-sdk/core', '@dfinity/agent', '@dfinity/principal',
  '@dfinity/identity', '@dfinity/identity-secp256k1', '@dfinity/candid'];
const packagePattern = sdkPackages.join('|');
const alias = new RegExp(`^npm:(${packagePattern})(?:@|/|$)`);
const gap = String.raw`(?:\s|/\*[\s\S]*?\*/|//[^\r\n]*(?:\r?\n|$))*`;
// Inspect literal source, including generated-source snippets and npm scripts.
// This deliberately is not a JavaScript parser or module-resolution proof.
const sdkImport = new RegExp(String.raw`\b(?:from${gap}|import${gap}(?:\(${gap})?|require${gap}(?:\.${gap}resolve${gap})?\(${gap})['"\x60](${packagePattern})(?=[/'"\x60])`);
const sourceExtensions = new Set(['.js', '.mjs', '.cjs', '.jsx', '.ts', '.mts', '.cts', '.tsx']);
const skippedDirectories = new Set(['node_modules', '.git', '.tools', 'target', 'dist']);

function checkLiteralImports(source, path) {
  const match = sdkImport.exec(source);
  if (match) {
    const line = source.slice(0, match.index).split('\n').length;
    throw new Error(`${path}:${line}: direct SDK import (${match[1]}); use @tokostudios/ic-platform`);
  }
}

function checkManifest(manifest, path) {
  assert(manifest && typeof manifest === 'object' && !Array.isArray(manifest), `${path}: expected a package object`);
  for (const field of ['dependencies', 'devDependencies', 'optionalDependencies', 'peerDependencies']) {
    const dependencies = manifest[field] === undefined ? {} : manifest[field];
    assert(dependencies && typeof dependencies === 'object' && !Array.isArray(dependencies), `${path}: invalid ${field}`);
    for (const [name, selector] of Object.entries(dependencies)) {
      assert(typeof selector === 'string', `${path}: invalid ${field}/${name}`);
      assert(!sdkPackages.some(sdk => name === sdk || name.startsWith(`${sdk}/`)) && !alias.test(selector),
        `${path}: direct SDK dependency (${field}/${name}); use @tokostudios/ic-platform`);
    }
  }
  for (const [name, script] of Object.entries(manifest.scripts ?? {})) {
    assert(typeof script === 'string', `${path}: invalid script ${name}`);
    checkLiteralImports(script, `${path} script ${name}`);
  }
}

/** Read only explicitly selected first-party roots; never execute their code.
 * Installed/transitive dependencies and lock resolution remain consumer-owned.
 */
export async function checkIcSdkBoundary(roots) {
  assert(Array.isArray(roots) && roots.length > 0, 'Select at least one first-party source or package root');
  const seen = new Set();
  const counts = { manifests: 0, sources: 0 };
  async function visit(path) {
    const stat = await lstat(path);
    assert(!stat.isSymbolicLink(), `${path}: selected SDK boundary paths must not be symlinks`);
    if (stat.isDirectory()) {
      let selected = 0;
      for (const entry of (await readdir(path)).sort()) {
        if (!skippedDirectories.has(entry)) selected += await visit(join(path, entry));
      }
      return selected;
    }
    assert(stat.isFile(), `${path}: expected a regular file or directory`);
    const manifest = path.endsWith('/package.json');
    if (!manifest && !sourceExtensions.has(extname(path))) return 0;
    if (seen.has(path)) return 1;
    seen.add(path);
    const source = await readFile(path, 'utf8');
    if (manifest) {
      checkManifest(JSON.parse(source), path);
      counts.manifests++;
    } else {
      checkLiteralImports(source, path);
      counts.sources++;
    }
    return 1;
  }
  for (const root of roots) {
    const path = resolve(root);
    assert(await visit(path) > 0, `${path}: no package.json or supported JavaScript/TypeScript files selected`);
  }
  return counts;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const usage = 'usage: node check-ic-sdk-boundary.mjs ROOT [ROOT ...]';
  try {
    const { values, positionals } = parseArgs({ options: { help: { type: 'boolean', short: 'h' } }, allowPositionals: true });
    if (values.help) console.log(usage);
    else {
      assert(positionals.length > 0, usage);
      const counts = await checkIcSdkBoundary(positionals);
      console.log(`SDK boundary passed: ${counts.manifests} manifests, ${counts.sources} source files; runtime qualification remains consumer-owned.`);
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
