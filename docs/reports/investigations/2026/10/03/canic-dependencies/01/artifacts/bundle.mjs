// Bundle the offline fixture against an existing, patched upstream checkout.
import assert from 'node:assert/strict';
import { resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const [rootArg, outputArg] = process.argv.slice(2);
assert(rootArg && outputArg, 'Usage: node bundle.mjs UPSTREAM_ROOT OUTPUT_FILE');
const root = resolve(rootArg);
const nodeModules = join(root, 'tests/browser/node_modules');
const { build } = await import(pathToFileURL(join(nodeModules, 'esbuild/lib/main.js')).href);
await build({
  entryPoints: [fileURLToPath(new URL('./budget-reproduction.mjs', import.meta.url))],
  bundle: true, format: 'esm', platform: 'node', outfile: resolve(outputArg),
  banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" },
  alias: {
    '@audit/publication': join(root, 'clients/browser/publication.js'),
    '@caffeineai/object-storage': join(root, '.tmp/browser/caffeine/dist/index.js'),
  },
  nodePaths: [nodeModules],
});
