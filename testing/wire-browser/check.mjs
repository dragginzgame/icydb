import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..'), out = resolve(root, 'target/wire-browser');
function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${command} failed`);
}
assert.equal(process.version, 'v24.21.0', 'select the declared Node runtime');
run('cargo', ['run', '--locked', '--offline', '-p', 'icydb-testing-build', '--example', 'wire_bindings', '--', out]);
mkdirSync(resolve(out, 'compiled'), { recursive: true });
writeFileSync(resolve(out, 'compiled/package.json'), '{"type":"module"}\n');
run(resolve(here, 'node_modules/.bin/tsc'), ['--ignoreConfig', '--strict', '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'Bundler', '--skipLibCheck', '--outDir', resolve(out, 'compiled'), resolve(out, 'icydb.ts'), resolve(out, 'native-runtime.ts'), resolve(out, 'wire-runtime.ts')]);
run(resolve(here, 'node_modules/.bin/tsc'), ['--ignoreConfig', '--strict', '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'Bundler', '--skipLibCheck', '--noEmit', resolve(here, 'client.ts'), resolve(here, 'installed.ts'), resolve(here, 'typechecks.ts')]);
const contracts = await import(pathToFileURL(resolve(out, 'compiled/icydb.js')));
const { WireError } = await import(pathToFileURL(resolve(out, 'compiled/native-runtime.js')));
const { vectors } = await import(pathToFileURL(resolve(out, 'vectors.mjs')));
// Compare the generator's full endpoint roster with the schema-owned browser
// projection. Intentional browser/native asymmetry is retained by that owner.
const budgetPolicy = JSON.parse(readFileSync(resolve(out, 'budget-policy.json'), 'utf8'));
const generatedContract = JSON.parse(readFileSync(resolve(out, 'contracts.json'), 'utf8'));
// This reviewed fixture is deliberately not rewritten by contract generation.
// Arrays retain field/variant order; object key formatting is not a contract.
assert.deepEqual(generatedContract, JSON.parse(readFileSync(resolve(here, 'contract-baseline.json'), 'utf8')),
  'Wire contract drift: review client/layout/identity and admission changes before updating the baseline');
const generatedMethods = generatedContract.methods;
function checkBudgets(method) {
  assert.deepEqual(Object.fromEntries(Object.keys(budgetPolicy).map(key => [key, method[key]])), budgetPolicy);
}
assert.equal(generatedMethods.length, 12);
for (const method of generatedMethods) {
  checkBudgets(method);
  for (const key of Object.keys(budgetPolicy)) {
    assert.throws(() => checkBudgets({ ...method, [key]: method[key] - 1 }), assert.AssertionError);
    assert.throws(() => checkBudgets({ ...method, [key]: method[key] + 1 }), assert.AssertionError);
  }
}
const plain = value => value instanceof Uint8Array ? value : Array.isArray(value) ? value.map(plain) : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([k, v]) => [k, plain(v)])) : value;
const refused = action => assert.throws(action, WireError);
let truncations = 0;
const methods = new Set();
for (const vector of vectors) {
  const codec = contracts[vector.method];
  methods.add(vector.method);
  const encode = vector.response ? codec.encodeResponse : codec.encodeRequest;
  const decode = vector.response ? codec.decodeResponse : codec.decodeRequest;
  assert.deepEqual(encode(vector.value), vector.bytes);
  assert.deepEqual(plain(decode(vector.bytes)), plain(vector.value));
  assert.deepEqual(encode(decode(vector.bytes)), vector.bytes);
  assert.deepEqual(encode(vector.value), encode(vector.value), 'exact retry bytes');
  const trailing = new Uint8Array([...vector.bytes, 0]);
  refused(() => decode(trailing));
  for (let n = 0; n < vector.bytes.length; n++) {
    refused(() => decode(vector.bytes.subarray(0, n))); truncations++;
  }
  for (const index of [0, 1, 2, 9, 10]) {
    const changed = vector.bytes.slice(); changed[index] ^= 1;
    refused(() => decode(changed));
  }
}
assert.equal(methods.size, 12);
const query = contracts.icydb_query;
const request = query.encodeRequest('x');
refused(() => query.decodeRequest(new Uint8Array([...request.subarray(0, 11), 0x81, 0, 120])));
refused(() => query.decodeRequest(new Uint8Array([...request.subarray(0, 11), 1, 255])));
refused(() => query.encodeRequest('\ud800'));
refused(() => query.encodeRequest('é'.repeat(1_048_576)));
const projection = value => ({ ok: { tag: 'Projection', value: { entity: 'Entry', columns: ['value'], rows: [[value]], row_count: 1 } } });
for (const length of [0, 1, 3]) {
  const pair = Array.from({ length }, () => ({ tag: 'Null', value: null }));
  assert.throws(() => query.encodeResponse(projection({ tag: 'Map', value: [pair] })),
    error => error instanceof WireError && error.code === 'value');
}
for (const bits of [32, 64]) {
  for (const number of [NaN, Infinity, -Infinity]) refused(() => query.encodeResponse(projection({ tag: `Float${bits}`, value: number })));
}
for (const bits of [32, 64]) {
  const vector = vectors.find(v => v.response && v.value.ok?.tag === 'Projection' && v.value.ok.value.rows[0]?.[0]?.tag === `Float${bits}`);
  assert(vector);
  for (const value of [NaN, Infinity, -Infinity]) {
    const invalid = vector.bytes.slice(), view = new DataView(invalid.buffer);
    if (bits === 32) view.setFloat32(invalid.length - 5, value, true);
    else view.setFloat64(invalid.length - 9, value, true);
    refused(() => query.decodeResponse(invalid));
  }
}
const decimal = vectors.find(v => v.response && v.value.ok?.tag === 'Projection' && v.value.ok.value.rows[0]?.[0]?.tag === 'Decimal');
assert(decimal);
refused(() => query.decodeResponse(new Uint8Array([...decimal.bytes.subarray(0, decimal.bytes.length - 3), 2, 48, 48, 1])));
refused(() => query.decodeRequest(new Uint8Array([...request.subarray(0, 11), ...Array(9).fill(255), 1])));
refused(() => query.encodeResponse(projection({ tag: 'Float32', value: 0.1 })));
for (const [tag, value] of [['Nat', 7], ['Nat128', 1n << 128n], ['Int', -(1n << 63n) - 1n], ['Date', 2_932_897], ['Principal', new Uint8Array(30)], ['Subaccount', new Uint8Array(31)], ['U256', new Uint8Array(33)], ['Ulid', new Uint8Array(15)], ['Decimal', '00.10']]) refused(() => query.encodeResponse(projection({ tag, value })));
let nested = { tag: 'Null', value: null };
for (let i = 0; i < 40; i++) nested = { tag: 'List', value: [nested] };
refused(() => query.encodeResponse(projection(nested)));
refused(() => query.encodeResponse(projection({ tag: 'List', value: Array.from({ length: 32769 }, () => ({ tag: 'Unit', value: null })) })));
const grouped = vectors.find(v => v.response && v.value.ok?.tag === 'Grouped' && v.value.ok.value.next_cursor?.some === 'opaque-authenticated-cursor');
assert(grouped);
const decoded = query.decodeResponse(grouped.bytes);
assert.deepEqual(decoded.ok.value.rows, [['NULL', '7'], ['"NULL"', '2']]);
assert.equal(decoded.ok.value.next_cursor.some, 'opaque-authenticated-cursor');
const oversized = structuredClone(grouped.value);
oversized.ok.value.rows = Array.from({ length: 32769 }, () => ['NULL', '1']);
const original = structuredClone(oversized);
refused(() => query.encodeResponse(oversized));
assert.deepEqual(oversized, original, 'oversized pages must not be truncated');

// Byte-buffer cursor inputs are a hard cut; numeric arrays are not a fallback.
const migration = contracts.icydb_schema_migration;
for (const length of [0, 1, 127, 128, 256]) {
  const cursor = new Uint8Array(length).fill(255);
  const value = { cursor: { some: cursor } };
  assert.deepEqual(plain(migration.decodeRequest(migration.encodeRequest(value))), value);
}
refused(() => migration.encodeRequest({ cursor: { some: [1, 2] } }));
console.log(`Passed ${vectors.length} native frames across ${methods.size} endpoints, ${truncations} truncated frames and malformed/value/limit controls`);
