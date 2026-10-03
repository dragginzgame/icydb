import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { StorageClient } from '@caffeineai/object-storage';
import { Ed25519KeyIdentity } from '@icp-sdk/core/identity';
import { createPublicationUpload } from '@audit/publication';

const body = new Uint8Array(1024).fill(42);
const prepared = await StorageClient.prepareFile(body, 'image/png');
const identity = Ed25519KeyIdentity.generate(new Uint8Array(32).fill(42));
const uploader = identity.getPrincipal().toText();
const service = 'rrkah-fqaaa-aaaaa-aaaaq-cai';
const sentinel = new Error('offline transport refusal');
const results = [];
for (const [name, limits] of [
  ['one_request_for_tree_and_chunk', { maxRequests: 1, maxRequestBytes: 65536, maxTotalRequestBytes: 65536 }],
  ['one_byte_per_request', { maxRequests: 2, maxRequestBytes: 1, maxTotalRequestBytes: 2 }],
  ['one_byte_total', { maxRequests: 2, maxRequestBytes: 65536, maxTotalRequestBytes: 1 }],
]) {
  let row, claims = 0, certificateRequests = 0, providerRequests = 0;
  const copy = value => structuredClone(value);
  // In-memory journal substitute. No IndexedDB or provider claim is qualified.
  const intents = {
    async save(binding) { row ??= { key: binding.key, binding: copy(binding), phase: 'saved', cancelled: false }; return copy(row); },
    async inspect() { return copy(row); },
    async claim(binding, envelope, requestId) {
      assert.equal(row.phase, 'saved');
      claims++;
      row = { ...row, phase: 'uncertain', envelope: copy(envelope), requestId };
      return copy(row);
    },
    async observe() { throw new Error('unexpected observation'); },
    async cancel() { row.cancelled = true; return copy(row); },
    async claimGateway() { throw new Error('unexpected gateway claim'); },
    async observeGateway() { throw new Error('unexpected gateway observation'); },
  };
  const upload = await createPublicationUpload({
    host: 'http://127.0.0.1:1', identity, rootKey: new Uint8Array([1]), intents,
    binding: { key: `${service}:${uploader}:1`, service, tenant: uploader, uploader,
      operation: '1', root: prepared.hash, project: 'offline-fixture', bucket: 'offline-fixture',
      permission: [68, 73, 68, 76] },
    body, bodySha256: createHash('sha256').update(body).digest('hex'),
    manifestJSON: prepared.manifestJSON, contentType: 'image/png', maxBodyBytes: 1024,
    origin: 'https://substitute.invalid', ...limits,
    certificateFetch: async () => { certificateRequests++; throw sentinel; },
    gatewayFetch: async () => { providerRequests++; throw new Error('unexpected provider request'); },
  });
  await assert.rejects(upload.upload(), error => error.cause?.code?.error === sentinel);
  assert.equal(claims, 1);
  assert.equal(certificateRequests, 1);
  assert.equal(providerRequests, 0);
  assert.equal(row.phase, 'uncertain');
  results.push({ name, limits, claims, certificateRequests, providerRequests,
    phase: row.phase, failure: 'offline transport refusal' });
}
console.log(JSON.stringify({ evidence: 'local SDK and in-memory journal with refusing fetch', results }, null, 2));
