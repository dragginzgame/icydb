import { HttpAgent } from '@tokostudios/ic-platform/agent';
import { Ed25519KeyIdentity } from '@tokostudios/ic-platform/identity';
import * as contracts from '../../target/wire-browser/icydb.js';
import { call } from './client.js';

type Frame = { method: string; request: number[]; response: number[] };
function require(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
async function installedCaller(host: string, canisterId: string) {
  // Public deterministic identities exist only for this disposable local fixture.
  const agent = await HttpAgent.create({ host, identity: Ed25519KeyIdentity.generate(new Uint8Array(32).fill(7)), shouldFetchRootKey: true });
  const frames: Frame[] = [];
  (globalThis as unknown as { icydbWireFrames: Frame[] }).icydbWireFrames = frames;
  async function invoke<Q, R>(contract: { methodName: string; mode: 'query' | 'update'; encodeRequest(value: Q): Uint8Array; decodeResponse(bytes: Uint8Array): R; encodeResponse(value: R): Uint8Array }, value: Q): Promise<R> {
    const request = contract.encodeRequest(value);
    // Record the actual reply passed to the browser decoder and prove its
    // canonical re-encoding, rather than synthesizing expected server bytes.
    return call(agent, canisterId, { ...contract, decodeResponse(bytes: Uint8Array): R {
      frames.push({ method: contract.methodName, request: Array.from(request), response: Array.from(bytes) });
      let response: R;
      try { response = contract.decodeResponse(bytes); }
      catch (error) { console.error('native reply decode failed', contract.methodName, bytes.length); throw error; }
      const again = contract.encodeResponse(response);
      require(again.length === bytes.length && again.every((b, i) => b === bytes[i]), 'canonical browser reply');
      return response;
    } }, value);
  }
  return { agent, frames, invoke };
}

export async function qualify(host: string, canisterId: string) {
  const { agent, frames, invoke } = await installedCaller(host, canisterId);
  require('ok' in await invoke(contracts.icydb_fixtures_reset, null), 'reset');
  require('ok' in await invoke(contracts.icydb_fixtures_load, null), 'load');
  const sql = 'SELECT COUNT(*) FROM SqlTestUser';
  function countValue(result: ReturnType<typeof contracts.icydb_query.decodeResponse>): bigint {
    require('ok' in result && result.ok.tag === 'Projection', 'native COUNT projection');
    require(result.ok.value.columns[0] === 'COUNT(*)' && result.ok.value.rows.length === 1, 'COUNT column');
    const value = result.ok.value.rows[0][0];
    require(value.tag === 'Nat', 'exact COUNT integer');
    return value.value;
  }
  const count = countValue(await invoke(contracts.icydb_query, sql));
  require(count === 3n, 'native fixture count');
  require(countValue(await invoke(contracts.icydb_query, sql)) === count, 'query retry');
  require(frames.at(-1)!.request.every((b, i) => b === frames.at(-2)!.request[i]), 'exact retry bytes');
  require('err' in await invoke(contracts.icydb_query, 'invalid SQL /* é🦀 */'), 'typed query error');
  require('err' in await invoke(contracts.icydb_ddl, 'invalid SQL'), 'typed DDL error');
  require('err' in await invoke(contracts.icydb_update, 'invalid SQL'), 'typed update error');
  require('err' in await invoke(contracts.icydb_integrity, 'invalid SQL'), 'typed integrity error');
  require('ok' in await invoke(contracts.icydb_metrics, null), 'metrics');
  require('ok' in await invoke(contracts.icydb_metrics_reset, null), 'metrics reset');
  require('ok' in await invoke(contracts.icydb_snapshot, null), 'snapshot');
  const schema = await invoke(contracts.icydb_schema, null);
  require('ok' in schema && schema.ok.length > 0, 'accepted schema');
  const status = await invoke(contracts.icydb_schema_migration, { cursor: null });
  require('ok' in status, 'migration status');
  await invoke(contracts.icydb_schema_migrate, { tag: 'Adopt', value: { expected_database: status.ok.database_identity, expected_head: status.ok.accepted_head } });
  const outsider = await HttpAgent.create({ host, identity: Ed25519KeyIdentity.generate(new Uint8Array(32).fill(8)), shouldFetchRootKey: true });
  const denied = await call(outsider, canisterId, contracts.icydb_query, sql);
  require('err' in denied, 'noncontroller query refusal');
  const request = contracts.icydb_query.encodeRequest(sql);
  const rejected = await agent.query(canisterId, { methodName: 'icydb_query', arg: new Uint8Array([...request, 0]) });
  require(rejected.status !== 'replied', 'trailing ingress refusal');
  const after = await invoke(contracts.icydb_query, sql);
  require(countValue(after) === count, 'query survives malformed ingress');
  // Status currently has a single bounded finding page: any supplied cursor is
  // invalid, including empty bytes. Exercise that semantic boundary over Wire.
  const beforeCursor = await invoke(contracts.icydb_schema_migration, { cursor: null });
  for (const length of [0, 1, 256, 257]) {
    const refused = await invoke(contracts.icydb_schema_migration, { cursor: { some: new Uint8Array(length).fill(255) } });
    require('err' in refused, 'supplied migration cursor must be rejected');
  }
  const afterCursor = await invoke(contracts.icydb_schema_migration, { cursor: null });
  const expectedStatus = contracts.icydb_schema_migration.encodeResponse(beforeCursor);
  const actualStatus = contracts.icydb_schema_migration.encodeResponse(afterCursor);
  require(expectedStatus.length === actualStatus.length && expectedStatus.every((b, i) => b === actualStatus[i]), 'cursor refusal preserves migration status');

  // Add a constraint without validating historical rows, then use the real
  // retained finding page to qualify nonempty native key bytes in Chromium.
  const added = await invoke(contracts.icydb_ddl,
    'ALTER TABLE SqlTestUser ADD CONSTRAINT browser_age CHECK (age >= 100) NOT VALID EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2');
  require('ok' in added, 'publish historical validation fixture');
  const validate = 'ALTER TABLE SqlTestUser VALIDATE CONSTRAINT browser_age';
  require('ok' in await invoke(contracts.icydb_ddl, validate), 'start historical validation');
  const findings = await invoke(contracts.icydb_ddl, validate);
  require('ok' in findings && findings.ok.tag === 'Ddl', 'historical validation reply');
  const validation = findings.ok.value.constraint_validation;
  require(validation !== null && validation.some.findings.length === 3, 'three historical findings');
  for (const finding of validation.some.findings) {
    require(finding.primary_key instanceof Uint8Array && finding.primary_key.length > 0, 'finding keys are nonempty byte buffers');
  }
  const replay = await invoke(contracts.icydb_ddl, validate);
  const expectedFindings = contracts.icydb_ddl.encodeResponse(findings);
  const replayFindings = contracts.icydb_ddl.encodeResponse(replay);
  require(expectedFindings.length === replayFindings.length && expectedFindings.every((b, i) => b === replayFindings[i]), 'unacknowledged finding page replays exactly');
  require(new Set(frames.map(f => f.method)).size === 12, 'complete endpoint roster');
  return { frames, rows: count.toString(), browser: navigator.userAgent };
}

// The native fixture has already rejected a checked cast before rewriting rows.
export async function qualifyRejectedMigration(host: string, canisterId: string) {
  const { frames, invoke } = await installedCaller(host, canisterId);
  const status = await invoke(contracts.icydb_schema_migration, { cursor: null });
  require('ok' in status && status.ok.phase.tag === 'Rejected', 'real rejected migration');
  require(status.ok.findings.length === 1, 'one checked-cast finding');
  const finding = status.ok.findings[0];
  require(finding.kind.tag === 'Transform', 'checked-cast finding kind');
  require(finding.source_field !== null && finding.target_field !== null && finding.source_field.some === finding.target_field.some, 'accepted source and target field IDs');
  require(finding.transform_reason !== null && finding.transform_reason.some.tag === 'Overflow', 'typed checked-cast overflow');
  require(finding.primary_key instanceof Uint8Array && finding.primary_key.length > 0, 'migration key is a nonempty byte buffer');
  require(status.ok.next_cursor === null, 'bounded status has no next cursor');
  const retry = await invoke(contracts.icydb_schema_migration, { cursor: null });
  const expected = contracts.icydb_schema_migration.encodeResponse(status);
  const actual = contracts.icydb_schema_migration.encodeResponse(retry);
  require(expected.length === actual.length && expected.every((b, i) => b === actual[i]), 'retained migration status replays exactly');
  require(frames[0].request.length === frames[1].request.length && frames[0].request.every((b, i) => b === frames[1].request[i]), 'exact migration retry request');
  return { frames, browser: navigator.userAgent };
}

// Rust supplies the exact request for a retained, populated physical page. It
// shares this authenticated fixture identity; the browser never authors a job ID.
export async function qualifyIntegrityFindings(host: string, canisterId: string, sql: string) {
  require(typeof sql === 'string' && sql.length > 0, 'native retained-page request');
  const { frames, invoke } = await installedCaller(host, canisterId);
  const result = await invoke(contracts.icydb_integrity, sql);
  require('ok' in result && result.ok.tag === 'Deep' && result.ok.value.tag === 'Page', 'physical Deep page');
  const page = result.ok.value.value;
  require(page.findings.length === 1, 'one controlled physical finding');
  for (const finding of page.findings) {
    require(finding.physical_key instanceof Uint8Array && finding.physical_key.length > 0, 'physical key is a nonempty byte buffer');
  }
  const field = page.findings.find(f => f.kind.tag === 'InvalidFieldValue');
  require(field !== undefined && field.verifier_family.tag === 'FieldValue', 'invalid field finding');
  require(field.field_paths.length === 1 && field.field_paths[0] === 'description', 'nonindexed fixture field');
  require(field.primary_key !== null && field.primary_key.some instanceof Uint8Array && field.primary_key.some.length > 0, 'decoded primary key is a nonempty optional byte buffer');
  await invoke(contracts.icydb_integrity, sql);
  require(frames.length === 2, 'two retained-page calls');
  for (const direction of ['request', 'response'] as const) {
    require(frames[0][direction].length === frames[1][direction].length && frames[0][direction].every((b, i) => b === frames[1][direction][i]), `exact integrity retry ${direction}`);
  }
  // A separate codec control covers absent optional bytes. This is deliberately
  // outside the actual-call frame log: the installed fault has a decoded key.
  const absent: ReturnType<typeof contracts.icydb_integrity.decodeResponse> = {
    ok: { tag: 'Deep', value: { tag: 'Page', value: {
      ...page, findings: page.findings.map(f => ({ ...f, primary_key: null })),
    } } },
  };
  const absentPrimaryKeyResponse = contracts.icydb_integrity.encodeResponse(absent);
  const decoded = contracts.icydb_integrity.decodeResponse(absentPrimaryKeyResponse);
  require('ok' in decoded && decoded.ok.tag === 'Deep' && decoded.ok.value.tag === 'Page' && decoded.ok.value.value.findings[0].primary_key === null, 'absent optional primary key codec control');
  return { frames, absentPrimaryKeyResponse: Array.from(absentPrimaryKeyResponse), browser: navigator.userAgent };
}
