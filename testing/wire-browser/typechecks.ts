import { icydb_query, icydb_metrics, type PublicValue } from '../../target/wire-browser/icydb.js';

icydb_query.encodeRequest('SELECT COUNT(*) FROM Entry');
icydb_metrics.encodeRequest(null);
const exact: PublicValue = { tag: 'Nat128', value: (1n << 128n) - 1n };
void exact;
const pair: PublicValue = { tag: 'Map', value: [[exact, exact]] };
// @ts-expect-error native map entries contain exactly two values
const shortPair: PublicValue = { tag: 'Map', value: [[exact]] };
// @ts-expect-error native map entries contain exactly two values
const longPair: PublicValue = { tag: 'Map', value: [[exact, exact, exact]] };
void pair; void shortPair; void longPair;
// @ts-expect-error wide integer payloads require bigint
const rounded: PublicValue = { tag: 'Nat', value: 7 };
// @ts-expect-error native byte strings require Uint8Array
const bytes: PublicValue = { tag: 'Blob', value: [1, 2] };
// @ts-expect-error SQL request is a string
icydb_query.encodeRequest({ sql: 'SELECT * FROM Entry' });
// @ts-expect-error unit request is explicit null
icydb_metrics.encodeRequest(undefined);
void rounded; void bytes;

// All key/cursor fields share the native byte-buffer client contract.
type ByteFields = [
  import('../../target/wire-browser/icydb.js').ConstraintValidationFindingOutput['primary_key'],
  import('../../target/wire-browser/icydb.js').IntegrityFinding['physical_key'],
  NonNullable<import('../../target/wire-browser/icydb.js').IntegrityFinding['primary_key']>['some'],
  import('../../target/wire-browser/icydb.js').SchemaMigrationFinding['primary_key'],
  NonNullable<import('../../target/wire-browser/icydb.js').SchemaMigrationStatusPage['next_cursor']>['some'],
  NonNullable<import('../../target/wire-browser/icydb.js').SchemaMigrationStatusRequest['cursor']>['some'],
];
const keyBytes = new Uint8Array([0, 128, 255]);
const byteFields: ByteFields = [keyBytes, keyBytes, keyBytes, keyBytes, keyBytes, keyBytes];
const exactByteFields: [Uint8Array, Uint8Array, Uint8Array, Uint8Array, Uint8Array, Uint8Array] = byteFields;
// @ts-expect-error byte fields no longer accept numeric arrays
const oldKey: ByteFields[number] = [0, 128, 255];
void exactByteFields; void oldKey;
