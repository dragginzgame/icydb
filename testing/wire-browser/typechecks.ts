import { icydb_query, icydb_metrics, type PublicValue } from '../../target/wire-browser/icydb.js';

icydb_query.encodeRequest('SELECT COUNT(*) FROM Entry');
icydb_metrics.encodeRequest(null);
const exact: PublicValue = { tag: 'Nat128', value: (1n << 128n) - 1n };
void exact;
// @ts-expect-error wide integer payloads require bigint
const rounded: PublicValue = { tag: 'Nat', value: 7 };
// @ts-expect-error native byte strings require Uint8Array
const bytes: PublicValue = { tag: 'Blob', value: [1, 2] };
// @ts-expect-error SQL request is a string
icydb_query.encodeRequest({ sql: 'SELECT * FROM Entry' });
// @ts-expect-error unit request is explicit null
icydb_metrics.encodeRequest(undefined);
void rounded; void bytes;
