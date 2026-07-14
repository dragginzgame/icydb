## Wasm Size Report: `one_entity_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2862856 |
| icp-built deterministic `.wasm.gz` | 964649 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2662479 |
| icp-shrunk `.wasm.gz` (canonical) | 921053 |
| Shrink delta `.wasm` | 200377 |
| Shrink delta `.wasm.gz` | 43596 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_entity_sql`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_entity_sql_query.wasm-release.report.json`
