## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3073045 |
| icp-built deterministic `.wasm.gz` | 1036904 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2863363 |
| icp-shrunk `.wasm.gz` (canonical) | 988609 |
| Shrink delta `.wasm` | 209682 |
| Shrink delta `.wasm.gz` | 48295 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_reset` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_sql`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_sql_query.wasm-release.report.json`
