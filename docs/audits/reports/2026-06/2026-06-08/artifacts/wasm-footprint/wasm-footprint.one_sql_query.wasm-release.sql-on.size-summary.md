## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3129414 |
| icp-built deterministic `.wasm.gz` | 1053326 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2916941 |
| icp-shrunk `.wasm.gz` (canonical) | 1006786 |
| Shrink delta `.wasm` | 212473 |
| Shrink delta `.wasm.gz` | 46540 |

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
