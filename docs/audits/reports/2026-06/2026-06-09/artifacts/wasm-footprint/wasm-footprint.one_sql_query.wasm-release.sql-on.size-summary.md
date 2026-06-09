## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3100379 |
| icp-built deterministic `.wasm.gz` | 1045951 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2889405 |
| icp-shrunk `.wasm.gz` (canonical) | 998123 |
| Shrink delta `.wasm` | 210974 |
| Shrink delta `.wasm.gz` | 47828 |

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
