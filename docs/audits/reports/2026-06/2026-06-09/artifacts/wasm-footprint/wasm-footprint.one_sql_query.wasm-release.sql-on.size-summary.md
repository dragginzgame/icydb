## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3073068 |
| icp-built deterministic `.wasm.gz` | 1036875 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2863391 |
| icp-shrunk `.wasm.gz` (canonical) | 988584 |
| Shrink delta `.wasm` | 209677 |
| Shrink delta `.wasm.gz` | 48291 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_sql`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_sql_query.wasm-release.report.json`
