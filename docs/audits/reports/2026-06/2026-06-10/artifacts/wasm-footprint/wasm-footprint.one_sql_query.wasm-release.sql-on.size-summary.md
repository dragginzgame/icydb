## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2926907 |
| icp-built deterministic `.wasm.gz` | 988294 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2729686 |
| icp-shrunk `.wasm.gz` (canonical) | 942293 |
| Shrink delta `.wasm` | 197221 |
| Shrink delta `.wasm.gz` | 46001 |

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
