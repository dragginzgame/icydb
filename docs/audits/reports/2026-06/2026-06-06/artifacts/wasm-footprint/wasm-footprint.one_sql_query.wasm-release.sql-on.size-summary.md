## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3170336 |
| icp-built deterministic `.wasm.gz` | 1071475 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2955294 |
| icp-shrunk `.wasm.gz` (canonical) | 1023008 |
| Shrink delta `.wasm` | 215042 |
| Shrink delta `.wasm.gz` | 48467 |

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

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_sql_query.wasm-release.report.json`
