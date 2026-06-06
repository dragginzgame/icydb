## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3170357 |
| icp-built deterministic `.wasm.gz` | 1071418 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2955322 |
| icp-shrunk `.wasm.gz` (canonical) | 1021895 |
| Shrink delta `.wasm` | 215035 |
| Shrink delta `.wasm.gz` | 49523 |

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
