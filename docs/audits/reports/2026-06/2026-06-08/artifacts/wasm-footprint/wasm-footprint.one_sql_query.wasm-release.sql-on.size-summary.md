## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3147348 |
| icp-built deterministic `.wasm.gz` | 1061452 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2933300 |
| icp-shrunk `.wasm.gz` (canonical) | 1012434 |
| Shrink delta `.wasm` | 214048 |
| Shrink delta `.wasm.gz` | 49018 |

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
