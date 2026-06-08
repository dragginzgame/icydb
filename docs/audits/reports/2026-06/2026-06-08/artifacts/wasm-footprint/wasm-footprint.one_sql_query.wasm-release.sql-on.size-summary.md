## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3148591 |
| icp-built deterministic `.wasm.gz` | 1061502 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2934410 |
| icp-shrunk `.wasm.gz` (canonical) | 1012846 |
| Shrink delta `.wasm` | 214181 |
| Shrink delta `.wasm.gz` | 48656 |

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
