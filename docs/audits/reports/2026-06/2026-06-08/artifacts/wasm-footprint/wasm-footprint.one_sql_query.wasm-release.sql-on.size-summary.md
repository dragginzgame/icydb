## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3147859 |
| icp-built deterministic `.wasm.gz` | 1061643 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2933764 |
| icp-shrunk `.wasm.gz` (canonical) | 1012520 |
| Shrink delta `.wasm` | 214095 |
| Shrink delta `.wasm.gz` | 49123 |

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
