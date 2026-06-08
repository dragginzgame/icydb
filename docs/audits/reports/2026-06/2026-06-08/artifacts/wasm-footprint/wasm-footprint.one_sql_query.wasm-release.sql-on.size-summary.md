## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 3129276 |
| icp-built deterministic `.wasm.gz` | 1052868 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2916794 |
| icp-shrunk `.wasm.gz` (canonical) | 1006633 |
| Shrink delta `.wasm` | 212482 |
| Shrink delta `.wasm.gz` | 46235 |

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
