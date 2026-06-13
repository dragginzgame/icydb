## Wasm Size Report: `one_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2821951 |
| icp-built deterministic `.wasm.gz` | 951708 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2624578 |
| icp-shrunk `.wasm.gz` (canonical) | 907613 |
| Shrink delta `.wasm` | 197373 |
| Shrink delta `.wasm.gz` | 44095 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_sql`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_sql_query.wasm-release.report.json`
