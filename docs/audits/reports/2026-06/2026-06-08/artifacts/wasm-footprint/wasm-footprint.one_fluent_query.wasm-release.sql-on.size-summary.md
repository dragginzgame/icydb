## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2530582 |
| icp-built deterministic `.wasm.gz` | 813522 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2357536 |
| icp-shrunk `.wasm.gz` (canonical) | 772414 |
| Shrink delta `.wasm` | 173046 |
| Shrink delta `.wasm.gz` | 41108 |

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

Custom exports: `query_one_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_fluent_query.wasm-release.report.json`
