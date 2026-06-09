## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2499324 |
| icp-built deterministic `.wasm.gz` | 806286 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2327351 |
| icp-shrunk `.wasm.gz` (canonical) | 763809 |
| Shrink delta `.wasm` | 171973 |
| Shrink delta `.wasm.gz` | 42477 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | yes |
| `metrics_reset` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_simple_fluent`

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_simple.wasm-release.report.json`
