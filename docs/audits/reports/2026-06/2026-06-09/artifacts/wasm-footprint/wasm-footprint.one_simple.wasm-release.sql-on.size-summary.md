## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2522199 |
| icp-built deterministic `.wasm.gz` | 812932 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2348857 |
| icp-shrunk `.wasm.gz` (canonical) | 770377 |
| Shrink delta `.wasm` | 173342 |
| Shrink delta `.wasm.gz` | 42555 |

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
