## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2500666 |
| icp-built deterministic `.wasm.gz` | 805778 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2328689 |
| icp-shrunk `.wasm.gz` (canonical) | 764167 |
| Shrink delta `.wasm` | 171977 |
| Shrink delta `.wasm.gz` | 41611 |

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
