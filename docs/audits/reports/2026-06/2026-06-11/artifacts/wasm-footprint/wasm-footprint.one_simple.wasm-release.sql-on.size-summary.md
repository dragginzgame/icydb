## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2361120 |
| icp-built deterministic `.wasm.gz` | 760972 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2202324 |
| icp-shrunk `.wasm.gz` (canonical) | 722117 |
| Shrink delta `.wasm` | 158796 |
| Shrink delta `.wasm.gz` | 38855 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_simple_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_simple.wasm-release.report.json`
