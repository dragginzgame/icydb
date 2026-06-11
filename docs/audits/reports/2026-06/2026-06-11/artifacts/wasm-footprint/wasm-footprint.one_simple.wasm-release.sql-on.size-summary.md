## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2360018 |
| icp-built deterministic `.wasm.gz` | 762175 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2201366 |
| icp-shrunk `.wasm.gz` (canonical) | 721617 |
| Shrink delta `.wasm` | 158652 |
| Shrink delta `.wasm.gz` | 40558 |

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
