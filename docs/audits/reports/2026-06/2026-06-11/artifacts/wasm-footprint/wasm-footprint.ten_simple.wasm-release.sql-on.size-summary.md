## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2289982 |
| icp-built deterministic `.wasm.gz` | 731732 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2129989 |
| icp-shrunk `.wasm.gz` (canonical) | 691918 |
| Shrink delta `.wasm` | 159993 |
| Shrink delta `.wasm.gz` | 39814 |

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

Custom exports: `query_ten_simple_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
