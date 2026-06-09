## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2464261 |
| icp-built deterministic `.wasm.gz` | 793178 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2294926 |
| icp-shrunk `.wasm.gz` (canonical) | 753022 |
| Shrink delta `.wasm` | 169335 |
| Shrink delta `.wasm.gz` | 40156 |

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
