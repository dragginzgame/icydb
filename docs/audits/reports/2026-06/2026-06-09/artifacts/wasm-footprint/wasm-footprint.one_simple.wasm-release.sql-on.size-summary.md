## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2497895 |
| icp-built deterministic `.wasm.gz` | 805686 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2325923 |
| icp-shrunk `.wasm.gz` (canonical) | 763370 |
| Shrink delta `.wasm` | 171972 |
| Shrink delta `.wasm.gz` | 42316 |

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
