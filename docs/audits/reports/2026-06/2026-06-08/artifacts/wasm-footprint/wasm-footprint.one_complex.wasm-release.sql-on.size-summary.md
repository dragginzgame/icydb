## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2574218 |
| icp-built deterministic `.wasm.gz` | 827932 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2397956 |
| icp-shrunk `.wasm.gz` (canonical) | 786107 |
| Shrink delta `.wasm` | 176262 |
| Shrink delta `.wasm.gz` | 41825 |

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

Custom exports: `query_one_complex_fluent`

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_complex.wasm-release.report.json`
