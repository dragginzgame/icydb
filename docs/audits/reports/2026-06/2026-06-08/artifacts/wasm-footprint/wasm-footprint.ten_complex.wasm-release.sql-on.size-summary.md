## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2599358 |
| icp-built deterministic `.wasm.gz` | 829872 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2421415 |
| icp-shrunk `.wasm.gz` (canonical) | 788782 |
| Shrink delta `.wasm` | 177943 |
| Shrink delta `.wasm.gz` | 41090 |

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

Custom exports: `query_ten_complex_fluent`

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_complex.wasm-release.report.json`
