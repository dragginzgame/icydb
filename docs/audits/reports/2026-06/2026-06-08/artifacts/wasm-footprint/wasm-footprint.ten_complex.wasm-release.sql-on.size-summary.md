## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2669553 |
| icp-built deterministic `.wasm.gz` | 852213 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2486498 |
| icp-shrunk `.wasm.gz` (canonical) | 810401 |
| Shrink delta `.wasm` | 183055 |
| Shrink delta `.wasm.gz` | 41812 |

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

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_complex.wasm-release.report.json`
