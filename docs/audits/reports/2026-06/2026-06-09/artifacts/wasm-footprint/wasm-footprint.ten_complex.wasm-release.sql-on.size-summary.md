## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2549527 |
| icp-built deterministic `.wasm.gz` | 815906 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2374433 |
| icp-shrunk `.wasm.gz` (canonical) | 774663 |
| Shrink delta `.wasm` | 175094 |
| Shrink delta `.wasm.gz` | 41243 |

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
