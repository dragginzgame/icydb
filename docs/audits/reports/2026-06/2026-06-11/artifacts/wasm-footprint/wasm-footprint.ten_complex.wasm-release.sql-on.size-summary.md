## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2309105 |
| icp-built deterministic `.wasm.gz` | 738325 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2147799 |
| icp-shrunk `.wasm.gz` (canonical) | 698401 |
| Shrink delta `.wasm` | 161306 |
| Shrink delta `.wasm.gz` | 39924 |

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

Custom exports: `query_ten_complex_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_complex.wasm-release.report.json`
