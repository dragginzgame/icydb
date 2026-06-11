## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2411033 |
| icp-built deterministic `.wasm.gz` | 770931 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2249568 |
| icp-shrunk `.wasm.gz` (canonical) | 732258 |
| Shrink delta `.wasm` | 161465 |
| Shrink delta `.wasm.gz` | 38673 |

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
