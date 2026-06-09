## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2509890 |
| icp-built deterministic `.wasm.gz` | 802651 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2337752 |
| icp-shrunk `.wasm.gz` (canonical) | 761623 |
| Shrink delta `.wasm` | 172138 |
| Shrink delta `.wasm.gz` | 41028 |

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
