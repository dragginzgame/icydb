## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2395991 |
| icp-built deterministic `.wasm.gz` | 764916 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2234324 |
| icp-shrunk `.wasm.gz` (canonical) | 725752 |
| Shrink delta `.wasm` | 161667 |
| Shrink delta `.wasm.gz` | 39164 |

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
