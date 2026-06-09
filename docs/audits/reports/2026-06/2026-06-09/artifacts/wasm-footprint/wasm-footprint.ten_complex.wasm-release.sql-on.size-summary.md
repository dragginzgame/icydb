## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2571060 |
| icp-built deterministic `.wasm.gz` | 822508 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2394610 |
| icp-shrunk `.wasm.gz` (canonical) | 781128 |
| Shrink delta `.wasm` | 176450 |
| Shrink delta `.wasm.gz` | 41380 |

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
