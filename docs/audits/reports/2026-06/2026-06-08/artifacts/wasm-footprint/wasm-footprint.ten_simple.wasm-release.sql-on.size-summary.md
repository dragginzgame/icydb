## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2575592 |
| icp-built deterministic `.wasm.gz` | 822222 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2399077 |
| icp-shrunk `.wasm.gz` (canonical) | 781667 |
| Shrink delta `.wasm` | 176515 |
| Shrink delta `.wasm.gz` | 40555 |

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

Custom exports: `query_ten_simple_fluent`

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
