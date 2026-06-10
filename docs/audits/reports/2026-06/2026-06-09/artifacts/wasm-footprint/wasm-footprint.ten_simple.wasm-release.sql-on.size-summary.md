## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2463593 |
| icp-built deterministic `.wasm.gz` | 787380 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2295015 |
| icp-shrunk `.wasm.gz` (canonical) | 747157 |
| Shrink delta `.wasm` | 168578 |
| Shrink delta `.wasm.gz` | 40223 |

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

Custom exports: `query_ten_simple_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
