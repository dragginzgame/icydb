## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2386500 |
| icp-built deterministic `.wasm.gz` | 762971 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2226350 |
| icp-shrunk `.wasm.gz` (canonical) | 724732 |
| Shrink delta `.wasm` | 160150 |
| Shrink delta `.wasm.gz` | 38239 |

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
