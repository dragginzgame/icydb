## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2654510 |
| icp-built deterministic `.wasm.gz` | 848289 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2471772 |
| icp-shrunk `.wasm.gz` (canonical) | 806189 |
| Shrink delta `.wasm` | 182738 |
| Shrink delta `.wasm.gz` | 42100 |

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

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
