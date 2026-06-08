## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2571095 |
| icp-built deterministic `.wasm.gz` | 827291 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2394529 |
| icp-shrunk `.wasm.gz` (canonical) | 785550 |
| Shrink delta `.wasm` | 176566 |
| Shrink delta `.wasm.gz` | 41741 |

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

Custom exports: `query_one_simple_fluent`

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_simple.wasm-release.report.json`
