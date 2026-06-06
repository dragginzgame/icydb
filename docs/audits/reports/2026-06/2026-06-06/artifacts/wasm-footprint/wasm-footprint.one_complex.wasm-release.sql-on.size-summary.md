## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2653622 |
| icp-built deterministic `.wasm.gz` | 854407 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2471139 |
| icp-shrunk `.wasm.gz` (canonical) | 810896 |
| Shrink delta `.wasm` | 182483 |
| Shrink delta `.wasm.gz` | 43511 |

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

Custom exports: `query_one_complex_fluent`

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_complex.wasm-release.report.json`
