## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2385722 |
| icp-built deterministic `.wasm.gz` | 769663 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2225610 |
| icp-shrunk `.wasm.gz` (canonical) | 729878 |
| Shrink delta `.wasm` | 160112 |
| Shrink delta `.wasm.gz` | 39785 |

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

Custom exports: `query_one_complex_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_complex.wasm-release.report.json`
