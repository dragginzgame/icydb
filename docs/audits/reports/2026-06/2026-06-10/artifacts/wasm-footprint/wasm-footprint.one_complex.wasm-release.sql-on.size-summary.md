## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2373914 |
| icp-built deterministic `.wasm.gz` | 763573 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2213588 |
| icp-shrunk `.wasm.gz` (canonical) | 723253 |
| Shrink delta `.wasm` | 160326 |
| Shrink delta `.wasm.gz` | 40320 |

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
