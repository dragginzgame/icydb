## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2460679 |
| icp-built deterministic `.wasm.gz` | 792885 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2292472 |
| icp-shrunk `.wasm.gz` (canonical) | 750451 |
| Shrink delta `.wasm` | 168207 |
| Shrink delta `.wasm.gz` | 42434 |

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
