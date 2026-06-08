## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2608054 |
| icp-built deterministic `.wasm.gz` | 844251 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2430070 |
| icp-shrunk `.wasm.gz` (canonical) | 801280 |
| Shrink delta `.wasm` | 177984 |
| Shrink delta `.wasm.gz` | 42971 |

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
