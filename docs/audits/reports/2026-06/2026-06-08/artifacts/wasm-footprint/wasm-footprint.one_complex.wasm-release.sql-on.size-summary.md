## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2595689 |
| icp-built deterministic `.wasm.gz` | 834734 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2417673 |
| icp-shrunk `.wasm.gz` (canonical) | 793147 |
| Shrink delta `.wasm` | 178016 |
| Shrink delta `.wasm.gz` | 41587 |

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
