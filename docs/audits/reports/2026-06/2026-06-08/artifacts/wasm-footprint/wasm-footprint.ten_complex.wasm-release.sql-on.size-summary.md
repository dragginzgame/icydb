## Wasm Size Report: `ten_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2620829 |
| icp-built deterministic `.wasm.gz` | 837423 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2441132 |
| icp-shrunk `.wasm.gz` (canonical) | 795569 |
| Shrink delta `.wasm` | 179697 |
| Shrink delta `.wasm.gz` | 41854 |

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

Custom exports: `query_ten_complex_fluent`

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_complex.wasm-release.report.json`
