## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2291172 |
| icp-built deterministic `.wasm.gz` | 737030 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2131241 |
| icp-shrunk `.wasm.gz` (canonical) | 696413 |
| Shrink delta `.wasm` | 159931 |
| Shrink delta `.wasm.gz` | 40617 |

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
