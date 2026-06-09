## Wasm Size Report: `one_complex` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2487982 |
| icp-built deterministic `.wasm.gz` | 800641 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2317221 |
| icp-shrunk `.wasm.gz` (canonical) | 760516 |
| Shrink delta `.wasm` | 170761 |
| Shrink delta `.wasm.gz` | 40125 |

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
