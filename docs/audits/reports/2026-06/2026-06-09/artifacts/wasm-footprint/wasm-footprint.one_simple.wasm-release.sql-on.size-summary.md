## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2461237 |
| icp-built deterministic `.wasm.gz` | 792870 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2292201 |
| icp-shrunk `.wasm.gz` (canonical) | 751714 |
| Shrink delta `.wasm` | 169036 |
| Shrink delta `.wasm.gz` | 41156 |

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

Custom exports: `query_one_simple_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_simple.wasm-release.report.json`
