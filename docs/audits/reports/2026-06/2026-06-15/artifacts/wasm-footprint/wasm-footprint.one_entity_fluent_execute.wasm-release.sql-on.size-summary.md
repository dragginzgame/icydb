## Wasm Size Report: `one_entity_fluent_execute` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2277307 |
| icp-built deterministic `.wasm.gz` | 731898 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2118876 |
| icp-shrunk `.wasm.gz` (canonical) | 695621 |
| Shrink delta `.wasm` | 158431 |
| Shrink delta `.wasm.gz` | 36277 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_entity_fluent_execute`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_entity_fluent_execute.wasm-release.report.json`
