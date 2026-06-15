## Wasm Size Report: `ten_entity_fluent_rows` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2106834 |
| icp-built deterministic `.wasm.gz` | 662739 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 1960120 |
| icp-shrunk `.wasm.gz` (canonical) | 631452 |
| Shrink delta `.wasm` | 146714 |
| Shrink delta `.wasm.gz` | 31287 |

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

Custom exports: `query_ten_entity_fluent_rows`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_entity_fluent_rows.wasm-release.report.json`
