## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2480993 |
| icp-built deterministic `.wasm.gz` | 799081 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2310808 |
| icp-shrunk `.wasm.gz` (canonical) | 757669 |
| Shrink delta `.wasm` | 170185 |
| Shrink delta `.wasm.gz` | 41412 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_reset` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_one_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_fluent_query.wasm-release.report.json`
