## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2568374 |
| icp-built deterministic `.wasm.gz` | 829997 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2392849 |
| icp-shrunk `.wasm.gz` (canonical) | 788140 |
| Shrink delta `.wasm` | 175525 |
| Shrink delta `.wasm.gz` | 41857 |

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

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_fluent_query.wasm-release.report.json`
