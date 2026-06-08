## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2546592 |
| icp-built deterministic `.wasm.gz` | 819374 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2372150 |
| icp-shrunk `.wasm.gz` (canonical) | 777113 |
| Shrink delta `.wasm` | 174442 |
| Shrink delta `.wasm.gz` | 42261 |

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
