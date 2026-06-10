## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2351758 |
| icp-built deterministic `.wasm.gz` | 756352 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2192748 |
| icp-shrunk `.wasm.gz` (canonical) | 716557 |
| Shrink delta `.wasm` | 159010 |
| Shrink delta `.wasm.gz` | 39795 |

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

Custom exports: `query_one_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_fluent_query.wasm-release.report.json`
