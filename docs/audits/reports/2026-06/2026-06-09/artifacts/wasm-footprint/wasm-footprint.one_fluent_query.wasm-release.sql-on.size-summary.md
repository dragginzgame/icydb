## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2478109 |
| icp-built deterministic `.wasm.gz` | 797275 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2307934 |
| icp-shrunk `.wasm.gz` (canonical) | 756696 |
| Shrink delta `.wasm` | 170175 |
| Shrink delta `.wasm.gz` | 40579 |

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
