## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2361241 |
| icp-built deterministic `.wasm.gz` | 760783 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2202445 |
| icp-shrunk `.wasm.gz` (canonical) | 722028 |
| Shrink delta `.wasm` | 158796 |
| Shrink delta `.wasm.gz` | 38755 |

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
