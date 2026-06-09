## Wasm Size Report: `one_fluent_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2479651 |
| icp-built deterministic `.wasm.gz` | 799690 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2309470 |
| icp-shrunk `.wasm.gz` (canonical) | 757102 |
| Shrink delta `.wasm` | 170181 |
| Shrink delta `.wasm.gz` | 42588 |

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
