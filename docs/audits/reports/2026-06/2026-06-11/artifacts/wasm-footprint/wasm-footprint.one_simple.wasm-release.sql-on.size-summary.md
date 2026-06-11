## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2272108 |
| icp-built deterministic `.wasm.gz` | 730146 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2113490 |
| icp-shrunk `.wasm.gz` (canonical) | 690406 |
| Shrink delta `.wasm` | 158618 |
| Shrink delta `.wasm.gz` | 39740 |

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
