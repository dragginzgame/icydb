## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2486457 |
| icp-built deterministic `.wasm.gz` | 794717 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 2315722 |
| icp-shrunk `.wasm.gz` (canonical) | 754129 |
| Shrink delta `.wasm` | 170735 |
| Shrink delta `.wasm.gz` | 40588 |

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

Custom exports: `query_ten_simple_fluent`

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
