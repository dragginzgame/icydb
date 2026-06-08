## Wasm Size Report: `ten_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2596315 |
| icp-built deterministic `.wasm.gz` | 829270 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2418068 |
| icp-shrunk `.wasm.gz` (canonical) | 788003 |
| Shrink delta `.wasm` | 178247 |
| Shrink delta `.wasm.gz` | 41267 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_fixtures` | no |
| `metrics` | yes |
| `metrics_reset` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `query_ten_simple_fluent`

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/ten_simple.wasm-release.report.json`
