## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2571843 |
| icp-built deterministic `.wasm.gz` | 827053 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2395255 |
| icp-shrunk `.wasm.gz` (canonical) | 786012 |
| Shrink delta `.wasm` | 176588 |
| Shrink delta `.wasm.gz` | 41041 |

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

Custom exports: `query_one_simple_fluent`

Exports (shrunk): 3

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_simple.wasm-release.report.json`
