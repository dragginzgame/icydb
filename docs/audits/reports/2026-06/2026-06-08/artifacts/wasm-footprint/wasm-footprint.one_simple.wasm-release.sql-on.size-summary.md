## Wasm Size Report: `one_simple` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 2616425 |
| icp-built deterministic `.wasm.gz` | 841397 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 2436483 |
| icp-shrunk `.wasm.gz` (canonical) | 799718 |
| Shrink delta `.wasm` | 179942 |
| Shrink delta `.wasm.gz` | 41679 |

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
