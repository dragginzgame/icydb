## Wasm Size Report: `minimal` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 312606 |
| icp-built deterministic `.wasm.gz` | 116451 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 287347 |
| icp-shrunk `.wasm.gz` (canonical) | 110606 |
| Shrink delta `.wasm` | 25259 |
| Shrink delta `.wasm.gz` | 5845 |

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

Custom exports: none

Exports (shrunk): 1

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/minimal.wasm-release.report.json`
