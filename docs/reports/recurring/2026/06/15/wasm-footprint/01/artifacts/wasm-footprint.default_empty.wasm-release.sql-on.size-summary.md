## Wasm Size Report: `default_empty` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 37635 |
| icp-built deterministic `.wasm.gz` | 16452 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 6648 |
| icp-shrunk `.wasm.gz` (canonical) | 4108 |
| Shrink delta `.wasm` | 30987 |
| Shrink delta `.wasm.gz` | 12344 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: none

Exports (shrunk): 0

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/default_empty.wasm-release.report.json`
