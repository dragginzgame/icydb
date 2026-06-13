## Wasm Size Report: `minimal_metrics` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 314117 |
| icp-built deterministic `.wasm.gz` | 117937 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 288647 |
| icp-shrunk `.wasm.gz` (canonical) | 111774 |
| Shrink delta `.wasm` | 25470 |
| Shrink delta `.wasm.gz` | 6163 |

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_fixtures` | no |
| `metrics` | yes |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: none

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/minimal_metrics.wasm-release.report.json`
