## Wasm Size Report: `minimal_metrics` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 314118 |
| icp-built deterministic `.wasm.gz` | 117919 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 288647 |
| icp-shrunk `.wasm.gz` (canonical) | 111775 |
| Shrink delta `.wasm` | 25471 |
| Shrink delta `.wasm.gz` | 6144 |

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
