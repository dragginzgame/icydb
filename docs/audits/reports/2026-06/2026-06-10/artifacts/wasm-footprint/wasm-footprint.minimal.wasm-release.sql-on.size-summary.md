## Wasm Size Report: `minimal` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 37369 |
| icp-built deterministic `.wasm.gz` | 16359 |
| candid export | unavailable |
| icp-shrunk `.wasm` (canonical) | 6516 |
| icp-shrunk `.wasm.gz` (canonical) | 4095 |
| Shrink delta `.wasm` | 30853 |
| Shrink delta `.wasm.gz` | 12264 |

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

Custom exports: none

Exports (shrunk): 0

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/minimal.wasm-release.report.json`
