## Wasm Size Report: `minimal` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 362960 |
| icp-built deterministic `.wasm.gz` | 130590 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 333067 |
| icp-shrunk `.wasm.gz` (canonical) | 123367 |
| Shrink delta `.wasm` | 29893 |
| Shrink delta `.wasm.gz` | 7223 |

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

Exports (shrunk): 2

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/minimal.wasm-release.report.json`
