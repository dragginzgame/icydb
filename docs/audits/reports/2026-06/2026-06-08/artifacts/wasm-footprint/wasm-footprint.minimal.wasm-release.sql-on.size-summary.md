## Wasm Size Report: `minimal` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| icp-built `.wasm` | 328730 |
| icp-built deterministic `.wasm.gz` | 122775 |
| candid export | available |
| icp-shrunk `.wasm` (canonical) | 302135 |
| icp-shrunk `.wasm.gz` (canonical) | 116164 |
| Shrink delta `.wasm` | 26595 |
| Shrink delta `.wasm.gz` | 6611 |

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
