## Wasm Size Report: `default_empty_metrics` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| compiler-emitted `.wasm` | 2430457 |
| final deployable `.wasm` | 2127698 |
| final deployable deterministic `.wasm.gz` | 815445 |
| candid export | available |
| Post-link delta `.wasm` | -302759 |
| Post-link reduction | 1245 basis points |

Measurement profile: `icydb-wasm-footprint/0.220/v1` (v1)

Source revision: `12400d581c60e826e5ef8e6013366bbe18f2abd8`

Source dirty: `false`

Exact features: `candid-export`

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_integrity` | no |
| `sql_fixtures` | no |
| `metrics` | yes |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `canister_init`, `canister_global_timer`, `<ic-cdk`, `canister_post_upgrade`

Exports (final deployable): 7

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/default_empty_metrics.wasm-release.report.json`
