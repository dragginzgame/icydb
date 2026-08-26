## Wasm Size Report: `sql` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| compiler-emitted `.wasm` | 4394178 |
| final deployable `.wasm` | 3836191 |
| final deployable deterministic `.wasm.gz` | 1485875 |
| candid export | available |
| Post-link delta `.wasm` | -557987 |
| Post-link reduction | 1269 basis points |

Measurement profile: `icydb-wasm-footprint/0.220/v1` (v1)

Source revision: `12400d581c60e826e5ef8e6013366bbe18f2abd8`

Source dirty: `false`

Exact features: `candid-export,sql`

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | yes |
| `sql_update` | yes |
| `sql_integrity` | yes |
| `sql_fixtures` | no |
| `metrics` | yes |
| `metrics_extended` | no |
| `snapshot` | yes |
| `schema` | yes |

Custom exports: `canister_init`, `measure_application_behavior_perf`, `measure_identity_closeout_perf`, `seed_oversized_sql_group_name`, `canister_global_timer`, `<ic-cdk`, `canister_post_upgrade`

Exports (final deployable): 15

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/sql.wasm-release.report.json`
