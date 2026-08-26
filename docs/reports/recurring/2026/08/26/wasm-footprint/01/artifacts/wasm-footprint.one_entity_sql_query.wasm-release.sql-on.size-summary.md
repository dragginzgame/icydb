## Wasm Size Report: `one_entity_sql_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| compiler-emitted `.wasm` | 3730564 |
| final deployable `.wasm` | 3287248 |
| final deployable deterministic `.wasm.gz` | 1302888 |
| candid export | available |
| Post-link delta `.wasm` | -443316 |
| Post-link reduction | 1188 basis points |

Measurement profile: `icydb-wasm-footprint/0.220/v1` (v1)

Source revision: `12400d581c60e826e5ef8e6013366bbe18f2abd8`

Source dirty: `false`

Exact features: `candid-export,sql`

SQL variant: `sql-on`

Generated endpoint surface:

| Option | Enabled |
| --- | --- |
| `sql_readonly` | no |
| `sql_ddl` | no |
| `sql_update` | no |
| `sql_integrity` | no |
| `sql_fixtures` | no |
| `metrics` | no |
| `metrics_extended` | no |
| `snapshot` | no |
| `schema` | no |

Custom exports: `canister_init`, `query_one_entity_sql`, `canister_global_timer`, `<ic-cdk`, `canister_post_upgrade`

Exports (final deployable): 6

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_entity_sql_query.wasm-release.report.json`
