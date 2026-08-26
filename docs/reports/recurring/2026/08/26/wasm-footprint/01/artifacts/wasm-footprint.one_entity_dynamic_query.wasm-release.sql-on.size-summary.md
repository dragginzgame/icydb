## Wasm Size Report: `one_entity_dynamic_query` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| compiler-emitted `.wasm` | 3383230 |
| final deployable `.wasm` | 2971900 |
| final deployable deterministic `.wasm.gz` | 1172327 |
| candid export | available |
| Post-link delta `.wasm` | -411330 |
| Post-link reduction | 1215 basis points |

Measurement profile: `icydb-wasm-footprint/0.220/v1` (v1)

Source revision: `12400d581c60e826e5ef8e6013366bbe18f2abd8`

Source dirty: `false`

Exact features: `candid-export,request-diagnostics`

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

Custom exports: `canister_init`, `diagnose_repeated_point_queries`, `measure_parameterized_point_queries`, `measure_repeated_point_queries`, `measure_repeated_scan_queries`, `query_one_entity_dynamic_rows`, `canister_global_timer`, `<ic-cdk`, `canister_post_upgrade`

Exports (final deployable): 10

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/one_entity_dynamic_query.wasm-release.report.json`
