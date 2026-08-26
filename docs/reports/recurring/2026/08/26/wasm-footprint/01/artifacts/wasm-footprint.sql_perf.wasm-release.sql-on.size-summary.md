## Wasm Size Report: `sql_perf` (wasm-release, sql-on)

| Artifact | Bytes |
| --- | ---: |
| compiler-emitted `.wasm` | 5092561 |
| final deployable `.wasm` | 4453651 |
| final deployable deterministic `.wasm.gz` | 1731325 |
| candid export | available |
| Post-link delta `.wasm` | -638910 |
| Post-link reduction | 1254 basis points |

Measurement profile: `icydb-wasm-footprint/0.220/v1` (v1)

Source revision: `12400d581c60e826e5ef8e6013366bbe18f2abd8`

Source dirty: `false`

Exact features: `candid-export,diagnostics,sql`

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

Custom exports: `canister_init`, `accepted_schema_descriptions`, `collection_mutation_scale_fact`, `query_account`, `query_account_loop_with_perf`, `query_account_with_perf`, `query_blob`, `query_blob_loop_with_perf`, `query_blob_with_perf`, `query_heap_user_loop_with_perf`, `query_heap_user_total_only_perf`, `query_heap_user_with_perf`, `query_journaled_user_loop_with_perf`, `query_journaled_user_total_only_perf`, `query_journaled_user_with_perf`, `query_streaming_execution_exhaustive_page`, `query_streaming_execution_live_page`, `query_streaming_execution_loop_with_perf`, `query_streaming_execution_with_perf`, `query_token`, `query_token_loop_with_perf`, `query_token_with_perf`, `query_user`, `query_user_attributed_total_perf`, `query_user_loop_with_perf`, `query_user_total_only_perf`, `query_user_with_perf`, `acknowledge_collection_mutation_scale_job`, `advance_collection_mutation_scale_job`, `collection_mutation_scale_job_state`, `load_account_scale_fixture`, `load_blob_scale_fixture`, `load_collection_mutation_scale_page`, `load_heap_user_scale_fixture`, `load_journal_tail_integrity_fixture`, `load_journaled_reentry_probe_fixture`, `load_journaled_user_scale_fixture`, `load_relation_integrity_fixture`, `load_streaming_execution_continuation_fixture`, `load_streaming_execution_fixture`, `load_token_scale_fixture`, `load_user_scale_fixture`, `measure_heap_user_sql_write_materialization_perf`, `measure_heap_user_write_matrix_perf`, `measure_integrity_sql_perf`, `measure_journaled_reentry_perf`, `measure_journaled_user_checked_write_perf`, `measure_journaled_user_constraint_write_perf`, `measure_journaled_user_mutation_forward_perf`, `measure_journaled_user_sql_write_materialization_perf`, `measure_journaled_user_write_matrix_perf`, `recover_collection_mutation_scale_store`, `start_collection_mutation_scale_job`, `start_journaled_user_mutation_job`, `try_collection_eager_tier_reset`, `validate_journaled_user_perf_check`, `verify_journaled_user_mutation_job_lifecycle`, `warm_account_query_with_perf`, `warm_blob_query_with_perf`, `warm_heap_user_query_with_perf`, `warm_journaled_user_query_with_perf`, `warm_streaming_execution_query_with_perf`, `warm_token_query_with_perf`, `warm_user_query_with_perf`, `canister_global_timer`, `<ic-cdk`, `canister_post_upgrade`

Exports (final deployable): 70

JSON report: `/home/adam/projects/icydb/artifacts/wasm-size/sql_perf.wasm-release.report.json`
