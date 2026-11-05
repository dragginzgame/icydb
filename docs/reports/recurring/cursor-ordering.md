> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Cursor Ordering Audit - 2026-06-25

## 0. Run Metadata + Comparability Note

- scope: `cursor-ordering`

- recurring definition: `docs/audits/recurring/executor/cursor-ordering.md`

- compared baseline report path: `docs/audits/reports/2026-05/2026-05-11/cursor-ordering.md`

- code snapshot identifier: `d389eec3b` with dirty worktree context from earlier same-day audit/follow-up work

- method tag/version: `Method V5`

- comparability status: `non-comparable` - Method V5 replaces the removed `write_boundary_guards` integration-test target with current continuation source guards, accepted schema authority tests, and schema-fingerprint cache checks. It also adds focused grouped pagination evidence for current grouped page-finalization code.

## 7. Overall Risk Assessment

- critical issues: none found

- medium-risk drift: none found in current runtime behavior

- low-risk observations: scalar validation remains centralized; grouped cursor emission now has direct current evidence; schema-bound signatures reduce stale-cursor risk after accepted schema changes

- tests to add if coverage thins: stale cursor replay after accepted schema mutation, scalar direction mismatch, expression-index cursor replay

## Verification Readout

- `PASS`: `cargo test -p icydb-core --features sql cursor_validation -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql pk_cursor_decode_error_mapping_is_explicit_for_all_cursor_variants -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql desc_anchor_equal_to_lower_resumes_to_empty_envelope -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql load_cursor_live_state_delete_between_pages_can_shrink_remaining_results -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql runtime_continuation_semantic_definitions_stay_cursor_owned -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql runtime_cursor_signature_validation_internals_stay_cursor_owned -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql authority_finalization_uses_authority_schema_when_shape_is_missing -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql shared_query_plan_cache_schema_fingerprint_method_mismatch_fails_closed -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql sql_and_fluent_grouped_execution_match_groups_aggregates_and_cursor -- --nocapture`

- `PASS`: `cargo test -p icydb-core --features sql grouped_select_helper_limit_window_emits_cursor_and_resumes_next_group_page -- --nocapture`

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/25/cursor-ordering/01/report.md).
