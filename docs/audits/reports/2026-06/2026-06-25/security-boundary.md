# Security Boundary

Scope: `docs/audits/recurring/security/security-audit.md`
Compared baseline report path: `docs/audits/reports/2026-04/2026-04-14/security-boundary.md`
Code snapshot identifier: `e60dee9aa`
Method tag/version: `Security Boundary Method V2`
Comparability status: `non-comparable` - Method V2 refreshes owner paths, current script inventory, required SQL feature flags, grouped resource-policy checks, cache fail-closed checks, and read-only run discipline.
Auditor: `Codex`
Run timestamp: `2026-06-25T09:53:53+02:00`
Branch / worktree state: dirty worktree; initial read-only audit run after updating only the audit definition and this report. Supplemental drift review updated only the invariant script and this report.
Supplemental drift review: `check-index-range-spec-invariants.sh` was reconciled after the read-only run showed a stale helper-name expectation. The script still requires ordered-suffix identity in the cache key and fingerprint, but now matches the canonical `IndexBranchSetOrderedSuffix::label()` owner introduced by `0.184.42`.

## Method Changes

Method V2 updates the recurring definition before this run:

* declares the historical report scope as `security-boundary`
* fixes stale owner paths from `db/session/query.rs` and `db/session/sql/explain.rs`
* removes historical standalone invariant-script names that no longer exist in `scripts/ci/`
* adds `--features sql` to SQL/security `icydb-core` test commands
* adds current grouped resource-policy and shared-cache fail-closed test filters
* records read-only run constraints for canister and external-service checks

Because the verification method changed, deltas against Method V1 are treated as `N/A (method change)`.

## Findings Table

| Check | Evidence | Status | Risk |
| ----- | -------- | ------ | ---- |
| Audit definition freshness | Updated Method V2 in `docs/audits/recurring/security/security-audit.md` before this run. | PASS | Low |
| Public SQL query/update surface separation | `sql_query_surfaces_reject_non_query_statement_lanes_matrix` passed with `--features sql`. | PASS | Low |
| Invalid grouped SQL projection rejection | `execute_sql_query_rejects_invalid_grouped_projection_shapes` passed. | PASS | Low |
| Grouped resource-policy guardrails | `grouped_plan_rejects_validation_shape_matrix`, `grouped_plan_having_order_limit_composition_enforces_bounded_policy`, and `route_grouped_runtime_revalidation_flags_match_baseline` passed. | PASS | Low |
| Continuation/cursor tamper resistance | `grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy` and `anchor_containment_guard_rejects_out_of_envelope_anchor` passed. | PASS | Low |
| Recovery/replay fail-closed behavior | `recovery_replay_is_idempotent`, unique-conflict parity, and interrupted conflicting unique replay tests passed. | PASS | Low |
| SQL/shared cache surface isolation | shared plan-cache reuse, cache key/version/schema mismatch fail-closed checks, and SQL query/update compile-cache separation passed. | PASS | Low |
| Memory and layer authority static invariants | `check-memory-id-invariants.sh` and `check-layer-authority-invariants.sh` passed. | PASS | Low |
| Index/range branch-set identity guardrail | `check-index-range-spec-invariants.sh` passed after its stale helper-name expectations were reconciled with the centralized `IndexBranchSetOrderedSuffix::label()` implementation. | PASS | Low |
| Live canister public boundary checks | Not run in read-only mode because the integration harness builds/stages fixture canister WASM before installing into the standalone testkit. | BLOCKED | Medium-Low |

## Assumptions Validated

* No auth/tenant model is present in this snapshot.
* Public SQL entrypoints remain the relevant exposed adversarial surface.
* SQL compiled-command cache lanes remain explicitly surface-scoped by `SqlCompiledCommandSurface`.
* Shared query-plan cache fail-closed regression tests exist and pass for key version, schema fingerprint method, and schema version mismatches.
* Branch-set ordered-suffix identity is still included in shared plan-cache identity and query fingerprint identity.
* Read-only run mode means no product-code fixes, no canister builds, and no external service lifecycle changes.

## Structural Hotspots

* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/compile_cache.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/explain.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/query/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/cursor/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/index/envelope/tests.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store/mod.rs`
* `/home/adam/projects/icydb/scripts/ci/check-index-range-spec-invariants.sh`

## Early Warning Signals

* The index/range invariant script had drifted with respect to the `0.184.42` label-owner refactor; the guardrail now checks the centralized label path.
* Older reports referenced `check-field-projection-invariants.sh` and `check-architecture-text-scan-invariants.sh`; those files are no longer present in the current `scripts/ci/` inventory.
* Live canister coverage remains unavailable in read-only mode because the harness stages fixture WASM before installing testkit canisters.

## Risk Score

`3 / 10`

Rationale:

* Focused core SQL, cursor, recovery, grouped resource-policy, and cache-isolation tests passed.
* Security-adjacent static invariant scripts passed after reconciling one stale pattern-only guardrail.
* Live canister checks were intentionally blocked by read-only constraints.
* No concrete public-boundary fail-open behavior was reproduced in the executed checks.

## Verification Readout

* `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
* `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
* `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
* `cargo test -p icydb-core --features sql sql_query_surfaces_reject_non_query_statement_lanes_matrix -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql execute_sql_query_rejects_invalid_grouped_projection_shapes -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql db::query::plan::tests::group::grouped_plan_rejects_validation_shape_matrix -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql db::query::plan::tests::group::grouped_plan_having_order_limit_composition_enforces_bounded_policy -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql db::executor::planning::route::tests::route_grouped_runtime_revalidation_flags_match_baseline -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql recovery_replay_is_idempotent -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql shared_query_plan_cache_key_version_mismatch_fails_closed -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql shared_query_plan_cache_schema_fingerprint_method_mismatch_fails_closed -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql shared_query_plan_cache_schema_version_mismatch_fails_closed -- --nocapture` -> PASS
* `cargo test -p icydb-core --features sql sql_compile_cache_keeps_query_and_update_surfaces_separate -- --nocapture` -> PASS
* `cargo test -p icydb-testing-integration --test sql_canister sql_canister_query_endpoint_rejects_mutation_sql -- --nocapture` -> BLOCKED: read-only run did not build/stage fixture canister WASM.
* `cargo test -p icydb-testing-integration --test sql_canister sql_canister_query_endpoint_rejects_malformed_sql -- --nocapture` -> BLOCKED: read-only run did not build/stage fixture canister WASM.
* `cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_update_warm_persists_query_cache_across_calls -- --nocapture` -> BLOCKED: read-only run did not build/stage fixture canister WASM.
* `cargo test -p icydb-testing-integration --test fluent_perf_audit fluent_perf_update_warm_persists_query_cache_across_calls -- --nocapture` -> BLOCKED: read-only run did not build/stage fixture canister WASM.

## Follow-Up Actions

* Owner boundary: `testing/integration`
  Action: rerun live canister public-boundary checks in a non-read-only audit window where fixture WASM staging is allowed.
  Target report run: next `security-boundary` run.
