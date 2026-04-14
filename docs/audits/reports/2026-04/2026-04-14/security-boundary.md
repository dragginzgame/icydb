# Security Boundary

Scope: `docs/audits/recurring/security/security-audit.md`
Compared baseline report path: `N/A`
Code snapshot identifier: `279e99af1`
Method tag/version: `Method V1`
Comparability status: `comparable`
Auditor: `Codex`
Run timestamp: `2026-04-14T22:15:21+02:00`
Branch / worktree state: `main` with a dirty worktree

## Findings Table

| Check | Evidence | Status | Risk |
| ----- | -------- | ------ | ---- |
| Public SQL boundary rejects non-query lanes at the session boundary | `cargo test -p icydb-core sql_query_surfaces_reject_non_query_statement_lanes_matrix -- --nocapture` | PASS | Low |
| Grouped cursor rejection stays fail-closed for invalid payload and signature mismatch | `cargo test -p icydb-core grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy -- --nocapture` | PASS | Low |
| Recovery and replay parity checks remain green | `recovery_replay_is_idempotent`, `unique_conflict_classification_parity_holds_between_live_apply_and_replay`, `recovery_replay_interrupted_conflicting_unique_batch_fails_closed` | PASS | Low |
| Envelope containment check remains green | `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` | PASS | Low |
| Query/update cache separation and shared lower-cache reuse remain enforced | `sql_compile_cache_keeps_query_and_update_surfaces_separate`, `shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces` | PASS | Medium-Low |
| Live canister boundary rejects mutation SQL on the query endpoint | `cargo test -p icydb-testing-integration --test sql_canister sql_canister_query_endpoint_rejects_mutation_sql -- --nocapture` | PASS | Low |
| Live canister cache boundary preserves update-warms-query semantics for SQL and fluent | `sql_perf_update_warm_persists_query_cache_across_calls`, `fluent_perf_update_warm_persists_query_cache_across_calls` | PASS | Medium-Low |
| Field projection invariant script matches the current runtime module layout | `bash scripts/ci/check-field-projection-invariants.sh` | PASS | Low |
| Layer-authority invariant script matches the current envelope module layout | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Low |

## Assumptions Validated

* No auth/tenant model is present in this snapshot.
* Public SQL entrypoints remain the relevant exposed adversarial surface.
* Cache scope is present and security-relevant in this snapshot:
  * compiled-command cache
  * SQL select-plan cache
  * shared lower query-plan cache
* Standalone IC `query` calls are not relied upon as creating persistent cache state.
* Update-warmed cache reuse across later calls is present and verified by live canister tests.
* Continuation token trust model remains the same opaque-cursor contract used by the current session and canister surfaces.

## Structural Hotspots

* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/index/envelope/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/sql_surface.rs`

## Hub Module Pressure

High-fan-in security-sensitive hubs in this snapshot are:

* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/index/envelope/mod.rs`
* `/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store/mod.rs`

## Early Warning Signals

* Security-audit command names in the recurring definition have drifted from the current test names for grouped resource-policy coverage.
* There is still no dedicated live canister malformed-SQL negative test in the recurring command set, even though the session-boundary fail-closed test is green.

## Dependency Fan-In Pressure

The most security-sensitive dependency hubs remain concentrated in:

* SQL compilation and execution routing
* shared lower query-plan caching
* continuation-envelope ownership
* commit-marker and replay storage boundaries

This is acceptable, but it means drift in these modules has outsized security impact.

## Risk Score

`2 / 10`

Rationale:

* No concrete public-boundary, cursor, replay, or cache-isolation violation was reproduced.
* The stale recurring invariant scripts were corrected and now pass against the current module layout.
* Residual risk is mostly about keeping the recurring command set aligned with the evolving public canister surface.

## Verification Readout

* `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
* `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
* `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
* `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
* `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
* `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
* `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
* `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
* `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` -> PASS
* `cargo test -p icydb-core grouped_plan_rejects_validation_shape_matrix -- --nocapture` -> PASS
* `cargo test -p icydb-core sql_query_surfaces_reject_non_query_statement_lanes_matrix -- --nocapture` -> PASS
* `cargo test -p icydb-core grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy -- --nocapture` -> PASS
* `cargo test -p icydb-core shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces -- --nocapture` -> PASS
* `cargo test -p icydb-core sql_compile_cache_keeps_query_and_update_surfaces_separate -- --nocapture` -> PASS
* `cargo test -p icydb-testing-integration --test sql_canister sql_canister_query_endpoint_rejects_mutation_sql -- --nocapture` -> PASS
* `cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_update_warm_persists_query_cache_across_calls -- --nocapture` -> PASS
* `cargo test -p icydb-testing-integration --test fluent_perf_audit fluent_perf_update_warm_persists_query_cache_across_calls -- --nocapture` -> PASS

## Follow-Up Actions

* Owner boundary: `docs/audits/recurring/security`
  Action: refresh the recurring command list so grouped resource-policy coverage references current test names rather than the drifted historical filter.
  Target report run: next `security-boundary`
