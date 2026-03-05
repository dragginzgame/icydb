# Recurring Audit Summary - 2026-03-05

Run scope: all recurring audit definitions under `docs/audits/recurring/` executed and reported individually.

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity.md` (Risk: 3/10)
2. `contracts/contracts-error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
3. `contracts/contracts-resource-model-compliance` -> `resource-model-compliance.md` (PASS=7, PARTIAL=0, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 6/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation` -> `layer-violation.md` (Risk: 5/10)
7. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 5/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 6/10)
9. `cursor/cursor-ordering` -> `cursor-ordering.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity.md` (Risk: 4/10)
11. `invariants/invariants-invariant-preservation` -> `invariant-preservation.md` (Risk: 4/10)
12. `planner/planner-boundary-semantics` -> `boundary-semantics.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency.md` (Risk: 4/10)
14. `follow-up/execution-routing-refactor` -> `execution-routing-refactor-followup.md` (Refactor plan refinement)

## Global Findings

- Layer-authority checks passed with no comparator leakage outside `index/*` and no cross-layer policy re-derivation findings.
- Grouped resource-model compliance is now fully green (`PASS=7`) due explicit grouped `HAVING + ORDER + LIMIT` boundedness coverage.
- Main ongoing pressure remains continuation/anchor coordination spread across runtime files (velocity/complexity concern, not a critical correctness break).
- Re-ran `complexity-accretion` and `velocity-preservation` after continuation/route contract refactors: both remain at `6/10`; route branch pressure improved (`executor/route if: 82 -> 56`) while continuation/anchor spread remains the dominant drag signal (`849` mentions across `76` runtime files).
- Boundary semantics edge case `anchor == upper` was explicitly audited; current path short-circuits empty envelopes before range iteration (performance risk `1/10`).
- Route simplification follow-up now includes explicit `RouteShapeKind`, required `AccessRouteClass`, access-owned pushdown/index-range eligibility methods, and parity-shim retirement after soak.
- Development hardening assertion for continuation envelope containment is now present in `resume_bounds_from_refs`.
- Execution-stage routing is now shape-dispatch based in `route/planner/execution/mod.rs`, with local shape builders absorbing prior central branching.
- Continuation wiring follow-up added pure `ContinuationContract` accessor surface and moved load fast-path direction wiring to routed execution inputs (no ad-hoc `ExecutionOrderContract::from_plan(...)` in load fast-path modules).
- Continuation runtime plumbing now carries one shared `ScalarContinuationBindings` object through load/kernel materialization, replacing duplicated boundary/anchor/direction/signature parameter bundles.
- Initial-page offset semantics are now sourced from one continuation helper and consumed by both executor traversal and cursor token derivation paths.
- Scan-layer continuation wiring now carries one shared `IndexScanContinuationInput` object at executor/index boundaries, and index-range fast-path anchor wiring now consumes lowered anchors directly without `RangeToken` round-trips.
- Access-stream continuation wiring now carries one shared `AccessScanContinuationInput` object through executor access stream key/row helper boundaries, replacing duplicated anchor/direction argument pairs.
- `AccessStreamBindings` now carries continuation as one field with accessor-based reads, reducing direct primitive continuation field references in load/kernel/aggregate stream paths.
- `AccessStreamInputs` now carries continuation as one field in stream resolver internals, reducing remaining internal anchor/direction primitive spread on access-stream paths.
- Access-stream comparator selection now derives from continuation direction at the resolver boundary, removing duplicate comparator parameter plumbing.
- Route continuation policy/mode/window plumbing is now bundled behind `RouteContinuationPlan` in route planner/hint paths, reducing continuation primitive spread in router-stage derivation code.
- Downstream runtime/test consumers now read route continuation via `ExecutionRoutePlan::continuation()` only; direct `ExecutionRoutePlan` primitive continuation accessors were removed.
- Route continuation gate helpers now consume `RouteContinuationPlan` directly, removing remaining mode-primitive callsite extraction in route feasibility and scalar entrypoint continuation checks.
- Route strict-advance and grouped-safety continuation assertions now consume `RouteContinuationPlan` helper methods, removing direct continuation-policy primitive checks from feasibility/load callsites.
- Index-range continuation anchor gating now consumes `RouteContinuationPlan::index_range_limit_pushdown_allowed`, localizing the remaining continuation-mode branch to route contracts.
- Route/load continuation gates now call `RouteContinuationPlan` methods directly; free continuation gate wrapper helpers were removed from `route/contracts`.
- Grouped load continuation flow is now bundled behind grouped runtime continuation/pagination context objects, reducing grouped fold/page primitive pagination parameter spread.
- Grouped page-finalize no longer constructs grouped continuation tokens directly; grouped next-cursor creation/arity checks now flow through grouped continuation context methods.
- Grouped direction/plan-metrics/trace primitives are now read through grouped runtime projection accessors, reducing direct grouped runtime field plumbing across grouped stream/output stages.
- Grouped fold consumers now read grouped plan/route contracts via `GroupedRouteStage` accessor methods instead of direct grouped payload field reach-through.
- Grouped fold/output consumer signatures now use `GroupedRouteStageProjection` trait, making grouped stage consumption a compile-time projection boundary.
- Scalar load/kernel helpers now consume `ExecutionInputsProjection` instead of direct `ExecutionInputs` field reads, reducing scalar execution-input coupling spread.
- `ExecutionInputs` is now constructor-only at callsites (`ExecutionInputs::new(...)`), removing scalar execution-input struct literal coupling.
- `ResolvedExecutionKeyStream` now uses constructor/accessor APIs, and kernel/load/aggregate/grouped consumers no longer read its fields directly.
- Grouped stream/fold stage handoff now uses constructor/accessor APIs (`GroupedStreamStage::new/parts_mut`, `GroupedFoldStage::from_grouped_stream`) and grouped fold/output helpers no longer read grouped stage fields directly.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo fmt --all` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `cargo test -p icydb-core route_capabilities_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_field_extrema_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_load_index_range_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows -- --nocapture` -> PASS
- `cargo test -p icydb-core route_plan_mutation_ -- --nocapture` -> PASS
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` -> PASS
- `cargo test -p icydb-core load_cursor_with_offset_desc_secondary_pushdown_resume_matrix_is_boundary_complete -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_fluent_execute_initial_to_continuation_matrix_covers_offset_and_limit -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_fluent_execute_having_filters_groups_without_extra_continuation -- --nocapture` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core index_range_path_requires_pre_lowered_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core fast_stream_requires_exact_key_count_hint -- --nocapture` -> PASS

Per repository guidance, cross-filesystem test failures were not retried beyond initial attempts.
