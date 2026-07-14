# Recurring Audit Summary - 2026-03-05

Run scope: all recurring audit definitions under `docs/audits/recurring/` executed and reported individually.

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity.md` (Risk: 3/10)
2. `contracts/contracts-error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
3. `contracts/contracts-resource-model-compliance` -> `resource-model-compliance.md` (PASS=7, PARTIAL=0, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 5/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation` -> `layer-violation.md` (Risk: 5/10)
7. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 5/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 5/10)
9. `cursor/cursor-ordering` -> `cursor-ordering.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity.md` (Risk: 4/10)
11. `invariants/invariants-invariant-preservation` -> `invariant-preservation.md` (Risk: 4/10)
12. `planner/planner-boundary-semantics` -> `boundary-semantics.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency.md` (Risk: 4/10)
14. `follow-up/execution-routing-refactor` -> `execution-routing-refactor-followup.md` (Refactor plan refinement)
15. `follow-up/decode-cursor-authority` -> `decode-cursor-authority-followup.md` (Session decode leak closure)
16. `follow-up/load-entrypoints-continuation-contract` -> `load-entrypoints-continuation-contract-followup.md` (Scalar continuation assembly leak reduction)
17. `follow-up/load-entrypoints-cursor-resolution` -> `load-entrypoints-cursor-resolution-followup.md` (Cursor compatibility/revalidation leak reduction)
18. `follow-up/load-entrypoints-shape-dispatch` -> `load-entrypoints-shape-dispatch-followup.md` (Load shape compatibility branch reduction)
19. `follow-up/load-entrypoints-final-surface` -> `load-entrypoints-final-surface-followup.md` (Payload-shape branch reduction in paging/surface materialization)
20. `follow-up/load-entrypoints-wrapper-typed-surface` -> `load-entrypoints-wrapper-typed-surface-followup.md` (Wrapper-level surface conversion check removal)
21. `follow-up/load-entrypoints-module-split` -> `load-entrypoints-module-split-followup.md` (Entrypoint hub split into scalar/grouped modules)
22. `follow-up/access-stream-anchor-interpretation` -> `access-stream-anchor-interpretation-followup.md` (Stream layer anchor interpretation removal)
23. `follow-up/access-stream-anchor-interpretation-pass-2` -> `access-stream-anchor-interpretation-followup.md` (Removed stream anchor exposure and migrated remaining index-range fast-path callsites to continuation input)
24. `follow-up/access-stream-anchor-interpretation-pass-3` -> `access-stream-anchor-interpretation-followup.md` (Access continuation carrier now stores index-layer continuation contract directly)
25. `follow-up/load-hub-pk-boundary-decode-authority` -> `decode-cursor-authority-followup.md` (Moved PK fast-path boundary decode authority from load hub to continuation runtime)
26. `follow-up/load-entrypoint-scalar-invariant-centralization` -> `load-entrypoints-continuation-contract-followup.md` (Moved scalar strict-advance/effective-offset continuation invariant checks from load entrypoint to continuation runtime context)
27. `follow-up/load-page-scan-budget-continuation-guard-centralization` -> `load-entrypoints-continuation-contract-followup.md` (Moved load page scan-budget continuation precondition checks into continuation bindings)
28. `follow-up/load-kernel-cursor-boundary-read-collapse` -> `load-entrypoints-continuation-contract-followup.md` (Replaced direct load/kernel/route-mode cursor-boundary protocol reads with continuation-owned helpers)
29. `follow-up/route-mode-continuation-projection-centralization` -> `load-entrypoints-continuation-contract-followup.md` (route/mode continuation mode/window derivation now consumes continuation-owned projection helpers)
30. `follow-up/route-contract-assertion-dto-decoupling` -> `load-entrypoints-continuation-contract-followup.md` (scalar continuation invariant assertions now consume a minimal DTO instead of full route contract)
31. `follow-up/load-runtime-hub-split` -> `load-runtime-hub-split-followup.md` (Moved grouped runtime context/window types out of `load/mod.rs` into dedicated module)
32. `follow-up/route-window-math-guard-test` -> `load-runtime-hub-split-followup.md` (Added structural test that fails if `compute_page_window(` appears under `executor/route`)
31. `follow-up/router-window-math-projection-collapse` -> `load-entrypoints-continuation-contract-followup.md` (router no longer computes keep/fetch window math; continuation runtime now projects full route window inputs)
33. `rerun/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 5/10; effective-flow and semantic-layer corrections removed overcounting inflation)
34. `rerun/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 5/10; revised CAF + containment + density-adjusted shock radius confirm AccessPath decision surface as the main drag)

## Global Findings

- Layer-authority checks passed with no comparator leakage outside `index/*` and no cross-layer policy re-derivation findings.
- Grouped resource-model compliance is now fully green (`PASS=7`) due explicit grouped `HAVING + ORDER + LIMIT` boundedness coverage.
- Main ongoing pressure remains continuation/anchor coordination spread across runtime files (velocity/complexity concern, not a critical correctness break).
- Re-ran `complexity-accretion` and `velocity-preservation` with upgraded method definitions (decision-owner vs execution-consumer vs plumbing split, effective-flow model, semantic-vs-transport layer split, revised CAF + containment, density-adjusted enum shock radius, and refactor-noise filters): both now score `5/10`; route branch pressure is roughly flat (`executor/route if: 56 -> 57`, `match: 15 -> 13`) while continuation/anchor spread remains the dominant drag signal (`891` mentions across `79` runtime files).
- Complexity Step 4 overcount correction is now applied: continuation classification is `Decision Owners=10`, `Execution Consumers=48`, `Plumbing=21` (previous owner-only heuristic was inflated).
- Runtime hub pressure improved at the file-size layer (`>=600 LOC runtime files: 12 -> 11`) due the access-stream module split.
- Boundary semantics edge case `anchor == upper` was explicitly audited; current path short-circuits empty envelopes before range iteration (performance risk `1/10`).
- Route simplification follow-up now includes explicit `RouteShapeKind`, required `AccessRouteClass`, access-owned pushdown/index-range eligibility methods, and parity-shim retirement after soak.
- Development hardening assertion for continuation envelope containment is now present in `resume_bounds_from_refs`.
- Execution-stage routing is now shape-dispatch based in `route/planner/execution/mod.rs`, with local shape builders absorbing prior central branching.
- Continuation wiring follow-up added pure `ContinuationContract` accessor surface and moved load fast-path direction wiring to routed execution inputs (no ad-hoc `ExecutionOrderContract::from_plan(...)` in load fast-path modules).
- Continuation runtime plumbing now carries one shared `ScalarContinuationBindings` object through load/kernel materialization, replacing duplicated boundary/anchor/direction/signature parameter bundles.
- Initial-page offset semantics are now sourced from one continuation helper and consumed by both executor traversal and cursor token derivation paths.
- Session-layer continuation decode no longer calls `decode_cursor` directly; cursor token decode authority is now centralized in `db/cursor::decode_optional_cursor_token`.
- Load hub no longer decodes PK cursor boundaries directly; PK fast-path boundary decode now routes through `ScalarContinuationContext::validate_pk_fast_path_boundary` under route shape gating.
- Scalar load entrypoint no longer hosts strict-advance/effective-offset continuation protocol assertions; these now live in `ResolvedScalarContinuationContext::debug_assert_route_continuation_invariants`.
- Load page materialization no longer re-derives continuation scan-budget preconditions; this guard now lives in `ScalarContinuationBindings::validate_load_scan_budget_hint`.
- Load/kernel/route-mode continuations no longer inspect cursor-boundary presence directly at callsites; continuation runtime now exposes `continuation_applied`/`effective_keep_count_for_limit` and scalar-context route helpers for these protocol decisions.
- route/mode continuation derivation now consumes `ScalarContinuationContext` projection helpers (`route_continuation_mode`, `route_window_projection_for_plan`) instead of branching on raw boundary/token presence.
- scalar continuation invariant assertions now consume `ScalarRouteContinuationInvariantProjection`, reducing continuation-runtime coupling to full route contract types.
- Grouped runtime context/window definitions were split from `load/mod.rs` into `load/grouped_runtime.rs`, reducing load hub concentration without changing behavior.
- Added a guard test that enforces router boundary discipline by rejecting `compute_page_window(` in `executor/route` sources.
- route continuation window assembly now consumes `ScalarRouteWindowProjection` (including precomputed keep/fetch counts), and router no longer runs `compute_page_window` math.
- Practical stopping rule now holds on continuation routing: router continuation path is projection mapping to route contracts; continuation semantics/math/invariants are owned by continuation runtime.
- Scalar load entrypoints no longer unpack continuation anchor/token primitives directly; `ResolvedScalarContinuationContext` now owns scalar continuation binding/access-scan input assembly.
- Load entrypoint cursor compatibility + revalidation decisions now resolve through `ContinuationEngine::resolve_load_cursor_context`, removing local scalar/grouped cursor decision branching from `build_execution_context`.
- Load entrypoint mode/order compatibility now resolves through `RequestedLoadExecutionShape` + continuation resolver contracts, and grouping/projection dispatch now matches resolved cursor shape only.
- Load entrypoint paging/surface stages now emit typed final output (`LoadExecutionSurface`) and use centralized payload extractors, removing repeated scalar/grouped mismatch branch cross-products from `apply_paging` and `materialize_surface`.
- Public load entrypoint wrappers now call mode-typed helper boundaries directly, removing wrapper-level `LoadExecutionSurface` conversion helper checks.
- Load entrypoint implementation is now split into `entrypoints/mod.rs` + `entrypoints/scalar.rs` + `entrypoints/grouped.rs`, reducing single-file hub pressure while keeping one shared stage-order root.
- Access-stream physical index-range resolution now consumes one index-layer continuation contract (`IndexScanContinuationInput`) from `AccessScanContinuationInput::index_scan_continuation()`, removing stream-layer direct anchor read/interpretation in physical resolver wiring.
- Index-range stream fast-path wiring now consumes `AccessScanContinuationInput` end-to-end (including aggregate fast-path callsites), and stream-level raw anchor exposure helpers were removed.
- `AccessScanContinuationInput` now stores one `IndexScanContinuationInput` internally, removing remaining stream-owned anchor field/state representation while preserving existing runtime behavior.
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
- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> BLOCKED (local environment issue during test execution)
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> BLOCKED (local environment issue during test execution)
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> BLOCKED (local environment issue during test execution)
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
- `cargo test -p icydb-core --lib cursor::tests` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core index_range_path_requires_pre_lowered_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core fast_stream_requires_exact_key_count_hint -- --nocapture` -> PASS

Per repository guidance, environment-blocked test failures were not retried beyond initial attempts.
