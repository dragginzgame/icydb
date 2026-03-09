# Execution Routing Refactor Follow-Up - 2026-03-05

Scope: `crates/icydb-core/src/db/executor/route/` with planner/access/executor boundaries.

## 1. Router Structure Map

### Top-Level Entrypoints

| Router Function | Branch Count | Conditions Used | Execution Paths |
|---|---:|---|---|
| `build_execution_route_plan_for_load` (`route/planner/mod.rs`) | 0 | none (preflight only) | load route assembly |
| `build_execution_route_plan_for_mutation` (`route/planner/mod.rs`) | 1 | `mode.is_delete()` | mutation route vs invariant error |
| `build_execution_route_plan_for_aggregate_spec_with_preparation` (`route/planner/mod.rs`) | 0 | none | aggregate route assembly |
| `build_execution_route_plan_for_grouped_handoff` (`route/planner/mod.rs`) | 0 | none | grouped route assembly |
| `build_execution_route_plan` (`route/planner/mod.rs`) | 0 | none (stage pipeline) | intent -> feasibility -> execution -> contract |

### Routing Logic Hotspots

| Router Function | Branch Count | Conditions Used | Execution Paths |
|---|---:|---|---|
| `derive_route_intent_stage` (`route/planner/intent.rs`) | 3 | `RouteIntent::{Load, Aggregate, AggregateGrouped}` | fast-path order + grouped hint wiring |
| `derive_route_feasibility_stage` (`route/planner/feasibility.rs`) | 3 | index-range gate rejection, load/agg split, page/continuation guarding | feasibility with/without index-range pushdown |
| `derive_route_derivation_context` (`route/planner/feasibility.rs`) | 5 | aggregate present, grouped present, count pushdown, hint gates | direction/capability/hint contract synthesis |
| `derive_route_execution_stage` (`route/planner/execution/mod.rs`) | 4 (dispatch-oriented) | shape mapping + scalar-shape invariant guard | `Load`, `AggregateCount`, `AggregateNonCount`, `AggregateGrouped` |
| `derive_secondary_pushdown_applicability_from_contract` (`route/pushdown.rs`) | 2 | planner pushdown eligibility + ORDER presence | pushdown applicability delegated to access class |
| `derive_route_capabilities` + helpers (`route/capability.rs`) | medium (distributed) | access class, direction, aggregate policy payload | capability matrix used by route decisions |

Observation: the central router path is still deriving feature capability and policy, not just dispatching shape.

## 2. Feature Combination Matrix

| Feature | Current Location | Router Branch? | Should It Be? |
|---|---|---|---|
| grouped vs non-grouped | `route/planner/execution/mod.rs` + feasibility | Yes | Planner shape (`RouteShapeKind`) |
| count vs non-count aggregate | `route/planner/execution/mod.rs` | Yes | Planner shape (`RouteShapeKind`) |
| load vs aggregate | intent/execution stages | Yes | Planner shape (`RouteShapeKind`) |
| continuation mode/policy | `route/mode.rs`, `route/contracts.rs`, feasibility | Yes | Router contract assembly (keep) |
| secondary pushdown applicability | `route/pushdown.rs` + feasibility use | Reduced | Access contract method + planner logical eligibility |
| index-range-limit eligibility | `route/capability.rs` + feasibility/hints | Reduced | Access class method + executor hint policy |
| field-extrema fast-path eligibility | `aggregate/capability.rs` policy payload | Reduced | Aggregate planning/policy contract |
| grouped strategy revalidation | feasibility | Yes | Keep local to grouped shape module |
| access shape introspection (single/composite/reverse/index details) | route + executor access dispatcher | Reduced | Access layer (`AccessRouteClass`) |

Decision boundary: router should consume precomputed contracts, not re-derive capabilities.

## 3. Execution Shape Model

Existing near-shape signal already exists: `ExecutionModeRouteCase` (`route/contracts.rs`), but it is computed late inside execution routing.

Recommended shape authority:

```rust
enum RouteShapeKind {
    LoadScalar,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
    MutationDelete,
}
```

Mapping to current code paths:
- `LoadScalar` -> current `ExecutionModeRouteCase::Load`
- `AggregateCount` -> current `ExecutionModeRouteCase::AggregateCount`
- `AggregateNonCount` -> current `ExecutionModeRouteCase::AggregateNonCount`
- `AggregateGrouped` -> current `ExecutionModeRouteCase::AggregateGrouped`
- `MutationDelete` -> current `ExecutionRoutePlan::for_mutation` path

Access-side companion contract (new):

```rust
pub struct AccessRouteClass {
    pub single_path: bool,
    pub range_scan: bool,
    pub prefix_scan: bool,
    pub ordered: bool,
    pub reverse_supported: bool,
}
```

This should be produced once from lowered executable access shape and consumed by router/executor policy code.

## 4. Router Refactor Plan

Target: router becomes shape dispatcher + contract assembler.

- Eliminate from central router:
  - grouped/count/load branching in `derive_route_execution_stage`
  - repeated access-shape inspections (`lower_executable_access_plan` + capability derivations)
  - field-level aggregate fast-path eligibility checks
- Move to planner outputs:
  - `RouteShapeKind`
  - aggregate policy contract for extrema/streaming fast-path eligibility
- Move to access/index layers:
  - `AccessRouteClass` and shape eligibility methods used for pushdown/index-range decisions
- Keep in router:
  - continuation/window contract assembly
  - final dispatch by shape
- Keep in executor/load modules:
  - local execution-policy branches specific to one shape handler

Target dispatcher form:

```rust
match route_shape_kind {
    RouteShapeKind::LoadScalar => shape_load::build(...),
    RouteShapeKind::AggregateCount => shape_aggregate_count::build(...),
    RouteShapeKind::AggregateNonCount => shape_aggregate_non_count::build(...),
    RouteShapeKind::AggregateGrouped => shape_grouped::build(...),
    RouteShapeKind::MutationDelete => shape_mutation::build(...),
}
```

## 4.1 Current Implementation Status (This Run)

- Completed:
  - `RouteShapeKind` exists and is stored on `ExecutionRoutePlan`.
  - `planner/execution.rs` was split into directory module shape files:
    - `planner/execution/mod.rs`
    - `planner/execution/shape_load.rs`
    - `planner/execution/shape_aggregate_count.rs`
    - `planner/execution/shape_aggregate_non_count.rs`
    - `planner/execution/shape_aggregate_grouped.rs`
  - `derive_route_execution_stage` now performs shape dispatch into per-shape stage builders instead of central execution-mode/pushdown branching.
  - legacy execution-case parity shim was retired after route matrix soak; scalar stage derivation now uses shape mapping only.
  - mutation shape is now treated as unreachable in scalar execution-stage dispatch.
  - `AccessRouteClass` now owns:
    - `secondary_order_pushdown_applicability(...)`
    - `index_range_limit_pushdown_shape_eligible_for_order(...)`
  - Route pushdown/capability callsites now delegate to access class for the above checks.
  - Field-extrema fast-path eligibility policy moved out of route and into aggregate capability policy.
  - `ContinuationContract` gained pure semantic accessors used by contract-owned cursor logic:
    - `page_limit()`
    - `window_size()`
    - `access_plan()`
    - `grouped_cursor_policy_violation()`
    - `effective_offset(cursor_present)`
  - Runtime continuation naming now uses `ScalarContinuationContext` across executor runtime and route tests (legacy alias removed).
  - Runtime continuation primitive spread across load/kernel materialization now uses one shared boundary object:
    - `ScalarContinuationBindings` bundles cursor boundary, previous index-range anchor, routed direction, and continuation signature.
    - `load/page`, `kernel/mod`, and `kernel/post_access` consume this object instead of four parallel continuation parameters.
  - Route continuation primitive spread across planner/hint derivation now uses one shared boundary object:
    - `RouteContinuationPlan` bundles continuation mode, continuation policy, and route window.
    - `planner/feasibility` and `route/hints` now consume this object instead of threading those three continuation primitives separately.
    - `ExecutionRoutePlan` stores the same projection object and now exposes `continuation()` as the single continuation-routing accessor boundary.
    - scalar load entrypoints and route matrix tests now consume continuation through that one projection object; direct route-plan primitive continuation accessors were removed.
    - continuation activation/policy-gate helpers now consume `RouteContinuationPlan` directly (`continuation_applied`, load scan-budget policy gate), keeping continuation-mode extraction localized to route contracts.
    - strict-advance and grouped-safety assertions now consume `RouteContinuationPlan` helper methods (`strict_advance_required_when_applied`, `grouped_safe_when_applied`) in feasibility/load assertions.
    - index-range continuation anchor gating now consumes `RouteContinuationPlan::index_range_limit_pushdown_allowed`, localizing the remaining continuation-mode branch to route contracts.
    - removed remaining free continuation gate wrapper helpers in `route/contracts`; route/load callsites now invoke `RouteContinuationPlan` methods directly (`applied`, `index_range_limit_pushdown_allowed`, `load_scan_budget_hint_allowed`).
  - Grouped continuation runtime primitive spread now uses one shared grouped boundary object:
    - `GroupedContinuationContext` bundles grouped continuation signature, boundary arity, and grouped pagination projection.
    - `GroupedPaginationWindow` projects grouped limit/offset/selection/resume contracts for grouped fold/page stages.
    - `GroupedRuntimeProjection` bundles grouped direction, grouped plan-metrics strategy, and optional grouped execution trace.
    - `GroupedRouteStage` now exposes grouped plan/route contract accessors; grouped fold helpers consume those accessors instead of direct `planner_payload` / `route_payload` field reach-through.
    - `GroupedRouteStageProjection` now defines the grouped stage-consumer compile-time boundary; grouped fold/output helper signatures consume this trait instead of concrete grouped stage internals.
    - `grouped_fold` helpers now consume grouped pagination through that object instead of threading grouped pagination primitives in parallel.
    - grouped next-cursor construction and grouped boundary-arity validation now flow through `GroupedContinuationContext::grouped_next_cursor(...)` instead of page-finalize-local primitive handling.
  - Scalar execution-input coupling now uses one compile-time projection boundary:
    - `ExecutionInputsProjection` defines scalar execution-input consumer accessors.
    - scalar load fast-path resolver helpers and kernel materialization helpers now consume that trait instead of direct `ExecutionInputs` field reads.
    - `ExecutionInputs` is now private-by-construction and built via `ExecutionInputs::new(...)` at executor callsites.
  - Resolved key-stream coupling now uses constructor/accessor ownership:
    - `ResolvedExecutionKeyStream` now exposes constructor/accessor APIs (`new`, `into_parts`, stream/metrics accessors).
    - kernel distinct decoration and scalar/grouped aggregate/load consumers now read/mutate resolved streams through those APIs instead of direct field access.
  - Grouped stream/fold stage coupling now uses constructor/accessor ownership:
    - `GroupedStreamStage` now exposes `new(...)` + `parts_mut(...)` in load stage handoff.
    - `GroupedFoldStage` now exposes `from_grouped_stream(...)` and accessor reads for grouped output observability.
    - grouped fold/output helpers consume those APIs instead of direct grouped stage field reads.
  - Scan-layer continuation boundary now uses one shared object:
    - `IndexScanContinuationInput` bundles index-range resume anchor plus direction.
    - executor access scan adapters and index-store scan entrypoint now consume this object instead of `anchor + direction` primitive pairs.
  - Access-stream continuation boundary now uses one shared object:
    - `AccessScanContinuationInput` bundles optional index-range anchor plus direction for access-stream physical resolver calls.
    - `executor/stream/access` key-stream and row-stream helper boundaries consume this object instead of `anchor + direction` primitive pairs.
  - `AccessStreamBindings` now carries continuation as one field:
    - bindings expose continuation-derived `direction()` / `index_range_anchor()` accessors.
    - load/kernel/aggregate stream callsites consume continuation through bindings accessors instead of separate anchor/direction fields.
  - `AccessStreamInputs` now also carries continuation as one field in `executor/stream/access` resolver internals, replacing remaining internal anchor/direction primitive pairs on that path.
  - Access-stream comparator plumbing was simplified:
    - removed separate `key_comparator` fields from `AccessExecutionDescriptor` / `AccessStreamInputs`.
    - union/intersection comparator selection now derives from continuation direction at resolver boundary.
  - Initial-vs-continuation offset semantics now route through one helper:
    - `effective_offset_for_cursor_window(...)` in query-plan continuation authority.
    - consumed by both `executor/traversal.rs` and `cursor/continuation.rs`.
  - Index-range fast-path anchor wiring now consumes lowered anchor directly (removed redundant `LoweredKey -> RangeToken -> LoweredKey` conversion in load fast-path path).
  - Load fast-path modules no longer derive ordering ad-hoc via `ExecutionOrderContract::from_plan(...)`:
    - `load/pk_stream.rs`
    - `load/secondary_index.rs`
    now consume routed stream direction from execution inputs.
- Remaining:
  - none for the current planned refactor slices; continue soak via recurring route matrix coverage.

## 5. Safe Migration Steps

1. Introduce `RouteShapeKind` and `shape_from_intent_stage(...)` in route planner.
2. Add parity guard (`ExecutionShapeParityGuard`) in `derive_route_execution_stage`:
   - compute old execution case
   - compute case from `RouteShapeKind`
   - `debug_assert_eq!(old, new)`
3. Store `RouteShapeKind` in `ExecutionRoutePlan` (read-only, behavior-neutral).
4. Introduce `AccessRouteClass` in `db/access/execution_contract.rs` and expose `class()` from lowered executable access plan.
5. Add adapter methods in route policy code to read `AccessRouteClass` once per plan (no behavior changes yet).
6. Extract field-extrema eligibility to aggregate planning/policy contract; router reads policy flag only.
7. Split central execution routing into per-shape builders and leave only `match shape` in the router core.
8. Remove obsolete feature-derivation branches after parity guard has stayed stable across test/audit runs. (Completed)

Each step is independently shippable and keeps behavior constant until the final cleanup step.

## 6. Expected Complexity Reduction

Estimated outcomes after full migration:

| Metric | Before | After |
|---|---:|---:|
| `executor/route` `if` branches | ~82 | ~30-40 |
| central router branches | ~12 (single hotspot) | ~4-6 |
| router decision dimensions | ~8 | ~3 |
| router role | feature derivation + dispatch | contract assembly + dispatch |

Primary correctness risks to watch:
- shape mapping drift during transition (mitigated by route matrix/budget/grouped+mutation tests after parity removal)
- grouped continuation/order contract regressions
- pushdown/index-range capability semantics drift while moving to `AccessRouteClass`
- mutation route accidentally defaulting to scalar load shape

Primary performance risk to preserve during migration:
- keep fast empty short-circuit behavior for continuation edge `anchor == upper` (already audited).

## Verification (2026-03-05)

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
