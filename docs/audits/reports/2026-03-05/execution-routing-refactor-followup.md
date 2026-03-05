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
