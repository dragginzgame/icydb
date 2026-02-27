### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

 Scope: static architectural audit of crates/icydb-core/src/db (no code changes).

  Dead Code Candidates

  - None found as high-confidence Safe delete in production paths.
  - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:89) -> Test-
    only utility -> build_execution_route_plan_for_aggregate is #[cfg(test)].
  - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:98) -> Test-
    only utility -> build_execution_route_plan_for_aggregate_spec is #[cfg(test)].
  - executor/route/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/mod.rs:462) -> Test-only
    utility -> route_capability_flag_count_guard is only for test budget/guard checks.
  - executor/route/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/mod.rs:481) -> Test-only
    utility -> route_execution_mode_case_count_guard is test-only.
  - query/plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan.rs:277) -> Test-only utility ->
    assess_secondary_order_pushdown_if_applicable is #[cfg(test)].
  - access/plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/access/plan.rs:388) -> Test-only utility ->
    assess_secondary_order_pushdown_if_applicable_from_parts is #[cfg(test)].
  - query/predicate/validate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/predicate/validate.rs:84) ->
    Test-only utility -> validate_model is #[cfg(test)].
  - commit/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/mod.rs:73) -> Test-only utility ->
    commit_marker_present is #[cfg(test)].
  - commit/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/mod.rs:82) -> Test-only utility ->
    init_commit_store_for_tests is #[cfg(test)].
  - query/builder/field.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/builder/field.rs:49) -> Future
    extension hook -> text_eq_ci has only test references in-repo, but it is public builder API (risky to remove).

  Entropy Wrappers

  - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:79) ->
    Delete candidate -> validate_mutation_route_stage only calls build_execution_route_plan_for_mutation and returns
    Ok(()).
  - executor/stream/access/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/stream/access/mod.rs:356)
    -> Delete candidate -> lower_primary_key_access is a pure pass-through to ordered_key_stream_from_access.
  - executor/stream/access/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/stream/access/mod.rs:372)
    -> Delete candidate -> lower_secondary_index_access is identical pass-through; no extra boundary semantics.
  - cursor/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/cursor/mod.rs:102) -> Delete candidate ->
    validated_cursor_order_plan duplicates validated_cursor_order_internal.
  - cursor/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/cursor/mod.rs:124) -> Delete candidate ->
    duplicate logic of the prior function.
  - query/predicate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/predicate/mod.rs:13) -> Keep
    (Legitimate façade) -> explicit predicate-lowering seam for query/execution model boundary.
  - commit/recovery.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:70) -> Keep (Boundary
    adapter) -> write-side recovery gate semantics (not mere forwarding).
  - executor/load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:224) -> Keep
    (Legitimate façade) -> non-traced API boundary over traced execution path.

  Layering Violations

  - executor/kernel/post_access/order_cursor.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/kernel/
    post_access/order_cursor.rs:1) -> Cursor comparator/boundary semantics live outside db/cursor -> High.
  - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:10) ->
    executor route planner directly depends on query pushdown helper
    (assess_secondary_order_pushdown_if_applicable_validated) -> Medium (cross-layer policy coupling).
  - Explicitly confirmed: no index -> query internals, no access -> query predicate internals, and no commit -> query
    types in non-test code.

  Consolidation Opportunities

  - executor/route (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route) -> Consolidate route derivation
    into one immutable derivation context (direction, capabilities, hints, applicability) -> Medium.
  - executor/aggregate (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate) -> Centralize
    AggregateKind behavior mapping to reduce repeated match trees across contracts/fast-path/helpers -> Medium-High.
  - cursor/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/cursor/mod.rs:102) -> Merge duplicated cursor-
    order validators into one function -> Low.
  - executor/stream/access/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/stream/access/mod.rs:356)
    -> Collapse twin pass-through access lowerers -> Low.
  - query/plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan.rs:277) + access/plan.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/access/plan.rs:388) -> Unify secondary ORDER BY pushdown applicability
    ownership under one subsystem root -> Medium.
  - executor/stream (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/stream) and executor/load/terminal (/
    home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal) -> flatten tiny single-file roots only
    where no invariant boundary is encoded -> Low.

  Execution Duplication

  - Load vs aggregate fast-path routing -> both iterate fast_path_order with per-route eligibility + dispatch loops
    (load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/execute.rs:162), aggregate/
    fast_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/fast_path.rs:201)) -> Medium-
    High.
  - Direction derivation policy duplicated across executor layers (executable_plan.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/executor/executable_plan.rs:21), route/mode.rs (/home/adam/projects/icydb/crates/icydb-core/src/
    db/executor/route/mode.rs:24)) -> Medium.
  - Pushdown eligibility logic split across query/access/route boundaries (query/plan.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/query/plan.rs:277), access/plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    access/plan.rs:388), route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/
    planner.rs:145)) -> Medium.
  - Mutation entry checks repeated in save/delete flows (ensure_recovered_for_write, plan/context preflight) (mutation/
    save.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/mutation/save.rs:184), delete/mod.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/delete/mod.rs:125)) -> Medium.
  - Cursor continuation behavior split between kernel post-access and load page assembly (kernel/post_access/mod.rs (/
    home/adam/projects/icydb/crates/icydb-core/src/db/executor/kernel/post_access/mod.rs:312), load/page.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/load/page.rs:99)) -> Medium-High.

  GROUP BY Risk Areas

  - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:135) -> God-
    function/conditional explosion already present; GROUP BY will multiply branching -> Mitigation: split into staged
    derivation pipeline with typed intermediate structs.
  - executor/aggregate/contracts.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/
    contracts.rs:41) -> AggregateSpec models one terminal aggregate only -> Mitigation: add multi-aggregate/group
    contract (group_keys + aggregate_specs) before execution changes.
  - executor/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/mod.rs:264) ->
    execute_aggregate_spec is scalar-terminal oriented -> Mitigation: separate grouped reducer stage from scalar
    terminal stage.
  - executor/aggregate/fast_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/
    fast_path.rs:201) -> route-kind branching duplicated and tightly coupled to scalar outputs -> Mitigation:
    capability-driven strategy table per aggregate family.
  - executor/kernel/reducer.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/kernel/reducer.rs:112) ->
    reducer output type is single AggregateOutput<E>; no per-group state abstraction -> Mitigation: introduce group
    accumulator map abstraction at kernel boundary.
  - executor/route/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/mod.rs:481) -> execution
    mode taxonomy is Load | AggregateCount | AggregateNonCount only -> Mitigation: add explicit grouped execution mode
    instead of overloading aggregate non-count path.
  - query/plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan.rs:1) and query/plan_validate.rs (/
    home/adam/projects/icydb/crates/icydb-core/src/db/query/plan_validate.rs:1) -> no group-key/group-stage planning
    surface -> Mitigation: add dedicated grouping plan node and validation pass.
  - executor/kernel/post_access/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/kernel/post_access/
    mod.rs:1) -> post-access pipeline assumes row-level flow; grouping risks leaking into this layer -> Mitigation:
    insert dedicated “group stage” between access and post-access.
  - executor/load/page.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/page.rs:99) + cursor/mod.rs
    (/home/adam/projects/icydb/crates/icydb-core/src/db/cursor/mod.rs:29) -> continuation tokens are row-boundary based,
    not group-boundary based -> Mitigation: define grouped continuation token contract early.


› Find and fix a bug in @filename

  gpt-5.3-codex high · 78% left · ~/projects/icydb