### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

• Dead Code Candidates

  - query/plan.rs GroupedPlan::into_parts → Safe delete → no callsites found (including tests); grouped path never
    consumes this deconstructor.
  - cursor/mod.rs prepare_grouped_cursor → Future extension hook → zero production callsites; grouped continuation
    decoding is scaffolded but not wired.
  - cursor/mod.rs revalidate_grouped_cursor → Future extension hook → zero production callsites; grouped continuation
    lifecycle not integrated.
  - cursor/mod.rs validate_grouped_cursor_order_plan → Possibly dead but risky → only called by prepare_grouped_cursor
    (itself unused); deleting now would remove the only grouped order guard.
  - query/plan_validate.rs validate_group_query_semantics → Test-only utility → referenced from grouped validation
    tests, not production planning path.
  - query/plan_validate.rs validate_group_spec → Possibly dead but risky → currently only reachable from test-only
    grouped validator, but it is the canonical grouped semantics checker.
  - query/plan.rs GroupAggregateKind::{Exists, First, Last} → Future extension hook → no non-test usage.
  - query/plan.rs GroupAggregateKind::Count → Test-only utility → used only in grouped tests.
  - cursor/token.rs GroupedContinuationToken::encode → Future extension hook → no callsites; grouped next-cursor
    emission not implemented.
  - cursor/planned.rs GroupedPlannedCursor::last_group_key → Safe delete → no callsites.
  - executor/route/mod.rs ExecutionRoutePlan::execution_mode_case → Test-only utility → only route tests read it.
  - executor/route/mod.rs capability fields field_*_fast_path_ineligibility_reason → Unused capability flags / Test-
    only utility → populated in runtime, consumed only by #[cfg(test)] diagnostics.
  - query/plan.rs assess_secondary_order_pushdown_if_applicable → Test-only utility (#[cfg(test)]).
  - executor/route/planner.rs build_execution_route_plan_for_grouped_plan → Test-only utility (#[cfg(test)]).
  - commit/mod.rs commit_marker_present, init_commit_store_for_tests → Test-only utility (#[cfg(test)]).

  Entropy Wrappers

  - executor/aggregate/fast_path.rs verified_aggregate_fast_path_route → Delete candidate → pure pass-through wrapper
    over verify_aggregate_fast_path_eligibility with no transformation.
  - executor/aggregate/fast_path.rs fold_aggregate_over_key_stream → Delete candidate → one-call indirection to
    run_streaming_aggregate_reducer.
  - executor/load/mod.rs execute / execute_paged_with_cursor → Keep (Legitimate façade) → public boundary
    normalization (Response vs CursorPage) and staged entrypoint semantics.
  - executor/mutation/save.rs insert/update/replace/*many* wrappers → Keep (Legitimate façade) → encode write-mode
    semantics explicitly.
  - session.rs many execute_load_query_* wrappers → Keep (Legitimate façade) → session boundary + metrics/error
    mapping; high duplication but real API surface.
  - executor/mutation/mod.rs mutation_write_context → Keep (Boundary adapter) → enforces ensure_recovered_for_write
    before context creation.
  - executor/route/planner.rs test route builders (#[cfg(test)]) → Keep (Test probe).

  Layering Violations

  - executor/* (26 production files) → executor importing query internals (query::plan::* pervasive) → High.
  - executor/kernel/post_access/mod.rs → query type (AccessPlannedQuery) carries executor execution methods via impl
    in executor layer → High.
  - executor/route/capability.rs → route depends on query pushdown internals
    (assess_secondary_order_pushdown_if_applicable_validated) → Medium.
  - cursor/boundary.rs, cursor/mod.rs, cursor/spine.rs → cursor depends on query::plan::OrderSpec/OrderDirection →
    Medium.
  - executor/kernel/post_access/order_cursor.rs, executor/kernel/post_access/mod.rs → cursor logic implemented outside
    db/cursor → High.
  - query/fluent/load.rs, query/fluent/delete.rs, query/intent/mod.rs → query depends on executor::ExecutablePlan
    (reverse coupling) → Medium.
  - Confirmed: no index -> query imports in production.
  - Confirmed: no access -> query::predicate imports in production.
  - Confirmed: no commit -> query imports in production.

  Consolidation Opportunities

  - query/plan.rs → split giant planner surface (1893 LOC) into pushdown, projection, contracts/grouped units → Risk:
    Medium.
  - executor/route/planner.rs + route siblings → collapse intent/feasibility/execution policy into a single typed
    “route policy” boundary (currently spread across planner/mode/hints/capability) → Risk: Medium.
  - executor/aggregate/mod.rs + executor/route/planner.rs + executor/route/mode.rs → centralize AggregateKind decision
    matrix in one place; remove parallel branching copies → Risk: Medium.
  - executor/stream/access/mod.rs + executor/physical_path.rs → unify physical access ownership (currently split root
    + impl location) → Risk: Medium.
  - executor/delete/mod.rs + executor/mutation/save.rs + executor/mutation/commit_window.rs → factor common commit-
    window orchestration/metrics hooks; remove save/delete divergence → Risk: Medium.
  - executor/kernel/post_access/order_cursor.rs → move boundary comparison logic under db/cursor to restore subsystem
    boundary → Risk: High (hot path).
  - query/predicate/normalize (1 production file) → flatten into query/predicate root to reduce micro-fragmentation →
    Risk: Low.
  - executor/kernel/post_access/window.rs (2-file dir with mod) → inline into post_access/mod.rs unless expected
    growth → Risk: Low.
  - index/key → no high-signal consolidation found; current split (codec, ordered, builders) is coherent.

  Execution Duplication

  - Load vs aggregate fast-path dispatch → duplicated route-loop and branch eligibility flow (load/execute.rs vs
    aggregate/fast_path.rs) → Severity: High.
  - Access stream request assembly → repeated AccessPlanStreamRequest construction patterns across load/pk_stream.rs,
    load/secondary_index.rs, load/index_range_limit.rs, aggregate/fast_path.rs → Severity: Medium.
  - Fallback materialization block → near-duplicate block in kernel/mod.rs for initial pass and residual-retry pass →
    Severity: High.
  - Direction derivation → repeated scan-direction derivation in route, load helpers, executable plan cursor prep, and
    query pushdown conversion utilities → Severity: Medium.
  - Window/fetch hint math → related offset/fetch decisions distributed across route/hints.rs, route/mode.rs, route/
    planner.rs, kernel/post_access/mod.rs, window.rs → Severity: Medium.
  - Mutation commit sequencing → save and delete each orchestrate open/apply/metrics with partially duplicated
    behavior → Severity: Medium.
  - Session aggregate execution path → many near-identical execute_load_query_* terminal wrappers (plan ->
    with_metrics -> load_executor aggregate) → Severity: Low (API-level duplication).

  GROUP BY Risk Areas

  - query/intent/mod.rs → query intent has no first-class grouped state; grouped contracts are detached from intent
    pipeline → Mitigation: introduce grouped intent type before planning.
  - query/fluent/load.rs → fluent API is scalar-terminal oriented (count/min/max/...) with no grouped projection
    surface → Mitigation: add grouped query builder/result API, not scalar overloads.
  - query/plan.rs → grouped structs exist but are not integrated into normal planning flow → Mitigation: make grouped
    plan a first-class planner output variant.
  - query/plan_validate.rs → grouped semantic validation is test-only reachable → Mitigation: wire grouped validation
    into production plan construction.
  - executor/route/planner.rs → RouteIntentStage uses Option<AggregateSpec> + grouped: bool; this will not scale to
    multi-aggregate GROUP BY → Mitigation: replace with explicit enum carrying grouped aggregate list + key spec.
  - executor/aggregate/mod.rs → hard scalar assumptions (is_grouped() rejected, aggregate_specs().len() != 1 rejected)
    block GROUP BY directly → Mitigation: add grouped reducer pipeline with keyed accumulators.
  - executor/aggregate/contracts.rs → grouped contract exists, but runtime mostly consumes global-terminal path only →
    Mitigation: promote grouped contract to execution descriptor and route contract.
  - executor/route/planner.rs + executor/aggregate/mod.rs → branch complexity risk: grouped behavior is bolted onto
    scalar routing with special cases; GROUP BY will cause conditional explosion → Mitigation: separate grouped route
    mode and grouped execution stages explicitly.
  - cursor/mod.rs, cursor/token.rs, cursor/planned.rs → grouped continuation path is scaffold-only; no production
    encode/decode roundtrip → Mitigation: implement grouped cursor protocol end-to-end before enabling grouped
    pagination.
  - executor/kernel/post_access/mod.rs + response/mod.rs + session.rs → post-access and response/session surfaces
    assume row-entity outputs, not grouped rowsets → Mitigation: introduce grouped result container and grouped post-
    access/cursor semantics.