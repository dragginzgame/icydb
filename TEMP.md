### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly


  1. LOCATION: crates/icydb-core/src/db/executor/executable_plan.rs:568 ExecutablePlan::bytes_by_projection_mode;
     crates/icydb-core/src/db/executor/terminal/bytes.rs:57 LoadExecutor::bytes_by_projection_mode_from_prepared
     PATTERN TYPE: duplicate semantic computation
     WHY IT IS REDUNDANT: Both functions run the same four-way classification over consistency, constant covering,
     residual predicate, and covering-index context to produce the same BytesByProjectionMode.
     DELETION HYPOTHESIS: Keep one shared classifier next to BytesByProjectionMode; both plan-side preview and terminal
     execution should call it.
     CONFIDENCE: high
  2. LOCATION: crates/icydb-core/src/db/query/plan/group.rs:397 grouped_executor_handoff; crates/icydb-core/src/db/
     query/plan/group.rs:559 planned_projection_layout_and_aggregate_projection_specs_from_spec; crates/icydb-core/src/
     db/query/plan/group.rs:602 expression_without_alias; crates/icydb-core/src/db/executor/aggregate/runtime/
     grouped_output/projection.rs:21 project_grouped_rows_from_projection; crates/icydb-core/src/db/executor/aggregate/
     runtime/grouped_output/projection.rs:59 projection_is_identity_grouped_projection; crates/icydb-core/src/db/
     executor/aggregate/runtime/grouped_output/projection.rs:140 expression_without_alias
     PATTERN TYPE: projection duplication
     WHY IT IS REDUNDANT: Planner handoff already walks the grouped projection, strips aliases, classifies field-vs-
     aggregate positions, and builds grouped aggregate specs. Grouped output then re-walks the same projection to
     rediscover whether it is the identity grouped shape.
     DELETION HYPOTHESIS: Make planner-side grouped projection classification the single source of truth and carry an
     identity/projection-shape flag or reusable classified form into grouped output.
     CONFIDENCE: high
  3. LOCATION: crates/icydb-core/src/db/query/plan/group.rs:298 GroupedExecutorHandoff::projection_layout_valid; crates/
     icydb-core/src/db/query/plan/group.rs:399 grouped_executor_handoff; crates/icydb-core/src/db/executor/pipeline/
     grouped_runtime/route_stage.rs:55 LoadExecutor::resolve_grouped_route
     PATTERN TYPE: unused or write-only field
     WHY IT IS REDUNDANT: projection_layout_valid is set from validate_grouped_projection_layout(...).map(|()| true)?,
     so successful construction already proves the invariant. The stored bool is only read by a later debug_assert!.
     DELETION HYPOTHESIS: Remove the field and getter; the successful grouped_executor_handoff() result should remain
     the single proof that layout validation passed.
     CONFIDENCE: high
  4. LOCATION: crates/icydb-core/src/db/executor/route/grouped_runtime.rs:14 grouped_route_observability_for_runtime;
     crates/icydb-core/src/db/executor/pipeline/grouped_runtime/route_stage.rs:67 LoadExecutor::resolve_grouped_route
     PATTERN TYPE: dead wrapper / pass-through
     WHY IT IS REDUNDANT: The helper just calls grouped_route_plan.grouped_observability(), converts None into one
     invariant error, runs two debug asserts, and returns the same payload unchanged.
     DELETION HYPOTHESIS: Collapse this into ExecutionRoutePlan::grouped_observability() or inline it at the single
     callsite.
     CONFIDENCE: high
  5. LOCATION: crates/icydb-core/src/db/executor/executable_plan.rs:35 ExecutionStrategy; crates/icydb-core/src/db/
     query/trace.rs:9 TraceExecutionStrategy; crates/icydb-core/src/db/session/query.rs:483 trace_execution_strategy;
     crates/icydb-core/src/db/executor/route/contracts/execution/mod.rs:137 RouteExecutionMode; crates/icydb-core/src/
     db/query/explain/execution.rs:107 ExplainExecutionMode; crates/icydb-core/src/db/executor/explain/descriptor/
     shared.rs:750 explain_execution_mode
     PATTERN TYPE: shadow enum / type alias drift
     WHY IT IS REDUNDANT: ExecutionStrategy and TraceExecutionStrategy are identical 3-variant enums with a pure remap.
     RouteExecutionMode and ExplainExecutionMode are identical 2-variant enums with another pure remap.
     DELETION HYPOTHESIS: Keep one canonical execution-shape enum per concept and stringify/project at the outer
     diagnostics boundary instead of carrying mirrored enums.
     CONFIDENCE: medium
  6. LOCATION: crates/icydb-core/src/db/executor/route/contracts/execution/mod.rs:152 GroupedExecutionMode; crates/
     icydb-core/src/db/executor/plan_metrics.rs:26 record_grouped_plan_metrics; crates/icydb-core/src/metrics/
     sink.rs:100 MetricsEvent::Plan; crates/icydb-core/src/metrics/sink.rs:336 GlobalMetricsSink::record; crates/icydb-
     core/src/metrics/state.rs:47 EventOps
     PATTERN TYPE: shadow enum / type alias drift
     WHY IT IS REDUNDANT: Metrics carry grouped_execution_mode_code: Option<&'static str>, produced from
     GroupedExecutionMode::code(), and the sink immediately matches the same strings back into the two grouped counters.
     DELETION HYPOTHESIS: Store Option<GroupedExecutionMode> or a dedicated metrics enum in MetricsEvent::Plan; keep
     string codes only for external rendering.
     CONFIDENCE: high
  7. LOCATION: crates/icydb-core/src/db/query/plan/access_choice/model.rs:61 AccessChoiceSelectedReason; crates/icydb-
     core/src/db/query/plan/access_choice/model.rs:95 AccessChoiceRejectedReason; crates/icydb-core/src/db/query/plan/
     access_choice/evaluator.rs:764 chosen_selection_reason; crates/icydb-core/src/db/query/plan/access_choice/
     evaluator.rs:807 ranked_rejection_reason
     PATTERN TYPE: shadow enum / type alias drift
     WHY IT IS REDUNDANT: Both enums encode the same tie-break semantics for NonIndexAccess, ExactMatchPreferred,
     OrderCompatiblePreferred, and LexicographicTiebreak, with the same code strings, while the evaluator derives them
     from the same score comparisons.
     DELETION HYPOTHESIS: Merge overlapping ranking reasons into one taxonomy and keep outcome polarity separate from
     the reason itself.
     CONFIDENCE: medium-high
  8. LOCATION: crates/icydb-core/src/db/query/plan/group.rs:497 GroupedDistinctExecutionStrategy; crates/icydb-core/src/
     db/executor/aggregate/runtime/grouped_distinct/strategy.rs:19 GlobalDistinctFieldExecutionSpec; crates/icydb-core/
     src/db/executor/aggregate/runtime/grouped_distinct/aggregate.rs:40 GlobalDistinctFieldAggregateKind
     PATTERN TYPE: shadow enum / type alias drift
     WHY IT IS REDUNDANT: Planner lowers grouped DISTINCT into Count/Sum/Avg + target_field; executor immediately re-
     expresses the same shape as GlobalDistinctFieldExecutionSpec { target_field, aggregate_kind }, where aggregate_kind
     is another Count/Sum/Avg enum.
     DELETION HYPOTHESIS: Keep either the planner enum as the runtime contract, or lower once to a single executor spec
     and stop carrying both representations.
     CONFIDENCE: medium
  9. LOCATION: crates/icydb-core/src/db/executor/route/contracts/execution/observability.rs:17
     GroupedRouteDecisionOutcome; crates/icydb-core/src/db/executor/route/contracts/execution/plan.rs:167
     ExecutionRoutePlan::grouped_observability; crates/icydb-core/src/db/executor/route/planner/execution/
     shape_aggregate_grouped.rs:21 build_execution_stage_for_aggregate_grouped; crates/icydb-core/src/db/executor/
     pipeline/grouped_runtime/route_stage.rs:71 LoadExecutor::resolve_grouped_route
     PATTERN TYPE: legacy compatibility path
     WHY IT IS REDUNDANT: Grouped route planning hardcodes RouteExecutionMode::Materialized, and grouped runtime re-
     asserts that grouped execution remains materialized. That makes GroupedRouteDecisionOutcome::Selected and the
     Streaming => Selected branch unreachable under current behavior.
     DELETION HYPOTHESIS: Remove the Selected grouped outcome and streaming branch until grouped streaming is actually
     implemented, or remove the materialized-only invariant if streaming is meant to be real.
     CONFIDENCE: high
  10. LOCATION: crates/icydb-core/src/db/executor/preparation.rs:46 ExecutionPreparation::from_plan; crates/icydb-core/
     src/db/executor/preparation.rs:120 ExecutionPreparation::from_runtime_plan
     PATTERN TYPE: split entrypoints doing same work
     WHY IT IS REDUNDANT: Both constructors rebuild the same compiled_predicate, compile_targets, and slot-map-driven
     index program machinery, differing mainly in predicate source and whether they emit strict/capability data or
     conservative runtime data.
     DELETION HYPOTHESIS: Introduce one internal builder parameterized by predicate source and compile policy set; keep
     separate public entrypoints only if the names still help callsites.
     CONFIDENCE: medium