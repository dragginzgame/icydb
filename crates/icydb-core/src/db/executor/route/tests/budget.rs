use super::*;

#[test]
fn route_feature_budget_capability_flags_stay_within_soft_delta() {
    let capability_flags = route_capability_flag_count_guard();
    assert!(
        capability_flags <= ROUTE_CAPABILITY_FLAG_BASELINE_0247 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route capability flags exceeded soft feature budget; consolidate before adding more flags"
    );
}

#[test]
fn route_feature_budget_execution_mode_cases_stay_within_soft_delta() {
    let execution_mode_cases = route_execution_mode_case_count_guard();
    assert!(
        execution_mode_cases
            <= ROUTE_EXECUTION_MODE_CASE_BASELINE_0246 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route execution-mode branching exceeded soft feature budget; consolidate before adding more cases"
    );
}

#[test]
fn route_feature_budget_no_eligibility_helpers_outside_route_module() {
    let aggregate_source = include_str!("../../load/aggregate/mod.rs");
    let aggregate_fast_path_source = include_str!("../../load/aggregate/fast_path.rs");
    let execute_source = include_str!("../../load/execute.rs");
    let index_range_limit_source = include_str!("../../load/index_range_limit.rs");
    let page_source = include_str!("../../load/page.rs");
    let pk_stream_source = include_str!("../../load/pk_stream.rs");
    let secondary_index_source = include_str!("../../load/secondary_index.rs");
    let mod_source = include_str!("mod.rs");

    assert_no_eligibility_helper_defs("aggregate/mod.rs", aggregate_source);
    assert_no_eligibility_helper_defs("aggregate/fast_path.rs", aggregate_fast_path_source);
    assert_no_eligibility_helper_defs("execute.rs", execute_source);
    assert_no_eligibility_helper_defs("index_range_limit.rs", index_range_limit_source);
    assert_no_eligibility_helper_defs("page.rs", page_source);
    assert_no_eligibility_helper_defs("pk_stream.rs", pk_stream_source);
    assert_no_eligibility_helper_defs("secondary_index.rs", secondary_index_source);
    assert_no_eligibility_helper_defs("mod.rs", mod_source);
}

#[test]
fn load_stream_construction_routes_through_route_facade() {
    let load_sources = [
        (
            "load/aggregate/mod.rs",
            include_str!("../../load/aggregate/mod.rs"),
        ),
        (
            "load/aggregate/fast_path.rs",
            include_str!("../../load/aggregate/fast_path.rs"),
        ),
        ("load/execute.rs", include_str!("../../load/execute.rs")),
        (
            "load/fast_stream.rs",
            include_str!("../../load/fast_stream.rs"),
        ),
        (
            "load/index_range_limit.rs",
            include_str!("../../load/index_range_limit.rs"),
        ),
        ("load/pk_stream.rs", include_str!("../../load/pk_stream.rs")),
        (
            "load/secondary_index.rs",
            include_str!("../../load/secondary_index.rs"),
        ),
    ];

    for (path, source) in load_sources {
        assert!(
            !source_uses_direct_context_stream_construction(source),
            "{path} must construct streams via route::RoutedKeyStreamRequest facade",
        );
    }
}

#[test]
fn load_fast_path_resolution_is_gated_by_route_execution_mode() {
    let execute_source = include_str!("../../load/execute.rs");

    assert!(
        execute_source.contains("match route_plan.execution_mode"),
        "load execution must branch on route-owned execution mode before fast-path evaluation",
    );
    assert!(
        execute_source.contains("ExecutionMode::Materialized => FastPathDecision::None"),
        "materialized load routes must bypass fast-path stream attempts",
    );
}

#[test]
fn aggregate_fast_path_dispatch_requires_verified_gate_marker() {
    let aggregate_fast_path_source = include_str!("../../load/aggregate/fast_path.rs");
    assert!(
        aggregate_fast_path_source.contains("struct VerifiedAggregateFastPathRoute"),
        "aggregate fast-path dispatch must define a verified route marker type",
    );
    assert!(
        aggregate_fast_path_source.contains("fn verify_aggregate_fast_path_eligibility("),
        "aggregate fast-path dispatch must include one shared eligibility verifier",
    );
    assert!(
        aggregate_fast_path_source
            .contains("Result<Option<VerifiedAggregateFastPathRoute>, InternalError>"),
        "aggregate fast-path eligibility verifier must return a verified route marker",
    );
    assert!(
        aggregate_fast_path_source.contains("fn try_execute_verified_aggregate_fast_path("),
        "aggregate fast-path branch execution must flow through a verified-dispatch helper",
    );
    assert!(
        aggregate_fast_path_source.contains(
            "let Some(verified_route) = Self::verify_aggregate_fast_path_eligibility(inputs, route)?"
        ),
        "aggregate fast-path loop must obtain a verified marker before branch execution",
    );
}

#[test]
fn ranked_terminal_families_share_one_ranked_row_helper() {
    let terminal_source = include_str!("../../load/terminal/mod.rs");
    assert!(
        terminal_source.contains("fn rank_k_rows_from_materialized("),
        "ranked terminals must expose one shared ranked-row helper",
    );
    assert!(
        terminal_source.contains("Self::rank_k_rows_from_materialized("),
        "top/bottom terminal helpers must route through the shared ranked-row helper",
    );
    assert!(
        terminal_source.contains("RankedFieldDirection::Descending"),
        "top-k ranking must route through descending field direction",
    );
    assert!(
        terminal_source.contains("RankedFieldDirection::Ascending"),
        "bottom-k ranking must route through ascending field direction",
    );
}

#[test]
fn ranked_terminals_remain_materialized_without_heap_streaming_path() {
    let terminal_source = include_str!("../../load/terminal/mod.rs");

    assert!(
        terminal_source.contains("let response = self.execute(plan)?;"),
        "ranked terminals must run over canonical materialized execute() responses in 0.29",
    );
    assert!(
        !terminal_source.contains("BinaryHeap"),
        "0.29 must defer heap-streaming top-k optimization to preserve current ranking semantics",
    );
}

#[test]
fn aggregate_execution_mode_selection_is_route_owned_and_explicit() {
    let aggregate_orchestration_source = include_str!("../../load/aggregate/orchestration.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let distinct_source = include_str!("../../load/aggregate/distinct.rs");
    let fold_source = include_str!("../../fold.rs");

    assert!(
        aggregate_orchestration_source.contains("build_execution_route_plan_for_aggregate_spec"),
        "aggregate execution mode must be derived by route planning",
    );
    assert!(
        kernel_aggregate_source.contains("AggregateReducerDispatch::from_descriptor(&descriptor)"),
        "aggregate orchestration must derive reducer dispatch from one descriptor boundary",
    );
    assert!(
        kernel_aggregate_source.contains("descriptor.route_plan.execution_mode"),
        "aggregate reducer dispatch must remain route-execution-mode driven",
    );
    assert!(
        aggregate_orchestration_source
            .contains("ExecutionKernel::execute_aggregate_spec(self, plan, spec)"),
        "load aggregate entrypoint must delegate orchestration ownership to execution kernel",
    );
    assert!(
        kernel_aggregate_source
            .contains("executor.execute_materialized_aggregate_spec(plan, spec)"),
        "kernel aggregate orchestration should route materialized terminals through one shared helper boundary",
    );
    assert!(
        distinct_source.contains("let response = self.execute(plan)?;"),
        "count_distinct must run through canonical execute() orchestration",
    );
    assert!(
        !distinct_source.contains("build_execution_route_plan_for_load"),
        "count_distinct should not carry standalone route orchestration once unified",
    );
    assert!(
        !fold_source.contains("ExecutionMode"),
        "fold internals must not own or branch on execution mode",
    );
}

#[test]
fn strict_index_predicate_compile_policy_has_one_executor_source_of_truth() {
    let planner_source = include_str!("../planner.rs");
    let kernel_predicate_source = include_str!("../../kernel/predicate.rs");

    assert!(
        !planner_source.contains(".compile_index_program_strict("),
        "route planner must not compile strict index predicates directly; use shared executor helper",
    );
    assert!(
        planner_source.contains("compile_index_predicate_program_from_slots("),
        "route planner strict predicate policy must call the shared executor compile helper",
    );
    assert!(
        kernel_predicate_source.contains("match mode"),
        "kernel predicate helper must own the compile-mode switch boundary",
    );
    assert!(
        kernel_predicate_source.contains("IndexPredicateCompileMode::StrictAllOrNone"),
        "kernel predicate helper must include strict all-or-none compilation policy",
    );
}

#[test]
fn aggregate_streaming_paths_share_one_preparation_boundary() {
    let aggregate_orchestration_source = include_str!("../../load/aggregate/orchestration.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");

    assert!(
        aggregate_orchestration_source.contains("fn prepare_aggregate_streaming_inputs("),
        "aggregate execution must expose one shared streaming-input preparation helper",
    );
    assert_eq!(
        aggregate_orchestration_source
            .matches("prepare_aggregate_streaming_inputs(plan)?;")
            .count(),
        1,
        "load aggregate helpers should call shared preparation exactly once",
    );
    assert!(
        aggregate_orchestration_source
            .contains("let prepared = self.prepare_aggregate_streaming_inputs(plan)?;"),
        "field-extrema streaming should call the shared preparation helper directly",
    );
    assert!(
        kernel_aggregate_source
            .contains("let prepared = executor.prepare_aggregate_streaming_inputs(plan)?;"),
        "kernel aggregate orchestration should call the shared preparation helper",
    );
    assert_eq!(
        aggregate_orchestration_source
            .matches("plan.index_prefix_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming spec extraction should be defined in one shared helper only",
    );
    assert_eq!(
        aggregate_orchestration_source
            .matches("plan.index_range_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming range-spec extraction should be defined in one shared helper only",
    );
}

#[test]
fn aggregate_fast_path_folding_uses_shared_stream_helpers() {
    let aggregate_fast_path_source = include_str!("../../load/aggregate/fast_path.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");

    assert!(
        aggregate_fast_path_source.contains("fn fold_aggregate_over_key_stream("),
        "aggregate fast-path folding must expose a shared stream-fold helper",
    );
    assert!(
        aggregate_fast_path_source.contains("fn fold_aggregate_from_fast_path_result("),
        "aggregate fast-path folding must expose a shared fast-path fold helper",
    );
    assert!(
        kernel_aggregate_source.contains("fn fold_aggregate_from_routed_stream_request"),
        "aggregate routed-stream aggregate folding must expose a shared helper",
    );
    assert!(
        kernel_aggregate_source.contains("fn try_fold_secondary_index_aggregate"),
        "aggregate secondary-index probe/fallback folding must expose a shared helper",
    );
    assert_eq!(
        aggregate_fast_path_source
            .matches("ExecutionKernel::decorate_key_stream_for_plan(")
            .count(),
        1,
        "aggregate fast-path DISTINCT decoration should be wired in one helper only",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("LoadExecutor::<E>::resolve_routed_key_stream(")
            .count(),
        1,
        "aggregate routed-stream resolution should be centralized in one helper only",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("LoadExecutor::<E>::try_execute_secondary_index_order_stream(")
            .count(),
        1,
        "aggregate secondary-index stream resolution should be centralized in one helper only",
    );
    assert!(
        aggregate_fast_path_source
            .matches("fold_aggregate_from_fast_path_result(")
            .count()
            >= 2,
        "aggregate fast-path call sites should route through shared fast-path folding helper",
    );
    assert!(
        aggregate_fast_path_source
            .matches("fold_aggregate_from_routed_stream_request(")
            .count()
            >= 3,
        "aggregate routed-stream call sites should route through shared routed-stream helper",
    );
}

#[test]
fn cursor_spine_validates_signature_direction_and_window_shape() {
    let cursor_spine_source = include_str!("../../cursor/spine.rs");

    assert!(
        cursor_spine_source.contains(
            "validate_cursor_signature(entity_path, &expected_signature, &token.signature())"
        ),
        "cursor spine must validate continuation signatures before boundary materialization",
    );
    assert!(
        cursor_spine_source
            .contains("validate_cursor_direction(expected_direction, actual_direction)?;"),
        "cursor spine must validate cursor direction against executable direction",
    );
    assert!(
        cursor_spine_source.contains(
            "validate_cursor_window_offset(expected_initial_offset, actual_initial_offset)?;"
        ),
        "cursor spine must validate cursor window shape (initial offset) before boundary decode",
    );
}
