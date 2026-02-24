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
    let execute_source = include_str!("../../load/execute.rs");
    let index_range_limit_source = include_str!("../../load/index_range_limit.rs");
    let page_source = include_str!("../../load/page.rs");
    let pk_stream_source = include_str!("../../load/pk_stream.rs");
    let secondary_index_source = include_str!("../../load/secondary_index.rs");
    let mod_source = include_str!("mod.rs");

    assert_no_eligibility_helper_defs("aggregate/mod.rs", aggregate_source);
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
fn aggregate_fast_path_dispatch_requires_verified_gate_marker() {
    let aggregate_source = include_str!("../../load/aggregate/mod.rs");
    assert!(
        aggregate_source.contains("struct VerifiedAggregateFastPathRoute"),
        "aggregate fast-path dispatch must define a verified route marker type",
    );
    assert!(
        aggregate_source.contains("fn verify_aggregate_fast_path_eligibility("),
        "aggregate fast-path dispatch must include one shared eligibility verifier",
    );
    assert!(
        aggregate_source.contains("Result<Option<VerifiedAggregateFastPathRoute>, InternalError>"),
        "aggregate fast-path eligibility verifier must return a verified route marker",
    );
    assert!(
        aggregate_source.contains("fn try_execute_verified_aggregate_fast_path("),
        "aggregate fast-path branch execution must flow through a verified-dispatch helper",
    );
    assert!(
        aggregate_source.contains(
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
    let aggregate_source = include_str!("../../load/aggregate/mod.rs");
    let distinct_source = include_str!("../../load/aggregate/distinct.rs");
    let fold_source = include_str!("../../fold.rs");

    assert!(
        aggregate_source.contains("build_execution_route_plan_for_aggregate_spec"),
        "aggregate execution mode must be derived by route planning",
    );
    assert!(
        aggregate_source.contains("let execution_mode = descriptor.route_plan.execution_mode;"),
        "aggregate orchestration must snapshot route-owned execution mode explicitly",
    );
    assert!(
        distinct_source.contains("build_execution_route_plan_for_load"),
        "count_distinct execution mode must be derived by route planning",
    );
    assert!(
        distinct_source.contains("let execution_mode = route_plan.execution_mode;"),
        "count_distinct orchestration must snapshot route-owned execution mode explicitly",
    );
    assert!(
        !fold_source.contains("ExecutionMode"),
        "fold internals must not own or branch on execution mode",
    );
}

#[test]
fn cursor_spine_validates_signature_direction_and_window_shape() {
    let cursor_spine_source = include_str!("../../../query/cursor/spine.rs");

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
