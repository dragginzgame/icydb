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
