//! Module: db::executor::tests::route::budget_matrix
//! Responsibility: module-local ownership and contracts for db::executor::tests::route::budget_matrix.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
fn route_feature_budget_shape_kinds_stay_within_soft_delta() {
    let route_shape_kinds = route_shape_kind_count_guard();
    assert!(
        route_shape_kinds <= ROUTE_SHAPE_KIND_BASELINE_0256 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
        "route shape-kind partitioning exceeded soft feature budget; consolidate before adding more shape variants",
    );
}

#[test]
fn route_grouped_runtime_revalidation_flags_match_baseline() {
    let flags = grouped_ordered_runtime_revalidation_flag_count_guard();
    assert_eq!(
        flags, ROUTE_GROUPED_RUNTIME_REVALIDATION_FLAG_BASELINE_0251,
        "grouped ordered-route runtime revalidation flags changed; keep grouped semantics planner-owned and runtime revalidation capability-focused",
    );
}
