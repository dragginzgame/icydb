//! Module: db::executor::group::tests
//! Covers grouping behavior and grouped-row invariants in executor helpers.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    GroupedExecutionConfig, grouped_budget_observability,
    grouped_execution_config_from_planner_config, grouped_execution_context_from_planner_config,
};

#[test]
fn grouped_execution_config_from_planner_config_preserves_planner_limits() {
    let config = grouped_execution_config_from_planner_config(
        GroupedExecutionConfig::with_hard_limits(11, 2048),
    );

    assert_eq!(config.max_groups(), 11);
    assert_eq!(config.max_group_bytes(), 2048);
}

#[test]
fn planner_default_grouped_execution_authority_is_finite() {
    let planner_default = GroupedExecutionConfig::planner_default_bounded();
    let executor_default = grouped_execution_config_from_planner_config(planner_default);

    assert!(planner_default.is_finite_bounded());
    assert_eq!(planner_default.max_groups(), 10_000);
    assert_eq!(planner_default.max_group_bytes(), 16 * 1024 * 1024);
    assert_eq!(executor_default.max_groups(), planner_default.max_groups());
    assert_eq!(
        executor_default.max_group_bytes(),
        planner_default.max_group_bytes(),
    );
    assert!(!GroupedExecutionConfig::unbounded().is_finite_bounded());
    assert!(!GroupedExecutionConfig::with_hard_limits(0, 1).is_finite_bounded());
    assert!(!GroupedExecutionConfig::with_hard_limits(1, 0).is_finite_bounded());
}

#[test]
fn grouped_execution_context_starts_empty_with_planner_defaults() {
    let context = grouped_execution_context_from_planner_config(
        GroupedExecutionConfig::planner_default_bounded(),
    );

    assert_eq!(context.config().max_groups(), 10_000);
    assert_eq!(context.config().max_group_bytes(), 16 * 1024 * 1024);
    assert_eq!(context.budget().groups(), 0);
    assert_eq!(context.budget().aggregate_states(), 0);
    assert_eq!(context.budget().estimated_bytes(), 0);
}

#[test]
fn grouped_budget_observability_projects_budget_and_limits() {
    for (config, max_groups, max_group_bytes) in [
        (
            GroupedExecutionConfig::planner_default_bounded(),
            10_000,
            16 * 1024 * 1024,
        ),
        (GroupedExecutionConfig::with_hard_limits(11, 2048), 11, 2048),
    ] {
        let context = grouped_execution_context_from_planner_config(config);
        let budget = grouped_budget_observability(&context);

        assert_eq!(budget.groups(), 0);
        assert_eq!(budget.aggregate_states(), 0);
        assert_eq!(budget.estimated_bytes(), 0);
        assert_eq!(budget.max_groups(), max_groups);
        assert_eq!(budget.max_group_bytes(), max_group_bytes);
    }
}
