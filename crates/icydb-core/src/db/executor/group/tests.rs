use super::{
    GroupedExecutionConfig, grouped_budget_observability,
    grouped_execution_config_from_planner_config, grouped_execution_context_from_planner_config,
};

#[test]
fn grouped_execution_config_from_planner_config_prefers_planner_limits() {
    let config = grouped_execution_config_from_planner_config(Some(
        GroupedExecutionConfig::with_hard_limits(11, 2048),
    ));

    assert_eq!(config.max_groups(), 11);
    assert_eq!(config.max_group_bytes(), 2048);
}

#[test]
fn grouped_execution_context_from_planner_config_defaults_when_absent() {
    let context = grouped_execution_context_from_planner_config(None);

    assert_eq!(context.config().max_groups(), 10_000);
    assert_eq!(context.config().max_group_bytes(), 16 * 1024 * 1024);
    assert_eq!(context.budget().groups(), 0);
    assert_eq!(context.budget().aggregate_states(), 0);
    assert_eq!(context.budget().estimated_bytes(), 0);
}

#[test]
fn grouped_budget_observability_projects_budget_and_limits() {
    let context = grouped_execution_context_from_planner_config(Some(
        GroupedExecutionConfig::with_hard_limits(11, 2048),
    ));
    let budget = grouped_budget_observability(&context);

    assert_eq!(budget.groups(), 0);
    assert_eq!(budget.aggregate_states(), 0);
    assert_eq!(budget.estimated_bytes(), 0);
    assert_eq!(budget.max_groups(), 11);
    assert_eq!(budget.max_group_bytes(), 2048);
}

#[test]
fn grouped_budget_observability_contract_vectors_are_frozen() {
    let default_context = grouped_execution_context_from_planner_config(None);
    let constrained_context = grouped_execution_context_from_planner_config(Some(
        GroupedExecutionConfig::with_hard_limits(11, 2048),
    ));
    let actual_vectors = vec![
        grouped_budget_observability(&default_context),
        grouped_budget_observability(&constrained_context),
    ]
    .into_iter()
    .map(|budget| {
        (
            budget.groups(),
            budget.aggregate_states(),
            budget.estimated_bytes(),
            budget.max_groups(),
            budget.max_group_bytes(),
        )
    })
    .collect::<Vec<_>>();
    let expected_vectors = vec![(0, 0, 0, 10_000, 16 * 1024 * 1024), (0, 0, 0, 11, 2048)];

    assert_eq!(actual_vectors, expected_vectors);
}
