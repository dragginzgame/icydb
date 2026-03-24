//! Module: db::executor::tests::route::aggregate_matrix
//! Responsibility: module-local ownership and contracts for db::executor::tests::route::aggregate_matrix.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::{
    executor::{
        ExecutionPreparation,
        plan_metrics::GroupedPlanMetricsStrategy,
        route::{
            AggregateSeekSpec, GroupedExecutionStrategy,
            build_execution_route_plan_for_grouped_plan,
            grouped_plan_metrics_strategy_for_execution_strategy,
        },
    },
    index::{IndexCompilePolicy, compile_index_program},
    query::explain::{
        ExplainGroupAggregate, ExplainGroupField, ExplainGroupHaving, ExplainGroupHavingClause,
        ExplainGroupHavingSymbol, ExplainGroupedStrategy, ExplainGrouping,
    },
    query::plan::{GroupDistinctPolicyReason, GroupedPlanStrategyHint, grouped_plan_strategy_hint},
};

// Snapshot grouped policy decisions across planner, grouped handoff, and route projection.
fn grouped_policy_snapshot(
    plan: &AccessPlannedQuery,
) -> (
    GroupedPlanStrategyHint,
    Option<GroupDistinctPolicyReason>,
    GroupedExecutionStrategy,
    bool,
) {
    let planner_hint =
        grouped_plan_strategy_hint(plan).expect("grouped plans should project planner hints");
    let handoff = grouped_executor_handoff(plan).expect("grouped plans should project handoff");
    let distinct_violation = handoff.distinct_policy_violation_for_executor();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        handoff.base(),
        handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped plans should always project grouped route observability");

    (
        planner_hint,
        distinct_violation,
        grouped_observability.grouped_execution_strategy(),
        grouped_observability.eligible(),
    )
}

fn scalar_aggregate_route_snapshot(
    plan: &AccessPlannedQuery,
    aggregate: crate::db::query::builder::AggregateExpr,
) -> String {
    let route_plan =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            plan,
            aggregate.clone(),
        );

    [
        format!("aggregate_kind={:?}", aggregate.kind()),
        "grouped=false".to_string(),
        format!("distinct_mode={}", aggregate.is_distinct()),
        format!("target_field={:?}", aggregate.target_field()),
        format!(
            "route_strategy={:?}",
            route_plan.shape().execution_mode_case()
        ),
        format!("execution_mode={:?}", route_plan.shape().execution_mode()),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

fn grouped_aggregate_route_snapshot(plan: &AccessPlannedQuery) -> String {
    let planner_hint =
        grouped_plan_strategy_hint(plan).expect("grouped route snapshot requires grouped hint");
    let handoff = grouped_executor_handoff(plan).expect("grouped route snapshot requires handoff");
    let aggregate_contracts = handoff
        .aggregate_exprs()
        .iter()
        .map(|aggregate| {
            format!(
                "{:?}:{:?}:{}",
                aggregate.kind(),
                aggregate.target_field(),
                aggregate.is_distinct()
            )
        })
        .collect::<Vec<_>>();
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        handoff.base(),
        handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route snapshot requires grouped observability payload");

    [
        "grouped=true".to_string(),
        format!("planner_hint={planner_hint:?}"),
        format!("aggregate_contracts={aggregate_contracts:?}"),
        format!(
            "route_strategy={:?}",
            route_plan.shape().execution_mode_case()
        ),
        format!("execution_mode={:?}", route_plan.shape().execution_mode()),
        format!(
            "grouped_execution_strategy={:?}",
            grouped_observability.grouped_execution_strategy()
        ),
        format!("fold_mode={:?}", route_plan.aggregate_fold_mode),
    ]
    .join("\n")
}

#[test]
fn route_plan_aggregate_uses_route_owned_fast_path_order() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Exists,
    );

    assert_eq!(route_plan.fast_path_order(), &AGGREGATE_FAST_PATH_ORDER);
    assert_eq!(route_plan.grouped_observability(), None);
}

#[test]
fn route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths() {
    let mut base =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );

    assert_eq!(
        route_plan.shape().execution_mode_case(),
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_keeps_blocking_shape_under_tight_budget_config() {
    let mut base =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(1, 1),
    });

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );

    assert_eq!(
        route_plan.shape().execution_mode_case(),
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        crate::db::executor::route::GroupedExecutionStrategy::OrderedMaterialized
    );
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_when_residual_predicate_exists() {
    let mut grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_for_unsupported_having_operator() {
    let grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(1)]),
            }],
        }),
    );

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");
    let planner_hint =
        grouped_plan_strategy_hint(&grouped).expect("grouped plans should project strategy hints");

    assert_eq!(
        planner_hint,
        GroupedPlanStrategyHint::HashGroup,
        "unsupported grouped HAVING operators should be planner-policy rejected from ordered-group hints",
    );

    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_preserves_kind_matrix_in_query_handoff() {
    let kind_cases = [
        AggregateKind::Count,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ];
    let grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: kind_cases
                    .iter()
                    .map(|kind| GroupAggregateSpec {
                        kind: *kind,
                        target_field: None,
                        distinct: false,
                    })
                    .collect(),
                execution: GroupedExecutionConfig::unbounded(),
            });

    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 1);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), kind_cases.len());
    for (index, expected_kind) in kind_cases.iter().enumerate() {
        assert_eq!(
            grouped_handoff.aggregate_exprs()[index].kind(),
            *expected_kind
        );
        assert_eq!(
            grouped_handoff.aggregate_exprs()[index].target_field(),
            None
        );
    }
}

#[test]
fn route_plan_grouped_wrapper_preserves_target_field_in_query_handoff() {
    let grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank", "label"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });

    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), 1);
    assert_eq!(
        grouped_handoff.aggregate_exprs()[0].kind(),
        AggregateKind::Max
    );
    assert_eq!(
        grouped_handoff.aggregate_exprs()[0].target_field(),
        Some("rank")
    );
}

#[test]
fn route_plan_grouped_wrapper_preserves_supported_target_field_matrix_in_query_handoff() {
    let grouped_cases = [
        (AggregateKind::Count, None),
        (AggregateKind::Exists, None),
        (AggregateKind::Min, None),
        (AggregateKind::Min, Some("rank")),
        (AggregateKind::Sum, Some("rank")),
        (AggregateKind::Avg, Some("rank")),
        (AggregateKind::Max, None),
        (AggregateKind::Max, Some("label")),
        (AggregateKind::First, None),
        (AggregateKind::Last, None),
    ];
    let grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank", "label"]),
                aggregates: grouped_cases
                    .iter()
                    .map(|(kind, target_field)| GroupAggregateSpec {
                        kind: *kind,
                        target_field: target_field.map(str::to_string),
                        distinct: false,
                    })
                    .collect(),
                execution: GroupedExecutionConfig::unbounded(),
            });

    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(grouped_handoff.aggregate_exprs().len(), grouped_cases.len());
    for (index, (expected_kind, expected_target)) in grouped_cases.iter().enumerate() {
        let aggregate = &grouped_handoff.aggregate_exprs()[index];
        assert_eq!(aggregate.kind(), *expected_kind);
        assert_eq!(aggregate.target_field(), *expected_target);
    }
}

#[test]
fn route_plan_grouped_wrapper_observability_vector_is_frozen() {
    let grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
            });

    let grouped_handoff = grouped_executor_handoff(&grouped)
        .expect("grouped logical plans should build grouped handoff");
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    let observability = route_plan
        .grouped_observability()
        .expect("grouped route should always project grouped observability for grouped intents");
    let actual = (
        observability.outcome(),
        observability.rejection_reason(),
        observability.eligible(),
        observability.execution_mode(),
        observability.grouped_execution_strategy(),
    );
    let expected = (
        GroupedRouteDecisionOutcome::MaterializedFallback,
        None,
        true,
        RouteExecutionMode::Materialized,
        crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized,
    );

    assert_eq!(actual, expected);
}

#[test]
fn grouped_policy_snapshot_matrix_remains_consistent_across_planner_handoff_and_route() {
    // Phase 1: ordered-capable grouped shape should remain fully aligned.
    let ordered_grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    assert_eq!(
        grouped_policy_snapshot(&ordered_grouped),
        (
            GroupedPlanStrategyHint::OrderedGroup,
            None,
            GroupedExecutionStrategy::OrderedMaterialized,
            true,
        )
    );

    // Phase 2: grouped HAVING-policy rejection should force HashGroup across boundaries.
    let having_rejected_grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(1)]),
            }],
        }),
    );
    assert_eq!(
        grouped_policy_snapshot(&having_rejected_grouped),
        (
            GroupedPlanStrategyHint::HashGroup,
            None,
            GroupedExecutionStrategy::HashMaterialized,
            true,
        )
    );

    // Phase 3: scalar DISTINCT grouped policy violations must stay planner-projected.
    let mut scalar_distinct_grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    scalar_distinct_grouped.scalar_plan_mut().distinct = true;
    assert_eq!(
        grouped_policy_snapshot(&scalar_distinct_grouped),
        (
            GroupedPlanStrategyHint::HashGroup,
            Some(GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired),
            GroupedExecutionStrategy::HashMaterialized,
            true,
        )
    );
}

#[test]
fn grouped_policy_snapshot_global_distinct_field_target_kind_matrix_includes_avg() {
    for kind in [AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg] {
        let grouped =
            AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
                .into_grouped(GroupSpec {
                    group_fields: Vec::new(),
                    aggregates: vec![GroupAggregateSpec {
                        kind,
                        target_field: Some("rank".to_string()),
                        distinct: true,
                    }],
                    execution: GroupedExecutionConfig::unbounded(),
                });

        assert_eq!(
            grouped_policy_snapshot(&grouped),
            (
                GroupedPlanStrategyHint::HashGroup,
                None,
                GroupedExecutionStrategy::HashMaterialized,
                true,
            ),
            "global DISTINCT grouped strategy snapshot should stay stable for {kind:?}",
        );
    }
}

#[test]
fn route_plan_grouped_explain_projection_and_execution_contract_is_frozen() {
    let group_field = grouped_field_slot("rank");
    let grouped = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: vec![group_field.clone()],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(17, 8192),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    // Phase 1: freeze explain-surface grouped projection shape.
    assert_eq!(
        grouped.explain().grouping,
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
            group_fields: vec![ExplainGroupField {
                slot_index: group_field.index(),
                field: group_field.field().to_string(),
            }],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            having: Some(ExplainGroupHaving {
                clauses: vec![ExplainGroupHavingClause {
                    symbol: ExplainGroupHavingSymbol::AggregateIndex { index: 0 },
                    op: CompareOp::Gt,
                    value: Value::Uint(1),
                }],
            }),
            max_groups: 17,
            max_group_bytes: 8192,
        },
        "grouped explain projection must preserve strategy, fields, aggregates, having, and hard limits",
    );

    // Phase 2: freeze grouped route execution-mode and grouped-strategy selection.
    let grouped_handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");
    assert_eq!(grouped_handoff.execution().max_groups(), 17);
    assert_eq!(grouped_handoff.execution().max_group_bytes(), 8192);
    let route_plan = build_execution_route_plan_for_grouped_plan(
        RouteMatrixEntity::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy_hint(),
    );
    assert_eq!(
        route_plan.shape().execution_mode_case(),
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should always project grouped observability");
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_strategy(),
        GroupedExecutionStrategy::OrderedMaterialized
    );
}

#[test]
fn grouped_route_strategy_to_metrics_strategy_mapping_is_stable() {
    for (route_strategy, expected_metrics_strategy) in [
        (
            GroupedExecutionStrategy::HashMaterialized,
            GroupedPlanMetricsStrategy::HashMaterialized,
        ),
        (
            GroupedExecutionStrategy::OrderedMaterialized,
            GroupedPlanMetricsStrategy::OrderedMaterialized,
        ),
    ] {
        assert_eq!(
            grouped_plan_metrics_strategy_for_execution_strategy(route_strategy),
            expected_metrics_strategy,
            "grouped route strategy must map to stable grouped metrics strategy labels",
        );
    }
}

#[test]
fn aggregate_route_snapshot_for_scalar_count_is_stable() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::count());
    let expected = [
        "aggregate_kind=Count".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=None".to_string(),
        "route_strategy=AggregateCount".to_string(),
        "execution_mode=Streaming".to_string(),
        "fold_mode=KeysOnly".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar COUNT aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_sum_field_is_stable() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::sum("rank"));
    let expected = [
        "aggregate_kind=Sum".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar SUM(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_avg_field_is_stable() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::avg("rank"));
    let expected = [
        "aggregate_kind=Avg".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar AVG(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_grouped_field_aggregates_is_stable() {
    let grouped =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Avg,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });

    let actual = grouped_aggregate_route_snapshot(&grouped);
    let expected = [
        "grouped=true".to_string(),
        "planner_hint=HashGroup".to_string(),
        "aggregate_contracts=[\"Avg:Some(\\\"rank\\\"):false\"]".to_string(),
        "route_strategy=AggregateGrouped".to_string(),
        "execution_mode=Materialized".to_string(),
        "grouped_execution_strategy=HashMaterialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "grouped field-aggregate route snapshot drifted; grouped planner/route/executor strategy is stabilized",
    );
}

#[test]
fn aggregate_route_strategy_parity_for_scalar_avg_matches_sum_field() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let sum_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            crate::db::sum("rank"),
        );
    let avg_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            crate::db::avg("rank"),
        );

    assert_eq!(
        avg_route.shape().execution_mode_case(),
        sum_route.shape().execution_mode_case()
    );
    assert_eq!(
        avg_route.shape().execution_mode(),
        sum_route.shape().execution_mode()
    );
    assert_eq!(avg_route.aggregate_fold_mode, sum_route.aggregate_fold_mode);
}

#[test]
fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 2,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
}

#[test]
fn route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan, kind,
            );

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_numeric_field_aggregate_fold_mode_contract_maps_sum_avg_to_existing_rows() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    for aggregate in [crate::db::sum("rank"), crate::db::avg("rank")] {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan, aggregate,
            );

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_streams_with_existing_rows() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_predicate_streams() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_uncertainty_materializes() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(7)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_distinct_offset_disables_bounded_probe_hints_for_terminals() {
    let mut plan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });

    for kind in [
        AggregateKind::Count,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan, kind,
            );

        assert_eq!(
            route_plan.scan_hints.physical_fetch_hint, None,
            "DISTINCT+offset must disable bounded aggregate hints for {kind:?}"
        );
        assert_eq!(
            route_plan.aggregate_seek_fetch_hint(),
            None,
            "DISTINCT+offset must disable secondary extrema probe hints for {kind:?}"
        );
    }
}

#[test]
fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7103),
            Ulid::from_u128(7101),
            Ulid::from_u128(7102),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(!route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::First { fetch: 3 }),
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);

    let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );
    assert_eq!(
        first_asc.aggregate_seek_fetch_hint(),
        None,
        "secondary extrema probe hints must stay route-owned and Min/Max-only"
    );

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::Last { fetch: 3 }),
    );
    assert_eq!(min_desc.aggregate_seek_spec(), None);
}

#[test]
fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3)
    );
}

#[test]
fn route_matrix_aggregate_count_pushdown_boundary_matrix() {
    let assert_count_route = |plan: &AccessPlannedQuery, expected_fold_mode: AggregateFoldMode| {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                plan,
                AggregateKind::Count,
            );
        assert_eq!(
            route_plan.execution_mode,
            RouteExecutionMode::Streaming,
            "COUNT pushdown matrix should stay on streaming execution mode",
        );
        assert_eq!(
            route_plan.aggregate_fold_mode, expected_fold_mode,
            "COUNT pushdown matrix should preserve fold-mode contract",
        );

        route_plan
    };

    let mut full_scan =
        AccessPlannedQuery::new_typed(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    full_scan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let _full_scan_route = assert_count_route(&full_scan, AggregateFoldMode::KeysOnly);

    let mut key_range = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::KeyRange {
            start: Ulid::from_u128(1),
            end: Ulid::from_u128(9),
        },
        MissingRowPolicy::Ignore,
    );
    key_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let _key_range_route = assert_count_route(&key_range, AggregateFoldMode::KeysOnly);

    let mut secondary = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    secondary.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let _secondary_route = assert_count_route(&secondary, AggregateFoldMode::ExistingRows);

    let mut index_range = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    index_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    index_range.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let index_range_route = assert_count_route(&index_range, AggregateFoldMode::ExistingRows);
    assert_eq!(
        index_range_route
            .index_range_limit_spec
            .map(|spec| spec.fetch),
        Some(3),
        "index-range COUNT with page window should inherit bounded pushdown fetch",
    );
}

#[test]
fn route_matrix_secondary_extrema_probe_eligibility_is_min_max_only() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );
    let exists_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Exists,
    );
    let last_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(first_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(exists_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(last_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::First { fetch: 3 }),
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);
    assert_eq!(first_asc.aggregate_seek_spec(), None);
    assert_eq!(exists_asc.aggregate_seek_spec(), None);
    assert_eq!(last_asc.aggregate_seek_spec(), None);

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_spec(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::Last { fetch: 3 }),
    );
}

#[test]
fn route_matrix_index_predicate_compile_mode_subset_vs_strict_boundary_is_explicit() {
    let mut plan = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let execution_preparation = ExecutionPreparation::from_plan(
        RouteMatrixEntity::MODEL,
        &plan,
        crate::db::executor::preparation::slot_map_for_model_plan(RouteMatrixEntity::MODEL, &plan),
    );
    let predicate_slots = execution_preparation
        .compiled_predicate()
        .expect("predicate slots should compile for mixed strict/residual predicate");
    let index_slots = execution_preparation
        .slot_map()
        .expect("index-range plan should expose one resolvable index slot");
    let subset_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::ConservativeSubset,
    );
    let strict_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::StrictAllOrNone,
    );

    assert!(
        subset_program.is_some(),
        "subset compile mode should keep the strict index-covered rank clause as a safe AND subset",
    );
    assert!(
        strict_program.is_none(),
        "strict compile mode must fail closed when any predicate child is not index-only-safe",
    );
}

#[test]
fn route_matrix_aggregate_strict_compile_uncertainty_forces_materialized_execution_mode() {
    let mut strict_compatible = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let strict_compatible_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_compatible,
            AggregateKind::Exists,
        );
    assert_eq!(
        strict_compatible_route.execution_mode,
        RouteExecutionMode::Streaming,
        "strict-compilable secondary predicate shapes should keep aggregate streaming eligibility",
    );

    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_uncertain,
            AggregateKind::Exists,
        );
    assert_eq!(
        strict_uncertain_route.execution_mode,
        RouteExecutionMode::Materialized,
        "aggregate route planning must force materialized execution when strict index compile fails",
    );

    let load_route = build_load_route_plan_for_entity::<RouteMatrixEntity>(
        &strict_uncertain,
        &initial_scalar_continuation_context(),
    )
    .expect("load route plan should build for strict/subset parity boundary shape");
    assert_eq!(
        load_route.execution_mode,
        RouteExecutionMode::Streaming,
        "load routing should remain streaming for the same shape via conservative subset policy",
    );
}

#[test]
fn route_matrix_strict_vs_subset_decision_logs_are_stable() {
    let mut strict_compatible = AccessPlannedQuery::new_typed(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let strict_compatible_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_compatible,
            AggregateKind::Exists,
        );
    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_uncertain,
            AggregateKind::Exists,
        );
    let load_route = build_load_route_plan_for_entity::<RouteMatrixEntity>(
        &strict_uncertain,
        &initial_scalar_continuation_context(),
    )
    .expect("load route plan should build for strict/subset log shape");

    let strict_compatible_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_compatible_route.execution_mode,
        strict_compatible_route.aggregate_fold_mode,
        strict_compatible_route.scan_hints.physical_fetch_hint,
        strict_compatible_route.aggregate_seek_fetch_hint(),
        strict_compatible_route.index_range_limit_fast_path_enabled(),
        strict_compatible_route.continuation().mode(),
    );
    let strict_uncertain_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_uncertain_route.execution_mode,
        strict_uncertain_route.aggregate_fold_mode,
        strict_uncertain_route.scan_hints.physical_fetch_hint,
        strict_uncertain_route.aggregate_seek_fetch_hint(),
        strict_uncertain_route.index_range_limit_fast_path_enabled(),
        strict_uncertain_route.continuation().mode(),
    );
    let load_log = format!(
        "load:mode={:?};fetch={:?};scan_budget={:?};index_range_limit={};continuation={:?}",
        load_route.execution_mode,
        load_route.scan_hints.physical_fetch_hint,
        load_route.scan_hints.load_scan_budget_hint,
        load_route.index_range_limit_fast_path_enabled(),
        load_route.continuation().mode(),
    );

    assert_eq!(
        strict_compatible_log,
        "aggregate:mode=Streaming;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=true;continuation=Initial",
        "strict-compilable aggregate route decision log should remain stable",
    );
    assert_eq!(
        strict_uncertain_log,
        "aggregate:mode=Materialized;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=false;continuation=Initial",
        "strict-uncertain aggregate route decision log should remain stable",
    );
    assert_eq!(
        load_log,
        "load:mode=Streaming;fetch=None;scan_budget=None;index_range_limit=true;continuation=Initial",
        "subset load route decision log should remain stable for the same shape",
    );
}
