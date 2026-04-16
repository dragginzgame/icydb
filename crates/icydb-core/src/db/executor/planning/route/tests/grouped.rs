use super::*;

#[test]
fn route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        route_plan.route_shape_kind(),
        RouteShapeKind::AggregateGrouped
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert_eq!(
        grouped_observability.planner_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderUnavailable)
    );
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_keeps_blocking_shape_under_tight_budget_config() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        route_plan.route_shape_kind(),
        RouteShapeKind::AggregateGrouped
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
    assert_eq!(grouped_observability.rejection_reason(), None);
    assert_eq!(
        grouped_observability.planner_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderUnavailable)
    );
    assert!(grouped_observability.eligible());
    assert_eq!(
        grouped_observability.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_count_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("label".to_string()),
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_sum_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("label".to_string()),
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_avg_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("label".to_string()),
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_preserves_ordered_strategy_for_fully_indexable_predicate_shape() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_index_range_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        ),
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
    assert_eq!(grouped_observability.planner_fallback_reason(), None);
    assert_eq!(
        grouped_observability.outcome(),
        GroupedRouteDecisionOutcome::MaterializedFallback
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_when_residual_predicate_exists() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");

    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        grouped_observability.planner_fallback_reason(),
        Some(GroupedPlanFallbackReason::ResidualPredicateBlocksGroupedOrder)
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_for_unsupported_having_operator() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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
    let route_plan = build_grouped_route_plan(&grouped);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped route should project grouped observability payload");
    let planner_strategy =
        grouped_plan_strategy(&grouped).expect("grouped plans should project strategy");

    assert_eq!(
        planner_strategy,
        GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
        "unsupported grouped HAVING operators should be planner-policy rejected from ordered-group strategy",
    );
    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        grouped_observability.planner_fallback_reason(),
        Some(GroupedPlanFallbackReason::HavingBlocksGroupedOrder)
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
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
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
    let finalized = finalized_plan_for_authority(route_capability_authority(), &grouped);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 1);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(
        grouped_handoff.aggregate_projection_specs().len(),
        kind_cases.len()
    );
    for (index, expected_kind) in kind_cases.iter().enumerate() {
        assert_eq!(
            grouped_handoff.aggregate_projection_specs()[index].kind(),
            *expected_kind
        );
        assert_eq!(
            grouped_handoff.aggregate_projection_specs()[index].target_field(),
            None
        );
    }
}

#[test]
fn route_plan_grouped_wrapper_preserves_target_field_in_query_handoff() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank", "label"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Max,
                target_field: Some("rank".to_string()),
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });
    let finalized = finalized_plan_for_authority(route_capability_authority(), &grouped);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(grouped_handoff.aggregate_projection_specs().len(), 1);
    assert_eq!(
        grouped_handoff.aggregate_projection_specs()[0].kind(),
        AggregateKind::Max
    );
    assert_eq!(
        grouped_handoff.aggregate_projection_specs()[0].target_field(),
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
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
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
    let finalized = finalized_plan_for_authority(route_capability_authority(), &grouped);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(grouped_handoff.group_fields().len(), 2);
    assert_eq!(grouped_handoff.group_fields()[0].field(), "rank");
    assert_eq!(grouped_handoff.group_fields()[1].field(), "label");
    assert_eq!(
        grouped_handoff.aggregate_projection_specs().len(),
        grouped_cases.len()
    );
    for (index, (expected_kind, expected_target)) in grouped_cases.iter().enumerate() {
        let aggregate = &grouped_handoff.aggregate_projection_specs()[index];
        assert_eq!(aggregate.kind(), *expected_kind);
        assert_eq!(aggregate.target_field(), *expected_target);
    }
}

#[test]
fn route_plan_grouped_wrapper_observability_vector_is_frozen() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        });
    let route_plan = build_grouped_route_plan(&grouped);
    let observability = route_plan
        .grouped_observability()
        .expect("grouped route should always project grouped observability for grouped intents");
    let actual = (
        observability.outcome(),
        observability.rejection_reason(),
        observability.eligible(),
        observability.execution_mode(),
        observability.grouped_execution_mode(),
    );
    let expected = (
        GroupedRouteDecisionOutcome::MaterializedFallback,
        None,
        true,
        RouteExecutionMode::Materialized,
        GroupedExecutionMode::HashMaterialized,
    );

    assert_eq!(actual, expected);
}

#[test]
fn grouped_policy_snapshot_matrix_remains_consistent_across_planner_handoff_and_route() {
    let ordered_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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
            GroupedPlanStrategy::ordered_group(),
            None,
            GroupedExecutionMode::OrderedMaterialized,
            true,
        )
    );

    let having_rejected_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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
            GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
            None,
            GroupedExecutionMode::HashMaterialized,
            true,
        )
    );

    let mut scalar_distinct_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
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
            GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::DistinctGroupingNotAdmitted),
            Some(GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired),
            GroupedExecutionMode::HashMaterialized,
            true,
        )
    );
}

#[test]
fn grouped_policy_snapshot_global_distinct_field_target_kind_matrix_includes_avg() {
    for kind in [AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg] {
        let grouped =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
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
                GroupedPlanStrategy::hash_group_with_aggregate_family(
                    GroupedPlanFallbackReason::AggregateStreamingNotSupported,
                    GroupedPlanAggregateFamily::FieldTargetRows,
                ),
                None,
                GroupedExecutionMode::HashMaterialized,
                true,
            ),
            "global DISTINCT grouped strategy snapshot should stay stable for {kind:?}",
        );
    }
}

#[test]
fn grouped_policy_snapshot_non_specialized_grouped_families_collapse_to_generic_rows() {
    let storage_key_terminal_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::First,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    assert_eq!(
        grouped_policy_snapshot(&storage_key_terminal_grouped),
        (
            GroupedPlanStrategy::hash_group_with_aggregate_family(
                GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
                GroupedPlanAggregateFamily::GenericRows,
            ),
            None,
            GroupedExecutionMode::HashMaterialized,
            true,
        ),
        "storage-key grouped aggregates should stay on the generic grouped rows family",
    );

    let mixed_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: AggregateKind::Sum,
                        target_field: Some("rank".to_string()),
                        distinct: false,
                    },
                ],
                execution: GroupedExecutionConfig::unbounded(),
            });
    assert_eq!(
        grouped_policy_snapshot(&mixed_grouped),
        (
            GroupedPlanStrategy::hash_group_with_aggregate_family(
                GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
                GroupedPlanAggregateFamily::GenericRows,
            ),
            None,
            GroupedExecutionMode::HashMaterialized,
            true,
        ),
        "mixed grouped aggregate sets should collapse to the generic grouped rows family",
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_mixed_count_and_sum_shapes() {
    let group_field = grouped_field_slot("rank");
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![group_field],
        aggregates: vec![
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            },
            GroupAggregateSpec {
                kind: AggregateKind::Sum,
                target_field: Some("rank".to_string()),
                distinct: false,
            },
        ],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let finalized = finalized_plan_for_authority(route_capability_authority(), &grouped);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("mixed grouped plans should build handoff");
    assert_eq!(
        grouped_handoff.grouped_plan_strategy(),
        GroupedPlanStrategy::ordered_group_with_aggregate_family(
            GroupedPlanAggregateFamily::GenericRows,
        ),
        "mixed grouped count+sum shapes should stay on the generic grouped family without losing ordered-group admission",
    );

    let route_plan = build_execution_route_plan_for_grouped_plan(
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("mixed grouped route should always project grouped observability");
    assert_eq!(
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized,
        "mixed grouped count+sum shapes should keep the ordered grouped execution family when group-key order is proven",
    );
}

#[test]
fn route_plan_grouped_explain_projection_and_execution_contract_is_frozen() {
    let group_field = grouped_field_slot("rank");
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: ROUTE_CAPABILITY_INDEX_MODELS[0],
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

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
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
                expr: ExplainGroupHavingExpr::Compare {
                    left: ExplainGroupHavingValueExpr::AggregateIndex { index: 0 },
                    op: CompareOp::Gt,
                    right: ExplainGroupHavingValueExpr::Literal(Value::Uint(1)),
                },
            }),
            max_groups: 17,
            max_group_bytes: 8192,
        },
        "grouped explain projection must preserve strategy, fields, aggregates, having, and hard limits",
    );

    let finalized = finalized_plan_for_authority(route_capability_authority(), &grouped);
    let grouped_handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(grouped_handoff.execution().max_groups(), 17);
    assert_eq!(grouped_handoff.execution().max_group_bytes(), 8192);
    let route_plan = build_execution_route_plan_for_grouped_plan(
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy(),
    );
    assert_eq!(
        route_plan.route_shape_kind(),
        RouteShapeKind::AggregateGrouped
    );
    assert_eq!(
        route_plan.execution_mode(),
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
        grouped_observability.grouped_execution_mode(),
        GroupedExecutionMode::OrderedMaterialized
    );
}

#[test]
fn grouped_execution_mode_projection_contract_is_stable() {
    let direct_caps = RouteCapabilities {
        load_order_route_contract: LoadOrderRouteContract::DirectStreaming,
        load_order_route_reason: LoadOrderRouteReason::None,
        pk_order_fast_path_eligible: false,
        count_pushdown_shape_supported: false,
        composite_aggregate_fast_path_eligible: false,
        bounded_probe_hint_safe: false,
        field_min_fast_path_eligible: false,
        field_max_fast_path_eligible: false,
        field_min_fast_path_ineligibility_reason: None,
        field_max_fast_path_ineligibility_reason: None,
    };
    let materialized_caps = RouteCapabilities {
        load_order_route_contract: LoadOrderRouteContract::MaterializedFallback,
        load_order_route_reason: LoadOrderRouteReason::RequiresMaterializedSort,
        ..direct_caps
    };
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group(),
            GroupedExecutionModeProjection::from_route_inputs(
                Direction::Asc,
                true,
                direct_caps
                    .load_order_route_contract
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::OrderedMaterialized,
        "ordered grouped planner strategy should stay ordered on direct compatible routes",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group(),
            GroupedExecutionModeProjection::from_route_inputs(
                Direction::Desc,
                false,
                direct_caps
                    .load_order_route_contract
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "descending ordered grouped routes should fail closed when reverse traversal is unavailable",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group(),
            GroupedExecutionModeProjection::from_route_inputs(
                Direction::Asc,
                true,
                materialized_caps
                    .load_order_route_contract
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "ordered grouped routes should fail closed when route capability does not preserve ordered grouped projection",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
            GroupedExecutionModeProjection::from_route_inputs(
                Direction::Asc,
                true,
                direct_caps
                    .load_order_route_contract
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "hash grouped planner strategies must not be reinterpreted as ordered grouped execution modes",
    );
}
