use super::*;

fn additive_rank_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        crate::db::query::plan::expr::Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("rank"),
            )),
            right: Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("rank"),
            )),
        },
        direction,
    )
}

fn avg_rank_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        crate::db::query::plan::expr::Expr::Aggregate(crate::db::avg("rank")),
        direction,
    )
}

fn grouped_rank_plan_with_explicit_order(direction: OrderDirection) -> AccessPlannedQuery {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("rank"),
            ))),
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field("rank", direction)],
    });
    grouped
}

#[test]
fn route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

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
        route_plan.grouped_plan_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch)
    );
    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_keeps_blocking_shape_under_tight_budget_config() {
    let mut base = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let grouped = base.into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(1, 1),
    });
    let route_plan = build_grouped_route_plan(&grouped);

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
        route_plan.grouped_plan_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch)
    );
    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
}

#[test]
fn route_plan_grouped_wrapper_reports_prefix_mismatch_for_misaligned_grouped_order() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "label",
            OrderDirection::Asc,
        )],
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        route_plan.grouped_plan_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch)
    );
}

#[test]
fn route_plan_grouped_wrapper_reports_non_admissible_reason_for_computed_grouped_order() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![additive_rank_order_term(OrderDirection::Asc)],
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        route_plan.grouped_plan_fallback_reason(),
        Some(GroupedPlanFallbackReason::GroupKeyOrderExpressionNotAdmissible)
    );
}

#[test]
fn route_plan_grouped_wrapper_projects_top_k_group_strategy_for_aggregate_order() {
    let mut grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Avg,
                    input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                        crate::db::query::plan::expr::FieldId::new("rank"),
                    ))),
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![avg_rank_order_term(OrderDirection::Desc)],
    });

    assert_eq!(
        grouped_plan_strategy(&grouped).expect("grouped plan should project planner strategy"),
        GroupedPlanStrategy::top_k_group_with_aggregate_family(
            GroupedPlanAggregateFamily::FieldTargetRows,
        ),
        "aggregate grouped ORDER BY should reserve the Top-K planner lane before authority finalization widens grouped order validation",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::top_k_group_with_aggregate_family(
                GroupedPlanAggregateFamily::FieldTargetRows,
            ),
            GroupedExecutionModeContext::from_route_inputs(Direction::Desc, false, false),
        ),
        GroupedExecutionMode::HashMaterialized,
        "aggregate grouped ORDER BY should reserve the Top-K planner lane while route execution remains materialized hash-grouped until heap execution lands",
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
    assert!(
        route_plan
            .index_leaf_order_policy()
            .preserves_leaf_index_order(),
        "ordered grouped execution must preserve the physical index order that proves group contiguity",
    );
}

#[test]
fn route_plan_grouped_wrapper_uses_grouped_order_proof_for_explicit_canonical_direction() {
    for (direction, expected_direction) in [
        (OrderDirection::Asc, Direction::Asc),
        (OrderDirection::Desc, Direction::Desc),
    ] {
        let grouped = grouped_rank_plan_with_explicit_order(direction);
        let route_plan = build_grouped_route_plan(&grouped);

        assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
        assert_eq!(route_plan.direction(), expected_direction);
        assert_eq!(
            route_plan.load_order_route_reason(),
            LoadOrderRouteReason::None,
        );
        assert_eq!(
            grouped_execution_mode(&route_plan),
            GroupedExecutionMode::OrderedStreaming,
            "explicit canonical grouped ordering should consume the planner-owned grouped order proof",
        );
        assert!(
            route_plan
                .load_order_route_mode()
                .allows_ordered_group_projection(),
            "explicit canonical grouped ordering should not inherit scalar materialized-sort policy",
        );
    }
}

#[test]
fn route_plan_grouped_wrapper_rejects_mixed_group_key_directions() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_COMPOSITE_INDEX_MODEL,
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank", "label"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("label", OrderDirection::Desc),
        ],
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized,
    );
    let fallback_reason = route_plan
        .grouped_plan_fallback_reason()
        .expect("mixed group-key directions should retain a typed planner reason");
    assert_eq!(
        fallback_reason,
        GroupedPlanFallbackReason::GroupKeyOrderDirectionMismatch,
    );
    assert_eq!(fallback_reason.code(), "group_key_order_direction_mismatch",);
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_count_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("label"),
            ))),
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_sum_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("label"),
            ))),
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_avg_field_index_prefix_shape() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                crate::db::query::plan::expr::FieldId::new("label"),
            ))),
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
}

#[test]
fn route_plan_grouped_wrapper_preserves_ordered_strategy_for_fully_indexable_predicate_shape() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Nat64(7)],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Nat64(7)));
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
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
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
    assert_eq!(route_plan.grouped_plan_fallback_reason(), None);
    assert!(
        route_plan
            .index_leaf_order_policy()
            .preserves_leaf_index_order(),
        "ordered grouped range execution must preserve the physical index order that proves group contiguity",
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_when_residual_filter_predicate_exists() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Nat64(7)));
    let route_plan = build_grouped_route_plan(&grouped);

    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        route_plan.grouped_plan_fallback_reason(),
        Some(GroupedPlanFallbackReason::ResidualFilterBlocksGroupedOrder)
    );
}

#[test]
fn route_plan_grouped_wrapper_downgrades_ordered_strategy_for_non_streaming_having_expr() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having_expr(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(crate::db::query::plan::expr::Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(crate::db::query::plan::expr::Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(crate::db::query::plan::expr::Expr::Aggregate(
                    crate::db::count(),
                )),
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(1))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(5))),
        }),
    );
    let route_plan = build_grouped_route_plan(&grouped);
    let planner_strategy =
        grouped_plan_strategy(&grouped).expect("grouped plans should project strategy");

    assert_eq!(
        planner_strategy,
        GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
        "non-streaming grouped HAVING expressions should force the hash-group fallback",
    );
    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::HashMaterialized
    );
    assert_eq!(
        route_plan.grouped_plan_fallback_reason(),
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
                    input_expr: None,
                    filter_expr: None,
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
    assert_eq!(grouped_handoff.aggregate_specs().len(), kind_cases.len());
    for (index, expected_kind) in kind_cases.iter().enumerate() {
        assert_eq!(
            grouped_handoff.aggregate_specs()[index].kind(),
            *expected_kind
        );
        assert_eq!(
            grouped_handoff.aggregate_specs()[index].target_field(),
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
                input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                    crate::db::query::plan::expr::FieldId::new("rank"),
                ))),
                filter_expr: None,
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
    assert_eq!(grouped_handoff.aggregate_specs().len(), 1);
    assert_eq!(
        grouped_handoff.aggregate_specs()[0].kind(),
        AggregateKind::Max
    );
    assert_eq!(
        grouped_handoff.aggregate_specs()[0].target_field(),
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
                    input_expr: target_field.map(|field| {
                        Box::new(crate::db::query::plan::expr::Expr::Field(
                            crate::db::query::plan::expr::FieldId::new(field),
                        ))
                    }),
                    filter_expr: None,
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
    assert_eq!(grouped_handoff.aggregate_specs().len(), grouped_cases.len());
    for (index, (expected_kind, expected_target)) in grouped_cases.iter().enumerate() {
        let aggregate = &grouped_handoff.aggregate_specs()[index];
        assert_eq!(aggregate.kind(), *expected_kind);
        assert_eq!(aggregate.target_field(), *expected_target);
    }
}

#[test]
fn route_plan_grouped_wrapper_execution_contract_is_frozen() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        });
    let route_plan = build_grouped_route_plan(&grouped);
    let actual = (
        route_plan.execution_mode(),
        grouped_execution_mode(&route_plan),
        route_plan.grouped_plan_fallback_reason(),
    );
    let expected = (
        RouteExecutionMode::Materialized,
        GroupedExecutionMode::HashMaterialized,
        Some(GroupedPlanFallbackReason::GroupKeyOrderUnavailable),
    );

    assert_eq!(actual, expected);
}

#[test]
fn grouped_policy_snapshot_matrix_remains_consistent_across_planner_handoff_and_route() {
    let ordered_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: grouped_field_slots(&["rank"]),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    assert_eq!(
        grouped_policy_snapshot(&ordered_grouped),
        (
            GroupedPlanStrategy::ordered_group(),
            None,
            GroupedExecutionMode::OrderedStreaming,
        )
    );

    let having_rejected_grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having_expr(
        GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(crate::db::query::plan::expr::Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(crate::db::query::plan::expr::Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(crate::db::query::plan::expr::Expr::Aggregate(
                    crate::db::count(),
                )),
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(1))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(5))),
        }),
    );
    assert_eq!(
        grouped_policy_snapshot(&having_rejected_grouped),
        (
            GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
            None,
            GroupedExecutionMode::HashMaterialized,
        )
    );

    let mut scalar_distinct_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    input_expr: None,
                    filter_expr: None,
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
                        input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                            crate::db::query::plan::expr::FieldId::new("rank"),
                        ))),
                        filter_expr: None,
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
            ),
            "global DISTINCT grouped strategy snapshot should stay stable for {kind:?}",
        );
    }
}

#[test]
fn grouped_policy_snapshot_non_specialized_grouped_families_collapse_to_generic_rows() {
    let primary_key_terminal_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::First,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    assert_eq!(
        grouped_policy_snapshot(&primary_key_terminal_grouped),
        (
            GroupedPlanStrategy::hash_group_with_aggregate_family(
                GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
                GroupedPlanAggregateFamily::GenericRows,
            ),
            None,
            GroupedExecutionMode::HashMaterialized,
        ),
        "primary-key-value grouped aggregates should stay on the generic grouped rows family",
    );

    let mixed_grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: grouped_field_slots(&["rank"]),
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        input_expr: None,
                        filter_expr: None,
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: AggregateKind::Sum,
                        input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                            crate::db::query::plan::expr::FieldId::new("rank"),
                        ))),
                        filter_expr: None,
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
        ),
        "mixed grouped aggregate sets should collapse to the generic grouped rows family",
    );
}

#[test]
fn route_plan_grouped_wrapper_selects_ordered_group_strategy_for_mixed_count_and_sum_shapes() {
    let group_field = grouped_field_slot("rank");
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![group_field],
        aggregates: vec![
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            },
            GroupAggregateSpec {
                kind: AggregateKind::Sum,
                input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
                    crate::db::query::plan::expr::FieldId::new("rank"),
                ))),
                filter_expr: None,
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

    let route_plan = build_execution_route_plan(
        grouped_handoff.base(),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy: grouped_handoff.grouped_plan_strategy(),
        },
    )
    .expect("mixed grouped route test should build grouped route plan");
    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming,
        "mixed grouped count+sum shapes should keep the ordered grouped execution family when group-key order is proven",
    );
}

#[test]
fn route_plan_grouped_explain_projection_and_execution_contract_is_frozen() {
    let group_field = grouped_field_slot("rank");
    let group = GroupSpec {
        group_fields: vec![group_field.clone()],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(17, 8192),
    };
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having_expr(
        group.clone(),
        Some(aggregate_having_expr(
            &group,
            0,
            CompareOp::Gt,
            Value::Nat64(1),
        )),
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
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            having: Some(ExplainGroupHaving {
                expr: aggregate_having_expr(&group, 0, CompareOp::Gt, Value::Nat64(1)),
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
    let route_plan = build_execution_route_plan(
        grouped_handoff.base(),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy: grouped_handoff.grouped_plan_strategy(),
        },
    )
    .expect("grouped explain contract test should build grouped route plan");
    assert_eq!(
        route_plan.route_shape_kind(),
        RouteShapeKind::AggregateGrouped
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        route_plan.execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(
        grouped_execution_mode(&route_plan),
        GroupedExecutionMode::OrderedStreaming
    );
}

#[test]
fn grouped_execution_mode_context_is_stable() {
    let direct_facts = RouteCapabilityFacts {
        load_order_route_decision: LoadOrderRouteDecision::direct_streaming(),
        ordered_index_leaf_stream_eligible: false,
        pk_order_fast_path_eligible: false,
        count_pushdown_shape_supported: false,
        composite_aggregate_fast_path_eligible: false,
        residual_filter_present: false,
        bounded_probe_hint_safe: false,
        field_min_fast_path_eligible: false,
        field_max_fast_path_eligible: false,
        field_min_fast_path_ineligibility_reason: None,
        field_max_fast_path_ineligibility_reason: None,
    };
    let materialized_facts = RouteCapabilityFacts {
        load_order_route_decision: LoadOrderRouteDecision::materialized_fallback(
            LoadOrderRouteReason::RequiresMaterializedSort,
        ),
        ..direct_facts
    };
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group_with_aggregate_family(
                GroupedPlanAggregateFamily::GenericRows,
            ),
            GroupedExecutionModeContext::from_route_inputs(
                Direction::Asc,
                true,
                direct_facts
                    .load_order_route_mode()
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::OrderedStreaming,
        "ordered grouped planner strategy should stay ordered on direct compatible routes",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group(),
            GroupedExecutionModeContext::from_route_inputs(
                Direction::Desc,
                false,
                direct_facts
                    .load_order_route_mode()
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "descending ordered grouped routes should fail closed when reverse traversal is unavailable",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::ordered_group(),
            GroupedExecutionModeContext::from_route_inputs(
                Direction::Asc,
                true,
                materialized_facts
                    .load_order_route_mode()
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "ordered grouped routes should fail closed when route capability does not preserve ordered grouped projection",
    );
    assert_eq!(
        GroupedExecutionMode::from_planner_strategy(
            GroupedPlanStrategy::hash_group(GroupedPlanFallbackReason::HavingBlocksGroupedOrder),
            GroupedExecutionModeContext::from_route_inputs(
                Direction::Asc,
                true,
                direct_facts
                    .load_order_route_mode()
                    .allows_ordered_group_projection(),
            ),
        ),
        GroupedExecutionMode::HashMaterialized,
        "hash grouped planner strategies must not be reinterpreted as ordered grouped execution modes",
    );
}
