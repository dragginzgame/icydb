use super::*;
use crate::db::query::plan::expr::{BinaryOp, Expr, FieldId};

#[test]
fn explain_grouped_strategy_defaults_to_hash_group_for_full_scan_shapes() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
                    .expect("group field should resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("group_key_order_unavailable"),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_prefix_mismatch_for_misaligned_grouped_order() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "rank",
            OrderDirection::Asc,
        )],
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("group_key_order_prefix_mismatch"),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_non_admissible_reason_for_computed_grouped_order() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "tag + tag",
            OrderDirection::Asc,
        )],
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("group_key_order_expression_not_admissible"),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_top_k_group_for_aggregate_grouped_order() {
    let mut grouped =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                        .expect("group field should resolve"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Avg,
                    target_field: Some("rank".to_string()),
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });
    grouped.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "AVG(rank)",
            OrderDirection::Desc,
        )],
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "top_k_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_ordered_group_for_aligned_index_prefix_shapes() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_ordered_group_for_count_field_on_aligned_index_prefix_shapes() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("rank".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_ordered_group_for_sum_field_on_aligned_index_prefix_shapes() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("rank".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_ordered_group_for_avg_field_on_aligned_index_prefix_shapes() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("rank".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_preserves_ordered_group_for_fully_indexable_predicate_shapes() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq(
        "tag".to_string(),
        Value::Text("alpha".to_string()),
    ));

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_reports_ordered_group_for_order_only_index_range_shapes() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::index_range(
            PUSHDOWN_INDEX,
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        ),
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_downgrades_to_hash_for_residual_predicate_shapes() {
    let mut grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });
    grouped.scalar_plan_mut().predicate = Some(Predicate::eq(
        "tag".to_string(),
        Value::Text("alpha".to_string()),
    ));

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("residual_predicate_blocks_grouped_order",),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_downgrades_to_hash_for_non_streaming_having_expr() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having_expr(
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                    .expect("group field should resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Uint(1))),
            }),
            right: Box::new(Expr::Literal(Value::Uint(5))),
        }),
    );

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("having_blocks_grouped_order"),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_keeps_ordered_group_for_supported_having_operator() {
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
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
            Value::Uint(1),
        )),
    );

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_having_projection_is_reported() {
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having_expr(
            group.clone(),
            Some(aggregate_having_expr(
                &group,
                0,
                CompareOp::Gt,
                Value::Uint(1),
            )),
        );

    assert!(matches!(
        grouped.explain().grouping(),
        ExplainGrouping::Grouped {
            having: Some(_),
            ..
        }
    ));
}

#[test]
fn explain_grouped_distinct_aggregate_projection_is_reported() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
                    .expect("group field should resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });

    match grouped.explain().grouping() {
        ExplainGrouping::Grouped { aggregates, .. } => {
            assert_eq!(aggregates.len(), 1, "one grouped aggregate should project");
            assert!(
                aggregates[0].distinct,
                "grouped explain projection should include aggregate distinct modifier"
            );
        }
        ExplainGrouping::None => panic!("grouped explain must project grouped payload"),
    }
}

#[test]
fn explain_grouped_ordered_having_projection_shape_is_frozen() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
        .expect("group field should resolve");
    let group = GroupSpec {
        group_fields: vec![group_field.clone()],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
    };
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
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
            Value::Uint(1),
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
                expr: aggregate_having_expr(&group, 0, CompareOp::Gt, Value::Uint(1)),
            }),
            max_groups: 12,
            max_group_bytes: 4096,
        },
        "ordered grouped HAVING explain projection must remain stable",
    );
}

#[test]
fn explain_grouped_having_expression_projection_is_reported() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
        .expect("group field should resolve");
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having_expr(
        GroupSpec {
            group_fields: vec![group_field.clone()],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
        },
        Some(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Uint(1))),
            }),
            right: Box::new(Expr::Literal(Value::Uint(5))),
        }),
    );

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("having_blocks_grouped_order"),
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
                expr: Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Gt,
                    left: Box::new(Expr::Binary {
                        op: crate::db::query::plan::expr::BinaryOp::Add,
                        left: Box::new(Expr::Aggregate(crate::db::count())),
                        right: Box::new(Expr::Literal(Value::Uint(1))),
                    }),
                    right: Box::new(Expr::Literal(Value::Uint(5))),
                },
            }),
            max_groups: 12,
            max_group_bytes: 4096,
        },
        "widened grouped HAVING explain projection must preserve the post-aggregate expression tree",
    );
}

#[test]
fn explain_grouped_having_case_expression_projection_is_reported() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having_expr(
            GroupSpec {
                group_fields: vec![],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
            },
            Some(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Gt,
                left: Box::new(Expr::Case {
                    when_then_arms: vec![crate::db::query::plan::expr::CaseWhenArm::new(
                        Expr::Unary {
                            op: crate::db::query::plan::expr::UnaryOp::Not,
                            expr: Box::new(Expr::Literal(Value::Bool(false))),
                        },
                        Expr::Aggregate(crate::db::count()),
                    )],
                    else_expr: Box::new(Expr::Literal(Value::Uint(0))),
                }),
                right: Box::new(Expr::Literal(Value::Uint(5))),
            }),
        );

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("group_key_order_unavailable"),
            group_fields: vec![],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            having: Some(ExplainGroupHaving {
                expr: Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Gt,
                    left: Box::new(Expr::Case {
                        when_then_arms: vec![crate::db::query::plan::expr::CaseWhenArm::new(
                            Expr::Unary {
                                op: crate::db::query::plan::expr::UnaryOp::Not,
                                expr: Box::new(Expr::Literal(Value::Bool(false))),
                            },
                            Expr::Aggregate(crate::db::count()),
                        )],
                        else_expr: Box::new(Expr::Literal(Value::Uint(0))),
                    }),
                    right: Box::new(Expr::Literal(Value::Uint(5))),
                },
            }),
            max_groups: 12,
            max_group_bytes: 4096,
        },
        "grouped HAVING explain projection must preserve searched CASE and unary conditions on the shared post-aggregate expression seam",
    );
}

#[test]
fn explain_grouped_aggregate_input_expression_projection_is_reported() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
        .expect("group field should resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field.clone()],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Avg,
                target_field: None,
                input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(crate::db::query::plan::expr::Expr::Field(
                        crate::db::query::plan::expr::FieldId::new("rank"),
                    )),
                    right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Int(1))),
                })),
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
        });

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("aggregate_streaming_not_supported"),
            group_fields: vec![ExplainGroupField {
                slot_index: group_field.index(),
                field: group_field.field().to_string(),
            }],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Avg,
                target_field: None,
                input_expr: Some("rank + 1".to_string()),
                filter_expr: None,
                distinct: false,
            }],
            having: None,
            max_groups: 12,
            max_group_bytes: 4096,
        },
        "grouped explain projection must expose widened aggregate input expressions explicitly",
    );
}

#[test]
fn explain_grouped_hash_distinct_projection_shape_is_frozen() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
        .expect("group field should resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field.clone()],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(25, 16_384),
        });

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("aggregate_streaming_not_supported"),
            group_fields: vec![ExplainGroupField {
                slot_index: group_field.index(),
                field: group_field.field().to_string(),
            }],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            having: None,
            max_groups: 25,
            max_group_bytes: 16_384,
        },
        "hash grouped DISTINCT explain projection must remain stable",
    );
}

fn grouped_explain_plan_snapshot(explain: &ExplainPlan) -> String {
    explain.render_text_canonical()
}

fn grouped_explain_plan_json_snapshot(explain: &ExplainPlan) -> String {
    explain.render_json_canonical()
}

#[test]
fn explain_grouped_plan_snapshot_for_ordered_having_shape_is_stable() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
        .expect("group field should resolve");
    let group = GroupSpec {
        group_fields: vec![group_field],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
    };
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
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
            Value::Uint(1),
        )),
    );

    let actual = grouped_explain_plan_snapshot(&grouped.explain());
    let expected = "mode=Load(LoadSpec { limit: None, offset: 0 })
access=IndexPrefix { name: \"explain::pushdown_tag\", fields: [\"tag\"], prefix_len: 0, values: [] }
predicate=None
order_by=None
distinct=false
grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None, group_fields: [ExplainGroupField { slot_index: 1, field: \"tag\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, input_expr: None, filter_expr: None, distinct: false }], having: Some(ExplainGroupHaving { expr: Binary { op: Gt, left: Aggregate(AggregateExpr { kind: Count, input_expr: None, filter_expr: None, distinct: false }), right: Literal(Uint(1)) } }), max_groups: 12, max_group_bytes: 4096 }
order_pushdown=MissingModelContext
page=None
delete_limit=None
consistency=Ignore";

    assert_eq!(
        actual, expected,
        "ordered-grouped explain-plan snapshot drifted",
    );
}

#[test]
fn explain_plan_canonical_json_snapshot_for_simple_shape_is_stable() {
    let plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    let actual = grouped_explain_plan_json_snapshot(&plan.explain());
    let expected = "{\"mode\":{\"type\":\"Load\",\"limit\":null,\"offset\":0},\"access\":{\"type\":\"FullScan\"},\"predicate\":\"None\",\"order_by\":\"None\",\"distinct\":false,\"grouping\":\"None\",\"order_pushdown\":\"MissingModelContext\",\"page\":{\"type\":\"None\"},\"delete_limit\":{\"type\":\"None\"},\"consistency\":\"Ignore\"}";

    assert_eq!(
        actual, expected,
        "canonical logical explain JSON snapshot drifted",
    );
}

#[test]
fn explain_grouped_plan_snapshot_for_hash_distinct_shape_is_stable() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
        .expect("group field should resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(25, 16_384),
        });

    let actual = grouped_explain_plan_snapshot(&grouped.explain());
    let expected = "mode=Load(LoadSpec { limit: None, offset: 0 })
access=FullScan
predicate=None
order_by=None
distinct=false
grouping=Grouped { strategy: \"hash_group\", fallback_reason: Some(\"aggregate_streaming_not_supported\"), group_fields: [ExplainGroupField { slot_index: 2, field: \"rank\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, input_expr: None, filter_expr: None, distinct: true }], having: None, max_groups: 25, max_group_bytes: 16384 }
order_pushdown=MissingModelContext
page=None
delete_limit=None
consistency=Ignore";

    assert_eq!(
        actual, expected,
        "hash-grouped explain-plan snapshot drifted",
    );
}

#[test]
fn explain_grouped_plan_snapshot_for_filtered_shape_is_stable() {
    let group_field = FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
        .expect("group field should resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: Some(Box::new(Expr::Binary {
                    op: BinaryOp::Gte,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Uint(10))),
                })),
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
        });

    let actual = grouped_explain_plan_snapshot(&grouped.explain());
    let expected = "mode=Load(LoadSpec { limit: None, offset: 0 })
access=FullScan
predicate=None
order_by=None
distinct=false
grouping=Grouped { strategy: \"hash_group\", fallback_reason: Some(\"group_key_order_unavailable\"), group_fields: [ExplainGroupField { slot_index: 1, field: \"tag\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, input_expr: None, filter_expr: Some(\"rank >= 10\"), distinct: false }], having: None, max_groups: 12, max_group_bytes: 4096 }
order_pushdown=MissingModelContext
page=None
delete_limit=None
consistency=Ignore";

    assert_eq!(
        actual, expected,
        "filtered grouped explain-plan snapshot drifted",
    );
}

#[test]
fn explain_global_distinct_sum_projection_is_reported() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: Vec::new(),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Sum,
                target_field: Some("rank".to_string()),
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
        });

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: "hash_group",
            fallback_reason: Some("aggregate_streaming_not_supported",),
            group_fields: Vec::new(),
            aggregates: vec![crate::db::query::explain::ExplainGroupAggregate {
                kind: AggregateKind::Sum,
                target_field: Some("rank".to_string()),
                input_expr: None,
                filter_expr: None,
                distinct: true,
            }],
            having: None,
            max_groups: 1,
            max_group_bytes: 1024,
        },
        "global DISTINCT SUM should project explicit grouped explain payload with zero group keys",
    );
}
