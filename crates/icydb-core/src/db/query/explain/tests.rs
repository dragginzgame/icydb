//! Module: db::query::explain::tests
//! Responsibility: module-local ownership and contracts for db::query::explain::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::access::{
    AccessPath, AccessPlan, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
};
use crate::db::predicate::{CompareOp, MissingRowPolicy, Predicate, normalize};
use crate::db::query::builder::field::FieldRef;
use crate::db::query::intent::{KeyAccess, build_access_plan_from_keys};
use crate::db::query::plan::{
    AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause,
    GroupHavingSpec, GroupHavingSymbol, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan,
    OrderDirection, OrderSpec, QueryMode,
};
use crate::model::{field::FieldKind, index::IndexModel};
use crate::traits::EntitySchema;
use crate::types::Ulid;
use crate::value::Value;
use std::ops::Bound;

const PUSHDOWN_INDEX_FIELDS: [&str; 1] = ["tag"];
const PUSHDOWN_INDEX: IndexModel = IndexModel::new(
    "explain::pushdown_tag",
    "explain::pushdown_store",
    &PUSHDOWN_INDEX_FIELDS,
    false,
);

crate::test_entity! {
ident = ExplainPushdownEntity,
    id = Ulid,
    entity_name = "PushdownEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&PUSHDOWN_INDEX],
}

#[test]
fn explain_is_deterministic_for_same_query() {
    let predicate = FieldRef::new("id").eq(Ulid::default());
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().predicate = Some(predicate);

    assert_eq!(plan.explain(), plan.explain());
}

#[test]
fn explain_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = normalize(&Predicate::And(vec![
        FieldRef::new("id").eq(id),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]));
    let predicate_b = normalize(&Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        FieldRef::new("id").eq(id),
    ]));

    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = build_access_plan_from_keys(&KeyAccess::Many(vec![a, b, a]));
    let access_b = build_access_plan_from_keys(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };
    let plan_b: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_reports_deterministic_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel =
        IndexModel::new("explain::idx_a", "explain::store", &INDEX_FIELDS, false);
    const INDEX_B: IndexModel =
        IndexModel::new("explain::idx_a_alt", "explain::store", &INDEX_FIELDS, false);

    let mut indexes = [INDEX_B, INDEX_A];
    indexes.sort_by(|left, right| left.name().cmp(right.name()));
    let chosen = indexes[0];

    let plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: chosen,
            values: vec![Value::Text("alpha".to_string())],
        },
        crate::db::predicate::MissingRowPolicy::Ignore,
    );

    let explain = plan.explain();
    match explain.access() {
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            ..
        } => {
            assert_eq!(*name, "explain::idx_a");
            assert_eq!(fields.as_slice(), vec!["idx_a"]);
            assert_eq!(*prefix_len, 1);
        }
        _ => panic!("expected index prefix"),
    }
}

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
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::HashGroup,
            fallback_reason: Some(ExplainGroupedFallbackReason::GroupKeyOrderUnavailable),
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
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    });

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
            strategy: ExplainGroupedStrategy::HashGroup,
            fallback_reason: Some(
                ExplainGroupedFallbackReason::ResidualPredicateBlocksGroupedOrder,
            ),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_downgrades_to_hash_for_unsupported_having_operator() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                    .expect("group field should resolve"),
            ],
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

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::HashGroup,
            fallback_reason: Some(ExplainGroupedFallbackReason::HavingBlocksGroupedOrder),
            ..
        }
    ));
}

#[test]
fn explain_grouped_strategy_keeps_ordered_group_for_supported_having_operator() {
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "tag")
                    .expect("group field should resolve"),
            ],
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
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    let explain = grouped.explain();
    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::OrderedGroup,
            fallback_reason: None,
            ..
        }
    ));
}

#[test]
fn explain_grouped_having_projection_is_reported() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having(
            GroupSpec {
                group_fields: vec![
                    FieldSlot::resolve(<ExplainPushdownEntity as EntitySchema>::MODEL, "rank")
                        .expect("group field should resolve"),
                ],
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
                    op: CompareOp::Gt,
                    value: Value::Uint(1),
                }],
            }),
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
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
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
            execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
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
            strategy: ExplainGroupedStrategy::OrderedGroup,
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
                clauses: vec![ExplainGroupHavingClause {
                    symbol: ExplainGroupHavingSymbol::AggregateIndex { index: 0 },
                    op: CompareOp::Gt,
                    value: Value::Uint(1),
                }],
            }),
            max_groups: 12,
            max_group_bytes: 4096,
        },
        "ordered grouped HAVING explain projection must remain stable",
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
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(25, 16_384),
        });

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: ExplainGroupedStrategy::HashGroup,
            fallback_reason: Some(ExplainGroupedFallbackReason::AggregateStreamingNotSupported),
            group_fields: vec![ExplainGroupField {
                slot_index: group_field.index(),
                field: group_field.field().to_string(),
            }],
            aggregates: vec![ExplainGroupAggregate {
                kind: AggregateKind::Count,
                target_field: None,
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
    let grouped = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped_with_having(
        GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
        },
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    let actual = grouped_explain_plan_snapshot(&grouped.explain());
    let expected = "mode=Load(LoadSpec { limit: None, offset: 0 })
access=IndexPrefix { name: \"explain::pushdown_tag\", fields: [\"tag\"], prefix_len: 0, values: [] }
predicate=None
order_by=None
distinct=false
grouping=Grouped { strategy: OrderedGroup, fallback_reason: None, group_fields: [ExplainGroupField { slot_index: 1, field: \"tag\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, distinct: false }], having: Some(ExplainGroupHaving { clauses: [ExplainGroupHavingClause { symbol: AggregateIndex { index: 0 }, op: Gt, value: Uint(1) }] }), max_groups: 12, max_group_bytes: 4096 }
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
grouping=Grouped { strategy: HashGroup, fallback_reason: Some(AggregateStreamingNotSupported), group_fields: [ExplainGroupField { slot_index: 2, field: \"rank\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, distinct: true }], having: None, max_groups: 25, max_group_bytes: 16384 }
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
fn explain_global_distinct_sum_projection_is_reported() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: Vec::new(),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Sum,
                target_field: Some("rank".to_string()),
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
        });

    assert_eq!(
        grouped.explain().grouping(),
        &ExplainGrouping::Grouped {
            strategy: crate::db::query::explain::ExplainGroupedStrategy::HashGroup,
            fallback_reason: Some(
                crate::db::query::explain::ExplainGroupedFallbackReason::AggregateStreamingNotSupported,
            ),
            group_fields: Vec::new(),
            aggregates: vec![crate::db::query::explain::ExplainGroupAggregate {
                kind: AggregateKind::Sum,
                target_field: Some("rank".to_string()),
                distinct: true,
            }],
            having: None,
            max_groups: 1,
            max_group_bytes: 1024,
        },
        "global DISTINCT SUM should project explicit grouped explain payload with zero group keys",
    );
}

#[test]
fn explain_differs_for_semantic_changes() {
    let plan_a: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))),
        MissingRowPolicy::Ignore,
    );
    let plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert_ne!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_with_model_does_not_evaluate_order_pushdown() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_with_model_does_not_evaluate_descending_pushdown() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_with_model_does_not_evaluate_composite_pushdown_rejections() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::index_range(
                PUSHDOWN_INDEX,
                vec![],
                Bound::Included(Value::Text("alpha".to_string())),
                Bound::Excluded(Value::Text("omega".to_string())),
            )),
            AccessPlan::path(AccessPath::FullScan),
        ]),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_without_model_reports_missing_model_context() {
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn explain_pushdown_conversion_covers_all_variants() {
    let cases = vec![
        (
            SecondaryOrderPushdownEligibility::Eligible {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
            ExplainOrderPushdown::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
        ),
    ];

    for (input, expected) in cases {
        assert_eq!(ExplainOrderPushdown::from(input), expected);
    }
}

#[test]
fn explain_execution_node_type_vocabulary_is_frozen() {
    let actual = [
        ExplainExecutionNodeType::ByKeyLookup.as_str(),
        ExplainExecutionNodeType::ByKeysLookup.as_str(),
        ExplainExecutionNodeType::PrimaryKeyRangeScan.as_str(),
        ExplainExecutionNodeType::IndexPrefixScan.as_str(),
        ExplainExecutionNodeType::IndexRangeScan.as_str(),
        ExplainExecutionNodeType::IndexMultiLookup.as_str(),
        ExplainExecutionNodeType::FullScan.as_str(),
        ExplainExecutionNodeType::Union.as_str(),
        ExplainExecutionNodeType::Intersection.as_str(),
        ExplainExecutionNodeType::IndexPredicatePrefilter.as_str(),
        ExplainExecutionNodeType::ResidualPredicateFilter.as_str(),
        ExplainExecutionNodeType::OrderByAccessSatisfied.as_str(),
        ExplainExecutionNodeType::OrderByMaterializedSort.as_str(),
        ExplainExecutionNodeType::DistinctPreOrdered.as_str(),
        ExplainExecutionNodeType::DistinctMaterialized.as_str(),
        ExplainExecutionNodeType::ProjectionMaterialized.as_str(),
        ExplainExecutionNodeType::CoveringRead.as_str(),
        ExplainExecutionNodeType::LimitOffset.as_str(),
        ExplainExecutionNodeType::CursorResume.as_str(),
        ExplainExecutionNodeType::IndexRangeLimitPushdown.as_str(),
        ExplainExecutionNodeType::TopNSeek.as_str(),
        ExplainExecutionNodeType::AggregateCount.as_str(),
        ExplainExecutionNodeType::AggregateExists.as_str(),
        ExplainExecutionNodeType::AggregateMin.as_str(),
        ExplainExecutionNodeType::AggregateMax.as_str(),
        ExplainExecutionNodeType::AggregateFirst.as_str(),
        ExplainExecutionNodeType::AggregateLast.as_str(),
        ExplainExecutionNodeType::AggregateSum.as_str(),
        ExplainExecutionNodeType::AggregateSeekFirst.as_str(),
        ExplainExecutionNodeType::AggregateSeekLast.as_str(),
        ExplainExecutionNodeType::GroupedAggregateHashMaterialized.as_str(),
        ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized.as_str(),
        ExplainExecutionNodeType::SecondaryOrderPushdown.as_str(),
    ];
    let expected = [
        "ByKeyLookup",
        "ByKeysLookup",
        "PrimaryKeyRangeScan",
        "IndexPrefixScan",
        "IndexRangeScan",
        "IndexMultiLookup",
        "FullScan",
        "Union",
        "Intersection",
        "IndexPredicatePrefilter",
        "ResidualPredicateFilter",
        "OrderByAccessSatisfied",
        "OrderByMaterializedSort",
        "DistinctPreOrdered",
        "DistinctMaterialized",
        "ProjectionMaterialized",
        "CoveringRead",
        "LimitOffset",
        "CursorResume",
        "IndexRangeLimitPushdown",
        "TopNSeek",
        "AggregateCount",
        "AggregateExists",
        "AggregateMin",
        "AggregateMax",
        "AggregateFirst",
        "AggregateLast",
        "AggregateSum",
        "AggregateSeekFirst",
        "AggregateSeekLast",
        "GroupedAggregateHashMaterialized",
        "GroupedAggregateOrderedMaterialized",
        "SecondaryOrderPushdown",
    ];

    assert_eq!(
        actual, expected,
        "execution-node vocabulary drifted; node names are a stable EXPLAIN contract",
    );
}

#[test]
fn execution_descriptor_verbose_text_renders_all_optional_fields() {
    let mut node_properties = ExplainPropertyMap::new();
    node_properties.insert("fetch", Value::from(7_u64));
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::TopNSeek,
        execution_mode: ExplainExecutionMode::Streaming,
        access_strategy: Some(ExplainAccessPath::FullScan),
        predicate_pushdown: Some("strict_all_or_none".to_string()),
        residual_predicate: Some(ExplainPredicate::IsNull {
            field: "rank".to_string(),
        }),
        projection: Some("index_only".to_string()),
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(3),
        cursor: Some(false),
        covering_scan: Some(true),
        rows_expected: Some(3),
        children: Vec::new(),
        node_properties,
    };

    let verbose = descriptor.render_text_tree_verbose();
    assert!(
        verbose.contains("TopNSeek execution_mode=Streaming"),
        "verbose execution text should render root node heading",
    );
    assert!(
        verbose.contains("access_strategy=FullScan"),
        "verbose execution text should render access strategy details",
    );
    assert!(
        verbose.contains("predicate_pushdown=strict_all_or_none"),
        "verbose execution text should render predicate pushdown details",
    );
    assert!(
        verbose.contains("node_properties=fetch="),
        "verbose execution text should render node properties",
    );
}

#[test]
fn execution_descriptor_canonical_json_shape_is_stable() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::TopNSeek,
        execution_mode: ExplainExecutionMode::Streaming,
        access_strategy: Some(ExplainAccessPath::FullScan),
        predicate_pushdown: None,
        residual_predicate: None,
        projection: Some("index_only".to_string()),
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(3),
        cursor: Some(false),
        covering_scan: Some(true),
        rows_expected: Some(3),
        children: vec![ExplainExecutionNodeDescriptor {
            node_type: ExplainExecutionNodeType::LimitOffset,
            execution_mode: ExplainExecutionMode::Materialized,
            access_strategy: None,
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: None,
            limit: Some(1),
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children: Vec::new(),
            node_properties: ExplainPropertyMap::new(),
        }],
        node_properties: ExplainPropertyMap::new(),
    };

    let json = descriptor.render_json_canonical();
    let expected = "{\"node_id\":0,\"node_type\":\"TopNSeek\",\"layer\":\"pipeline\",\"execution_mode\":\"Streaming\",\"execution_mode_detail\":\"streaming\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":\"index_only\",\"ordering_source\":\"AccessOrder\",\"limit\":3,\"cursor\":false,\"covering_scan\":true,\"rows_expected\":3,\"children\":[{\"node_id\":1,\"node_type\":\"LimitOffset\",\"layer\":\"terminal\",\"execution_mode\":\"Materialized\",\"execution_mode_detail\":\"materialized\",\"access_strategy\":null,\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":null,\"limit\":1,\"cursor\":null,\"covering_scan\":null,\"rows_expected\":null,\"children\":[],\"node_properties\":{}}],\"node_properties\":{}}";

    assert_eq!(
        json, expected,
        "canonical execution-node JSON shape drifted",
    );
}

#[test]
fn execution_descriptor_canonical_json_field_order_is_stable() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::IndexPrefixScan,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: Some(ExplainAccessPath::IndexPrefix {
            name: "users_by_email",
            fields: vec!["email"],
            prefix_len: 1,
            values: vec![Value::Text("alpha@example.com".to_string())],
        }),
        predicate_pushdown: Some("strict_all_or_none".to_string()),
        residual_predicate: None,
        projection: None,
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(5),
        cursor: Some(true),
        covering_scan: Some(false),
        rows_expected: Some(5),
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };
    let json = descriptor.render_json_canonical();
    let ordered_fields = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    let mut last_position = 0usize;
    for (index, field) in ordered_fields.iter().enumerate() {
        let position = json.find(field).unwrap_or_else(|| {
            panic!("canonical execution JSON missing expected field at index {index}: {field}")
        });
        if index > 0 {
            assert!(
                position > last_position,
                "canonical execution JSON field ordering drifted at field `{field}`",
            );
        }
        last_position = position;
    }
}

fn assert_execution_json_top_level_field_order(json: &str) {
    let ordered_fields = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    let mut last_position = 0usize;
    for (index, field) in ordered_fields.iter().enumerate() {
        let position = json.find(field).unwrap_or_else(|| {
            panic!("canonical execution JSON missing expected field at index {index}: {field}")
        });
        if index > 0 {
            assert!(
                position > last_position,
                "canonical execution JSON field ordering drifted at field `{field}`",
            );
        }
        last_position = position;
    }
}

fn assert_execution_json_top_level_field_names_are_unique(json: &str) {
    let field_tokens = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    for field_token in field_tokens {
        let occurrences = json.match_indices(field_token).count();
        assert_eq!(
            occurrences, 1,
            "canonical execution JSON field naming drifted: expected exactly one top-level `{field_token}` token"
        );
    }
}

#[test]
fn execution_descriptor_canonical_json_schema_is_consistent_across_node_families() {
    let cases = [
        (
            "scan",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::IndexRangeScan,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "pipeline",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::TopNSeek,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: None,
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "aggregate",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateCount,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "terminal",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::LimitOffset,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: None,
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
    ];

    for (expected_layer, descriptor) in cases {
        let json = descriptor.render_json_canonical();
        assert_execution_json_top_level_field_order(&json);
        assert_execution_json_top_level_field_names_are_unique(&json);
        assert!(
            json.contains(&format!("\"layer\":\"{expected_layer}\"")),
            "canonical execution JSON must expose stable layer ownership for each node family",
        );
    }
}

#[test]
fn execution_descriptor_canonical_json_missing_optional_fields_render_explicit_nulls() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::LimitOffset,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: None,
        predicate_pushdown: None,
        residual_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };

    let json = descriptor.render_json_canonical();
    let expected_null_fields = [
        "\"access_strategy\":null",
        "\"predicate_pushdown\":null",
        "\"fast_path_selected\":null",
        "\"fast_path_reason\":null",
        "\"residual_predicate\":null",
        "\"projection\":null",
        "\"ordering_source\":null",
        "\"limit\":null",
        "\"cursor\":null",
        "\"covering_scan\":null",
        "\"rows_expected\":null",
    ];
    for expected_null in expected_null_fields {
        assert!(
            json.contains(expected_null),
            "canonical execution JSON optional/null projection drifted: missing `{expected_null}`",
        );
    }
}

fn assert_execution_additive_metadata_parity(
    descriptor: &ExplainExecutionNodeDescriptor,
    expected_layer: &str,
    expected_execution_mode_detail: &str,
    expected_pushdown_mode: &str,
    expected_fast_path_selected: Option<bool>,
    expected_fast_path_reason: Option<&str>,
) {
    let text = descriptor.render_text_tree();
    let json = descriptor.render_json_canonical();

    assert!(
        text.contains("node_id=0"),
        "text execution explain must expose deterministic node_id",
    );
    assert!(
        json.contains("\"node_id\":0"),
        "JSON execution explain must expose deterministic node_id",
    );
    assert!(
        text.contains(&format!("layer={expected_layer}")),
        "text execution explain must expose stable layer ownership",
    );
    assert!(
        json.contains(&format!("\"layer\":\"{expected_layer}\"")),
        "JSON execution explain must expose stable layer ownership",
    );
    assert!(
        text.contains(&format!(
            "execution_mode_detail={expected_execution_mode_detail}"
        )),
        "text execution explain must expose execution_mode_detail",
    );
    assert!(
        json.contains(&format!(
            "\"execution_mode_detail\":\"{expected_execution_mode_detail}\""
        )),
        "JSON execution explain must expose execution_mode_detail",
    );
    assert!(
        text.contains(&format!("predicate_pushdown_mode={expected_pushdown_mode}")),
        "text execution explain must expose predicate pushdown mode",
    );
    assert!(
        json.contains(&format!(
            "\"predicate_pushdown_mode\":\"{expected_pushdown_mode}\""
        )),
        "JSON execution explain must expose predicate pushdown mode",
    );

    if let Some(selected) = expected_fast_path_selected {
        assert!(
            text.contains(&format!("fast_path_selected={selected}")),
            "text execution explain must expose fast-path selection when present",
        );
        assert!(
            json.contains(&format!("\"fast_path_selected\":{selected}")),
            "JSON execution explain must expose fast-path selection when present",
        );
    } else {
        assert!(
            !text.contains("fast_path_selected="),
            "text execution explain must omit fast-path selection when absent",
        );
        assert!(
            json.contains("\"fast_path_selected\":null"),
            "JSON execution explain must project null fast-path selection when absent",
        );
    }

    if let Some(reason) = expected_fast_path_reason {
        assert!(
            text.contains(&format!("fast_path_reason={reason}")),
            "text execution explain must expose fast-path reason when present",
        );
        assert!(
            json.contains(&format!("\"fast_path_reason\":\"{reason}\"")),
            "JSON execution explain must expose fast-path reason when present",
        );
    } else {
        assert!(
            !text.contains("fast_path_reason="),
            "text execution explain must omit fast-path reason when absent",
        );
        assert!(
            json.contains("\"fast_path_reason\":null"),
            "JSON execution explain must project null fast-path reason when absent",
        );
    }
}

#[test]
fn execution_descriptor_text_json_additive_metadata_parity_is_stable_for_route_shapes() {
    let mut fast_path_properties = ExplainPropertyMap::new();
    fast_path_properties.insert(
        "fast_path_selected",
        Value::Text("secondary_index".to_string()),
    );
    fast_path_properties.insert(
        "fast_path_selected_reason",
        Value::Text("topn_eligible".to_string()),
    );

    let cases = [
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateSeekFirst,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }),
                limit: None,
                cursor: Some(false),
                covering_scan: Some(false),
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
            "aggregate",
            "materialized",
            "none",
            None,
            None,
        ),
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateExists,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
                limit: Some(3),
                cursor: Some(true),
                covering_scan: Some(false),
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
            "aggregate",
            "streaming",
            "none",
            None,
            None,
        ),
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::TopNSeek,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: Some("strict_all_or_none".to_string()),
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
                limit: Some(5),
                cursor: Some(false),
                covering_scan: Some(false),
                rows_expected: Some(5),
                children: Vec::new(),
                node_properties: fast_path_properties.clone(),
            },
            "pipeline",
            "streaming",
            "full",
            Some(true),
            Some("topn_eligible"),
        ),
    ];

    for (
        descriptor,
        expected_layer,
        expected_execution_mode_detail,
        expected_pushdown_mode,
        expected_fast_path_selected,
        expected_fast_path_reason,
    ) in cases
    {
        assert_execution_additive_metadata_parity(
            &descriptor,
            expected_layer,
            expected_execution_mode_detail,
            expected_pushdown_mode,
            expected_fast_path_selected,
            expected_fast_path_reason,
        );
    }
}

#[test]
fn execution_descriptor_pushdown_mode_projection_is_stable() {
    let mut descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::IndexPredicatePrefilter,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: None,
        predicate_pushdown: None,
        residual_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };

    let none_mode = descriptor.render_json_canonical();
    assert!(
        none_mode.contains("\"predicate_pushdown_mode\":\"none\""),
        "missing pushdown mode `none` projection",
    );

    descriptor.predicate_pushdown = Some("strict_all_or_none".to_string());
    let full_mode = descriptor.render_json_canonical();
    assert!(
        full_mode.contains("\"predicate_pushdown_mode\":\"full\""),
        "missing pushdown mode `full` projection",
    );

    descriptor.predicate_pushdown = Some("index_predicate".to_string());
    descriptor.residual_predicate = Some(ExplainPredicate::True);
    let partial_mode = descriptor.render_json_canonical();
    assert!(
        partial_mode.contains("\"predicate_pushdown_mode\":\"partial\""),
        "missing pushdown mode `partial` projection",
    );
}

fn aggregate_terminal_plan_snapshot(plan: &ExplainAggregateTerminalPlan) -> String {
    let execution = plan.execution();
    let node = plan.execution_node_descriptor();
    let descriptor_json = node.render_json_canonical();

    format!(
        concat!(
            "terminal={:?}\n",
            "route={:?}\n",
            "query_access={:?}\n",
            "query_order_by={:?}\n",
            "query_page={:?}\n",
            "query_grouping={:?}\n",
            "query_pushdown={:?}\n",
            "query_consistency={:?}\n",
            "execution_aggregation={:?}\n",
            "execution_mode={:?}\n",
            "execution_ordering_source={:?}\n",
            "execution_limit={:?}\n",
            "execution_cursor={}\n",
            "execution_covering_projection={}\n",
            "execution_node_properties={:?}\n",
            "execution_node_json={}",
        ),
        plan.terminal(),
        plan.route(),
        plan.query().access(),
        plan.query().order_by(),
        plan.query().page(),
        plan.query().grouping(),
        plan.query().order_pushdown(),
        plan.query().consistency(),
        execution.aggregation(),
        execution.execution_mode(),
        execution.ordering_source(),
        execution.limit(),
        execution.cursor(),
        execution.covering_projection(),
        execution.node_properties(),
        descriptor_json,
    )
}

#[test]
fn explain_aggregate_terminal_plan_snapshot_seek_route_is_stable() {
    // Phase 1: build a deterministic index-prefix query explain payload.
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let query_explain = plan.explain();

    // Phase 2: build one seek-route execution descriptor and snapshot the whole payload.
    let mut node_properties = ExplainPropertyMap::new();
    node_properties.insert("fetch", Value::from(1_u64));
    let terminal_plan = ExplainAggregateTerminalPlan::new(
        query_explain,
        AggregateKind::Min,
        ExplainExecutionDescriptor {
            access_strategy: ExplainAccessPath::IndexPrefix {
                name: "explain::pushdown_tag",
                fields: vec!["tag"],
                prefix_len: 1,
                values: vec![Value::Text("alpha".to_string())],
            },
            covering_projection: false,
            aggregation: AggregateKind::Min,
            execution_mode: ExplainExecutionMode::Materialized,
            ordering_source: ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 },
            limit: None,
            cursor: false,
            node_properties,
        },
    );

    let actual = aggregate_terminal_plan_snapshot(&terminal_plan);
    let expected = "terminal=Min
route=IndexSeekFirst { fetch: 1 }
query_access=IndexPrefix { name: \"explain::pushdown_tag\", fields: [\"tag\"], prefix_len: 1, values: [Text(\"alpha\")] }
query_order_by=Fields([ExplainOrder { field: \"tag\", direction: Asc }, ExplainOrder { field: \"id\", direction: Asc }])
query_page=None
query_grouping=None
query_pushdown=MissingModelContext
query_consistency=Ignore
execution_aggregation=Min
execution_mode=Materialized
execution_ordering_source=IndexSeekFirst { fetch: 1 }
execution_limit=None
execution_cursor=false
execution_covering_projection=false
execution_node_properties={\"fetch\": Uint(1)}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateSeekFirst\",\"layer\":\"aggregate\",\"execution_mode\":\"Materialized\",\"execution_mode_detail\":\"materialized\",\"access_strategy\":{\"type\":\"IndexPrefix\",\"name\":\"explain::pushdown_tag\",\"fields\":[\"tag\"],\"prefix_len\":1,\"values\":[\"Text(\\\"alpha\\\")\"]},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"IndexSeekFirst\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{\"fetch\":\"Uint(1)\"}}";

    assert_eq!(
        actual, expected,
        "aggregate terminal seek-route explain snapshot drifted",
    );
}

#[test]
fn explain_aggregate_terminal_plan_snapshot_standard_route_is_stable() {
    // Phase 1: build a deterministic full-scan query explain payload.
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(crate::db::query::plan::PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let query_explain = plan.explain();

    // Phase 2: build one standard-route execution descriptor and snapshot the whole payload.
    let terminal_plan = ExplainAggregateTerminalPlan::new(
        query_explain,
        AggregateKind::Exists,
        ExplainExecutionDescriptor {
            access_strategy: ExplainAccessPath::FullScan,
            covering_projection: false,
            aggregation: AggregateKind::Exists,
            execution_mode: ExplainExecutionMode::Streaming,
            ordering_source: ExplainExecutionOrderingSource::AccessOrder,
            limit: Some(3),
            cursor: true,
            node_properties: ExplainPropertyMap::new(),
        },
    );

    let actual = aggregate_terminal_plan_snapshot(&terminal_plan);
    let expected = "terminal=Exists
route=Standard
query_access=FullScan
query_order_by=Fields([ExplainOrder { field: \"id\", direction: Asc }])
query_page=Page { limit: Some(3), offset: 1 }
query_grouping=None
query_pushdown=MissingModelContext
query_consistency=Ignore
execution_aggregation=Exists
execution_mode=Streaming
execution_ordering_source=AccessOrder
execution_limit=Some(3)
execution_cursor=true
execution_covering_projection=false
execution_node_properties={}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateExists\",\"layer\":\"aggregate\",\"execution_mode\":\"Streaming\",\"execution_mode_detail\":\"streaming\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"AccessOrder\",\"limit\":3,\"cursor\":true,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{}}";

    assert_eq!(
        actual, expected,
        "aggregate terminal standard-route explain snapshot drifted",
    );
}
