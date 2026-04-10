//! Module: query::fingerprint::continuation_signature
//! Responsibility: deterministic continuation-signature derivation from explain plans.
//! Does not own: continuation token decoding/validation.
//! Boundary: query-plan shape signature surface used by cursor token checks.

///
/// TESTS
///
use crate::{
    db::{
        access::AccessPath,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, CursorBoundarySlot,
            GroupedContinuationToken, IndexRangeCursorAnchor, TokenWireError,
            prepare_grouped_cursor,
        },
        direction::Direction,
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::field::FieldRef,
            explain::ExplainGrouping,
            fingerprint::{
                finalize_sha256_digest, hash_parts, new_continuation_signature_hasher_v1,
            },
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::OrderDirection,
            plan::{
                AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec,
                GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
                GroupedExecutionConfig, LoadSpec, LogicalPlan, OrderSpec, PageSpec, QueryMode,
                expr::{Alias, Expr, FieldId, ProjectionField, ProjectionSpec},
            },
        },
    },
    model::index::IndexModel,
    types::Ulid,
    value::Value,
};
use std::fmt::Write;

fn continuation_signature_with_projection(
    explain: &crate::db::query::explain::ExplainPlan,
    entity_path: &'static str,
    projection: &ProjectionSpec,
) -> ContinuationSignature {
    let mut hasher = new_continuation_signature_hasher_v1();
    hash_parts::hash_explain_plan_profile_internal(
        &mut hasher,
        explain,
        hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        Some(projection),
    );

    ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
}

fn signature_hex(signature: ContinuationSignature) -> String {
    let mut hex = String::with_capacity(64);
    for byte in signature.into_bytes() {
        let _ = write!(&mut hex, "{byte:02x}");
    }

    hex
}

fn scalar_explain_with_fixed_shape() -> crate::db::query::explain::ExplainPlan {
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().predicate = Some(FieldRef::new("id").eq(Ulid::default()));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(25),
        offset: 0,
    });

    plan.explain()
}

fn grouped_query_with_fixed_shape() -> AccessPlannedQuery {
    AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore).into_grouped(
        GroupSpec {
            group_fields: vec![FieldSlot::from_parts_for_test(1, "rank")],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
        },
    )
}

fn grouped_explain_with_fixed_shape() -> crate::db::query::explain::ExplainPlan {
    grouped_query_with_fixed_shape().explain()
}

#[test]
fn signature_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = Predicate::And(vec![
        FieldRef::new("id").eq(id),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]);
    let predicate_b = Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        FieldRef::new("id").eq(id),
    ]);

    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_is_deterministic_for_by_keys() {
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
        static_planning_shape: None,
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
        static_planning_shape: None,
    };

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_excludes_pagination_window_state() {
    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    plan_b.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 999,
    });

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_order_changes() {
    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan_b.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Desc)],
    });

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_order_field_set_changes() {
    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan_b.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Asc)],
    });

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_distinct_flag_changes() {
    let plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().distinct = true;

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_with_entity_path() {
    let plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert_ne!(
        plan.continuation_signature("tests::EntityA"),
        plan.continuation_signature("tests::EntityB")
    );
}

#[test]
fn continuation_signature_projection_alias_only_change_does_not_invalidate() {
    let explain = scalar_explain_with_fixed_shape();
    let semantic_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let alias_only_projection =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Field(FieldId::new("rank"))),
                name: Alias::new("rank_expr"),
            },
            alias: Some(Alias::new("rank_alias")),
        }]);

    let semantic_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &semantic_projection);
    let alias_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &alias_only_projection);

    assert_eq!(semantic_signature, alias_signature);
}

#[test]
fn continuation_signature_numeric_projection_alias_only_change_does_not_invalidate() {
    let explain = scalar_explain_with_fixed_shape();
    let numeric_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let alias_only_numeric_projection =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_expr"),
            },
            alias: Some(Alias::new("rank_plus_one")),
        }]);

    let semantic_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &numeric_projection);
    let alias_signature = continuation_signature_with_projection(
        &explain,
        "tests::Entity",
        &alias_only_numeric_projection,
    );

    assert_eq!(
        semantic_signature, alias_signature,
        "numeric projection alias wrappers must not affect continuation identity",
    );
}

#[test]
fn continuation_decode_remains_stable_for_alias_only_numeric_projection_changes() {
    let explain = grouped_explain_with_fixed_shape();
    let numeric_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let alias_only_numeric_projection =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_expr"),
            },
            alias: Some(Alias::new("rank_plus_one")),
        }]);

    let semantic_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &numeric_projection);
    let alias_signature = continuation_signature_with_projection(
        &explain,
        "tests::Entity",
        &alias_only_numeric_projection,
    );
    let token = GroupedContinuationToken::new_with_direction(
        semantic_signature,
        vec![Value::Uint(7)],
        Direction::Asc,
        0,
    );
    let encoded = token
        .encode()
        .expect("grouped continuation token should encode");
    let decoded = prepare_grouped_cursor(
        "tests::Entity",
        None,
        alias_signature,
        0,
        Some(encoded.as_slice()),
    )
    .expect("alias-only numeric projection changes must preserve decode+resume");

    assert_eq!(
        decoded.last_group_key(),
        Some(vec![Value::Uint(7)].as_slice())
    );
}

#[test]
fn continuation_signature_changes_when_grouped_strategy_changes() {
    let mut hash_strategy = grouped_explain_with_fixed_shape();
    let mut ordered_strategy = hash_strategy.clone();

    let ExplainGrouping::Grouped {
        strategy: hash_value,
        ..
    } = &mut hash_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *hash_value = "hash_group";
    let ExplainGrouping::Grouped {
        strategy: ordered_value,
        ..
    } = &mut ordered_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *ordered_value = "ordered_group";

    assert_ne!(
        hash_strategy.continuation_signature("tests::Entity"),
        ordered_strategy.continuation_signature("tests::Entity"),
        "grouped continuation signatures must remain strategy-sensitive for resume compatibility",
    );
}

#[test]
fn continuation_signature_identity_projection_remains_stable() {
    let plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let explain = plan.explain();
    let identity_projection = plan.projection_spec_for_identity();

    let signature_from_plan = plan.continuation_signature("tests::Entity");
    let signature_from_identity =
        continuation_signature_with_projection(&explain, "tests::Entity", &identity_projection);

    assert_eq!(
        signature_from_plan, signature_from_identity,
        "identity projection must preserve continuation signature stability",
    );
}

#[test]
fn grouped_continuation_signature_identity_projection_remains_stable() {
    let plan = grouped_query_with_fixed_shape();
    let explain = plan.explain();
    let identity_projection = plan.projection_spec_for_identity();

    assert_eq!(
        plan.continuation_signature("tests::Entity"),
        continuation_signature_with_projection(&explain, "tests::Entity", &identity_projection),
        "grouped continuation signature must stay stable across plan-owned and explain-owned grouped projection seams",
    );
}

#[test]
fn continuation_signature_projection_semantic_change_invalidates() {
    let explain = scalar_explain_with_fixed_shape();
    let projection_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let projection_tenant = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("tenant")),
        alias: None,
    }]);

    let rank_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &projection_rank);
    let tenant_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &projection_tenant);

    assert_ne!(rank_signature, tenant_signature);
}

#[test]
fn continuation_signature_numeric_projection_semantic_change_invalidates() {
    let explain = scalar_explain_with_fixed_shape();
    let projection_add_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let projection_mul_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Mul,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);

    let add_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &projection_add_one);
    let mul_signature =
        continuation_signature_with_projection(&explain, "tests::Entity", &projection_mul_one);

    assert_ne!(
        add_signature, mul_signature,
        "numeric projection semantic operator changes must invalidate continuation identity",
    );
}

#[test]
fn continuation_signature_grouped_projection_semantic_change_invalidates() {
    let explain = grouped_explain_with_fixed_shape();
    let grouped_projection_a =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: None,
        }]);
    let grouped_projection_b =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            },
            alias: None,
        }]);

    let signature_a =
        continuation_signature_with_projection(&explain, "tests::Entity", &grouped_projection_a);
    let signature_b =
        continuation_signature_with_projection(&explain, "tests::Entity", &grouped_projection_b);

    assert_ne!(
        signature_a, signature_b,
        "grouped continuation signatures must invalidate on grouped projection semantic changes",
    );
}

#[test]
fn signature_changes_when_group_fields_change() {
    let grouped_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "phase"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "region"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_a.continuation_signature("tests::Entity"),
        grouped_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_spec_changes() {
    let grouped_count: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_rank: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count.continuation_signature("tests::Entity"),
        grouped_max_rank.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_target_field_changes() {
    let grouped_max_rank: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_score: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Max,
                    target_field: Some("score".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_max_rank.continuation_signature("tests::Entity"),
        grouped_max_score.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_distinct_changes() {
    let grouped_count: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_count_distinct: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count.continuation_signature("tests::Entity"),
        grouped_count_distinct.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_between_sum_and_sum_distinct_grouped_shapes() {
    let grouped_sum: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });
    let grouped_sum_distinct: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });

    assert_ne!(
        grouped_sum.continuation_signature("tests::Entity"),
        grouped_sum_distinct.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_field_order_changes() {
    let grouped_ab: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "phase"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_ba: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(2, "phase"),
                    FieldSlot::from_parts_for_test(1, "tenant"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_ab.continuation_signature("tests::Entity"),
        grouped_ba.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_order_changes() {
    let grouped_count_then_max: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: AggregateKind::Max,
                        target_field: Some("rank".to_string()),
                        distinct: false,
                    },
                ],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_then_count: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: AggregateKind::Max,
                        target_field: Some("rank".to_string()),
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    },
                ],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count_then_max.continuation_signature("tests::Entity"),
        grouped_max_then_count.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_between_scalar_and_grouped_shape() {
    let scalar: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let grouped: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        scalar.continuation_signature("tests::Entity"),
        grouped.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_grouped_limits_change() {
    let grouped_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(128, 4096),
            });

    assert_ne!(
        grouped_a.continuation_signature("tests::Entity"),
        grouped_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_grouped_having_changes() {
    let grouped_having_gt: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gt,
                        value: Value::Uint(1),
                    }],
                }),
            );
    let grouped_having_gte: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gte,
                        value: Value::Uint(1),
                    }],
                }),
            );

    assert_ne!(
        grouped_having_gt.continuation_signature("tests::Entity"),
        grouped_having_gte.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_snapshot_grouped_having_shape_is_stable() {
    let grouped_having: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: AggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gt,
                        value: Value::Uint(1),
                    }],
                }),
            );
    let signature = signature_hex(grouped_having.continuation_signature("tests::Entity"));
    let expected = "0e283854004019d25f3d5e0768e093e813e062eecc49346aa992a74f6ec5bb4a".to_string();

    assert_eq!(
        signature, expected,
        "grouped+having signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_grouped_distinct_shape_is_stable() {
    let grouped_distinct: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let signature = signature_hex(grouped_distinct.continuation_signature("tests::Entity"));
    let expected = "c5600a836ee70450c5c0f91672135c9e6e1278ecacf34144c64bd6387ebb04b4".to_string();

    assert_eq!(
        signature, expected,
        "grouped+distinct signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_global_distinct_sum_shape_is_stable() {
    let global_distinct_sum: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });
    let signature = signature_hex(global_distinct_sum.continuation_signature("tests::Entity"));
    let expected = "59ab29ffd24a417a14ae7b79cdc67b28d5b5ca07b957f03685256df87e38bab6".to_string();

    assert_eq!(
        signature, expected,
        "global distinct sum signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_ordered_group_hint_shape_is_stable() {
    let grouped_ordered: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: IndexModel::generated("idx_tenant", "tests", &["tenant"], false),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
    });
    let signature = signature_hex(grouped_ordered.continuation_signature("tests::Entity"));
    let expected = "7e2c97e1a1d66a17a7be13ef03fa513a532be3629974e57e5d24ccd04e0ab797".to_string();

    assert_eq!(
        signature, expected,
        "ordered-hint grouped signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn continuation_token_round_trips_index_range_anchor() {
    let raw_key = vec![0xAA, 0xBB, 0xCC];
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(42))],
    };
    let signature = ContinuationSignature::from_bytes([7u8; 32]);

    let token = ContinuationToken::new_index_range_with_direction(
        signature,
        boundary.clone(),
        IndexRangeCursorAnchor::new(raw_key.clone()),
        Direction::Asc,
        3,
    );

    let encoded = token
        .encode()
        .expect("token with index-range anchor encodes");
    let decoded =
        ContinuationToken::decode(&encoded).expect("token with index-range anchor decodes");

    assert_eq!(decoded.signature(), signature);
    assert_eq!(decoded.boundary(), &boundary);
    assert_eq!(decoded.initial_offset(), 3);
    let decoded_anchor = decoded
        .index_range_anchor()
        .expect("decoded token should include index-range anchor");
    assert_eq!(decoded_anchor.last_raw_key(), raw_key.as_slice());
}

#[test]
fn continuation_token_decode_rejects_unknown_version() {
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
    };
    let signature = ContinuationSignature::from_bytes([3u8; 32]);
    let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, 9);
    let encoded = token
        .encode_with_version_for_test(99)
        .expect("unknown-version wire token should encode");

    let err = ContinuationToken::decode(&encoded).expect_err("unknown version must fail");
    assert_eq!(err, TokenWireError::UnsupportedVersion { version: 99 });
}

#[test]
fn continuation_token_decode_rejects_v2_version() {
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
    };
    let signature = ContinuationSignature::from_bytes([4u8; 32]);
    let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Desc, 11);
    let encoded = token
        .encode_with_version_for_test(2)
        .expect("v2-version wire token should encode");

    let err = ContinuationToken::decode(&encoded).expect_err("v2-version token must fail");
    assert_eq!(err, TokenWireError::UnsupportedVersion { version: 2 });
}
