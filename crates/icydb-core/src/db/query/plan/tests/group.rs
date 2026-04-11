//! Module: db::query::plan::tests::group
//! Covers grouped query-plan semantics and grouped validation behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        predicate::{CompareOp, MissingRowPolicy},
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
            GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
            GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupPlanError, GroupSpec,
            GroupedCursorPolicyViolation, GroupedDistinctExecutionStrategy, GroupedExecutionConfig,
            GroupedFoldPath, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
            PlanPolicyError, PlanUserError, QueryMode,
            expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
            global_distinct_field_aggregate_admissibility,
            global_distinct_group_spec_for_semantic_aggregate, grouped_cursor_policy_violation,
            grouped_distinct_admissibility, grouped_executor_handoff,
            is_global_distinct_field_aggregate_candidate,
            validate::{
                ExprPlanError, PlanError, PolicyPlanError,
                validate_group_projection_expr_compatibility, validate_query_semantics,
            },
            validate_group_query_semantics,
        },
        schema::SchemaInfo,
    },
    model::{field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::generated("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity! {
    ident = PlanValidateGroupedEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

fn load_plan(access: AccessPlan<Value>) -> AccessPlannedQuery {
    load_plan_with_order_and_distinct(access, None, false)
}

fn load_plan_with_order_and_distinct(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
) -> AccessPlannedQuery {
    load_plan_with_order_distinct_and_limit(access, order, distinct, None)
}

fn load_plan_with_order_distinct_and_limit(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
    limit: Option<u32>,
) -> AccessPlannedQuery {
    AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order,
            distinct,
            delete_limit: None,
            page: limit.map(|limit| PageSpec {
                limit: Some(limit),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    }
}

fn grouped_plan(
    base: AccessPlannedQuery,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
) -> AccessPlannedQuery {
    grouped_plan_with_having(base, group_fields, aggregates, None)
}

fn grouped_plan_with_having(
    base: AccessPlannedQuery,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
    having: Option<GroupHavingSpec>,
) -> AccessPlannedQuery {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    base.into_grouped_with_having(
        GroupSpec {
            group_fields: group_fields
                .into_iter()
                .map(|field| {
                    FieldSlot::resolve(model, field).unwrap_or_else(|| {
                        FieldSlot::from_parts_for_test(usize::MAX, field.to_string())
                    })
                })
                .collect(),
            aggregates,
            execution: GroupedExecutionConfig::unbounded(),
        },
        having,
    )
}

fn grouped_spec_for_projection_expr_tests(group_fields: Vec<&str>) -> GroupSpec {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    GroupSpec {
        group_fields: group_fields
            .into_iter()
            .map(|field| {
                FieldSlot::resolve(model, field)
                    .expect("grouped projection compatibility tests require valid field slots")
            })
            .collect(),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    }
}

fn finalized_grouped_plan(plan: &AccessPlannedQuery) -> AccessPlannedQuery {
    let mut finalized = plan.clone();
    finalized
        .finalize_static_planning_shape_for_model(
            <PlanValidateGroupedEntity as EntitySchema>::MODEL,
        )
        .expect("grouped plan tests require planner-frozen execution shape");

    finalized
}

fn is_group_plan_error(err: &PlanError, predicate: impl FnOnce(&GroupPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Group(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        PlanError::Policy(inner) => match inner.as_ref() {
            PlanPolicyError::Group(inner) => predicate(inner.as_ref()),
            PlanPolicyError::Policy(_) => false,
        },
        PlanError::Cursor(_) => false,
    }
}

fn is_policy_plan_error(err: &PlanError, predicate: impl FnOnce(&PolicyPlanError) -> bool) -> bool {
    match err {
        PlanError::Policy(inner) => match inner.as_ref() {
            PlanPolicyError::Policy(inner) => predicate(inner.as_ref()),
            PlanPolicyError::Group(_) => false,
        },
        PlanError::User(_) | PlanError::Cursor(_) => false,
    }
}

fn is_expr_plan_error(err: &PlanError, predicate: impl FnOnce(&ExprPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Expr(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        PlanError::Policy(_) | PlanError::Cursor(_) => false,
    }
}

#[test]
fn grouped_plan_rejects_empty_group_fields() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("empty group-fields spec must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::EmptyGroupFields
    )));
}

#[test]
fn grouped_plan_accepts_global_distinct_count_field_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("global grouped count(distinct field) should be accepted");
}

#[test]
fn grouped_plan_accepts_global_distinct_sum_field_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("global grouped sum(distinct field) should be accepted");
}

#[test]
fn grouped_plan_accepts_global_distinct_avg_field_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("global grouped avg(distinct field) should be accepted");
}

#[test]
fn global_distinct_shape_helper_matches_aggregate_expr_path_for_count_sum_and_avg() {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);

    let helper_count =
        global_distinct_group_spec_for_semantic_aggregate(AggregateKind::Count, "tag", execution)
            .expect("count distinct helper shape should build");
    let builder_count = GroupSpec::global_distinct_shape_from_aggregate_expr(
        &crate::db::count_by("tag").distinct(),
        execution,
    );
    assert_eq!(
        helper_count, builder_count,
        "count distinct shape helper must match aggregate-expression semantic path",
    );

    let helper_sum =
        global_distinct_group_spec_for_semantic_aggregate(AggregateKind::Sum, "rank", execution)
            .expect("sum distinct helper shape should build");
    let builder_sum = GroupSpec::global_distinct_shape_from_aggregate_expr(
        &crate::db::sum("rank").distinct(),
        execution,
    );
    assert_eq!(
        helper_sum, builder_sum,
        "sum distinct shape helper must match aggregate-expression semantic path",
    );

    let helper_avg =
        global_distinct_group_spec_for_semantic_aggregate(AggregateKind::Avg, "rank", execution)
            .expect("avg distinct helper shape should build");
    let builder_avg = GroupSpec::global_distinct_shape_from_aggregate_expr(
        &crate::db::avg("rank").distinct(),
        execution,
    );
    assert_eq!(
        helper_avg, builder_avg,
        "avg distinct shape helper must match aggregate-expression semantic path",
    );
}

#[test]
fn global_distinct_shape_helper_rejects_unsupported_kinds_structurally() {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);

    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let result = global_distinct_group_spec_for_semantic_aggregate(kind, "tag", execution);

        assert!(
            matches!(
                result,
                Err(GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind)
            ),
            "unsupported grouped distinct kind should be rejected by semantic helper: {kind:?}",
        );
    }
}

#[test]
fn grouped_cursor_policy_violation_contract_is_shared_for_limit_and_global_distinct_cases() {
    let grouped_without_limit = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            None,
        ),
        vec!["tag"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );
    let grouped_without_limit_plan = grouped_without_limit
        .grouped_plan()
        .expect("grouped plan should be present");
    assert_eq!(
        grouped_cursor_policy_violation(grouped_without_limit_plan, true)
            .map(GroupedCursorPolicyViolation::invariant_message),
        Some("grouped continuation cursors require an explicit LIMIT"),
        "grouped cursor contract should require explicit limit when continuation is present",
    );

    let grouped_global_distinct = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );
    let grouped_global_distinct_plan = grouped_global_distinct
        .grouped_plan()
        .expect("grouped plan should be present");
    assert_eq!(
        grouped_cursor_policy_violation(grouped_global_distinct_plan, true)
            .map(GroupedCursorPolicyViolation::invariant_message),
        Some("global DISTINCT grouped aggregates do not support continuation cursors"),
        "global DISTINCT grouped cursor policy should reject continuation reuse",
    );
}

#[test]
fn grouped_plan_rejects_global_distinct_sum_non_numeric_target() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("global grouped sum(distinct non-numeric) should fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
            if *index == 0 && field == "tag"
    )));
}

#[test]
fn grouped_plan_rejects_global_distinct_unsupported_kind() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Exists,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("global grouped distinct should reject unsupported aggregate kinds");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )));
}

#[test]
fn grouped_plan_rejects_global_distinct_mixed_aggregate_shape() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: Some("tag".to_string()),
                distinct: true,
            },
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            },
        ],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("global grouped distinct shape should reject mixed aggregate list");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::GlobalDistinctAggregateShapeUnsupported
    )));
}

#[test]
fn grouped_plan_rejects_global_distinct_shape_with_having_clause() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("global DISTINCT grouped aggregate shape must reject HAVING");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::GlobalDistinctAggregateShapeUnsupported
    )));
}

#[test]
fn grouped_plan_rejects_unknown_group_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("unknown group field must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_plan_rejects_duplicate_group_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank", "rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("duplicate group field must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DuplicateGroupField { field } if field == "rank"
    )));
}

#[test]
fn grouped_plan_rejects_distinct_without_adjacency_proof() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped distinct should fail without ordered-group adjacency eligibility");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctAdjacencyEligibilityRequired
    )));
}

#[test]
fn grouped_plan_rejects_order_prefix_not_aligned_with_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped order should fail when grouped-key prefix is missing");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
    )));
}

#[test]
fn grouped_plan_rejects_order_without_limit() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_and_distinct(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped order should fail when LIMIT is omitted");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderRequiresLimit
    )));
}

#[test]
fn grouped_plan_accepts_order_prefix_aligned_with_group_keys_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped).expect(
        "grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit",
    );
}

#[test]
fn grouped_plan_having_order_limit_composition_enforces_bounded_policy() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);

    let build = |order: Option<OrderSpec>, limit: Option<u32>| {
        grouped_plan_with_having(
            load_plan_with_order_distinct_and_limit(
                AccessPlan::path(AccessPath::FullScan),
                order,
                false,
                limit,
            ),
            vec!["rank"],
            vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            Some(GroupHavingSpec {
                clauses: vec![GroupHavingClause {
                    symbol: GroupHavingSymbol::AggregateIndex(0),
                    op: CompareOp::Gt,
                    value: Value::Uint(0),
                }],
            }),
        )
    };

    // Accepted shape: grouped HAVING + grouped-key-aligned ORDER + explicit LIMIT.
    let accepted = build(
        Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        }),
        Some(1),
    );
    validate_group_query_semantics(schema, model, &accepted).expect(
        "grouped HAVING + ORDER should be accepted when ORDER prefix is aligned and LIMIT is explicit",
    );

    // Rejected shape: grouped HAVING + ORDER without LIMIT.
    let missing_limit = build(
        Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        }),
        None,
    );
    let err = validate_group_query_semantics(schema, model, &missing_limit)
        .expect_err("grouped HAVING + ORDER without LIMIT should fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderRequiresLimit
    )));

    // Rejected shape: grouped HAVING + LIMIT but ORDER prefix not aligned with grouped keys.
    let prefix_mismatch = build(
        Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        }),
        Some(1),
    );
    let err = validate_group_query_semantics(schema, model, &prefix_mismatch)
        .expect_err("grouped HAVING + misaligned ORDER should fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
    )));
}

#[test]
fn grouped_plan_rejects_empty_aggregate_spec_list() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        Vec::new(),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("empty grouped aggregate list must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::EmptyAggregates
    )));
}

#[test]
fn grouped_plan_rejects_unknown_aggregate_target_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Min,
            target_field: Some("missing_target".to_string()),
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("unknown grouped aggregate target field must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::UnknownAggregateTargetField { index, field }
            if *index == 0 && field == "missing_target"
    )));
}

#[test]
fn grouped_plan_accepts_min_field_aggregate_terminal_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Min,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped MIN(field) should be accepted in grouped v1");
}

#[test]
fn grouped_plan_accepts_max_field_aggregate_terminal_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Max,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped MAX(field) should be accepted in grouped v1");
}

#[test]
fn grouped_plan_accepts_count_field_aggregate_terminal_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped COUNT(field) should be accepted in grouped v1");
}

#[test]
fn grouped_plan_accepts_sum_field_aggregate_terminal_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped SUM(field) should be accepted in grouped v1");
}

#[test]
fn grouped_plan_accepts_avg_field_aggregate_terminal_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped AVG(field) should be accepted in grouped v1");
}

#[test]
fn grouped_plan_accepts_distinct_count_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: true,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("grouped distinct count should be accepted in grouped v1");
}

#[test]
fn grouped_plan_rejects_distinct_exists_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Exists,
            target_field: None,
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("distinct exists should be rejected until grouped distinct support expands");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )));
}

#[test]
fn grouped_plan_rejects_distinct_field_target_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Max,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("distinct field-target grouped terminals should remain rejected in grouped v1");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctAggregateFieldTargetUnsupported { index, kind, field }
            if *index == 0 && kind == "Max" && field == "rank"
    )));
}

#[test]
fn grouped_plan_rejects_having_with_distinct() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(0),
            }],
        }),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped having with distinct should be rejected");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctHavingUnsupported
    )));
}

#[test]
fn grouped_distinct_policy_contract_rejects_distinct_without_adjacency_proof() {
    assert_eq!(
        grouped_distinct_admissibility(true, false),
        GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired
        ),
        "grouped DISTINCT policy contract should classify adjacency-proof gating explicitly",
    );
}

#[test]
fn grouped_global_distinct_policy_contract_matches_candidate_and_having_rules() {
    let aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: Some("rank".to_string()),
        distinct: true,
    }];
    let having = GroupHavingSpec {
        clauses: vec![GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(0),
            op: CompareOp::Gt,
            value: Value::Uint(1),
        }],
    };

    assert!(
        is_global_distinct_field_aggregate_candidate(&[], aggregates.as_slice()),
        "global grouped DISTINCT contract should detect field-target aggregate candidates",
    );
    assert_eq!(
        global_distinct_field_aggregate_admissibility(aggregates.as_slice(), None),
        GroupDistinctAdmissibility::Allowed,
        "candidate global DISTINCT shape should be admissible without HAVING",
    );
    assert_eq!(
        global_distinct_field_aggregate_admissibility(aggregates.as_slice(), Some(&having)),
        GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        ),
        "global DISTINCT contract should reject HAVING consistently",
    );
}

#[test]
fn grouped_plan_rejects_having_group_field_outside_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::GroupField(
                    FieldSlot::resolve(model, "tag")
                        .expect("having group field slot should resolve for test"),
                ),
                op: CompareOp::Eq,
                value: Value::Text("alpha".to_string()),
            }],
        }),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("having should reject group-field symbols not declared in group keys");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingNonGroupFieldReference { field, .. } if field == "tag"
    )));
}

#[test]
fn grouped_plan_rejects_having_aggregate_index_out_of_bounds() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(1),
                op: CompareOp::Eq,
                value: Value::Uint(1),
            }],
        }),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("having should reject aggregate indexes beyond declared aggregate count");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingAggregateIndexOutOfBounds { aggregate_index, aggregate_count, .. }
            if *aggregate_index == 1 && *aggregate_count == 1
    )));
}

#[test]
fn grouped_plan_accepts_having_over_group_and_aggregate_symbols() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![
                GroupHavingClause {
                    symbol: GroupHavingSymbol::GroupField(
                        FieldSlot::resolve(model, "rank")
                            .expect("group field slot should resolve for test"),
                    ),
                    op: CompareOp::Gte,
                    value: Value::Int(1),
                },
                GroupHavingClause {
                    symbol: GroupHavingSymbol::AggregateIndex(0),
                    op: CompareOp::Gt,
                    value: Value::Uint(0),
                },
            ],
        }),
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("having over grouped keys and grouped aggregate symbols should be accepted");
}

#[test]
fn grouped_executor_handoff_preserves_group_fields_aggregates_and_execution_config() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = base.into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                .expect("rank field must resolve"),
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "tag")
                .expect("tag field must resolve"),
        ],
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
        execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
    });

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert!(
        handoff.projection_is_identity(),
        "planner grouped handoff should carry the canonical identity-projection contract",
    );
    assert_eq!(
        handoff
            .group_fields()
            .iter()
            .map(|field| field.field().to_string())
            .collect::<Vec<_>>(),
        vec!["rank".to_string(), "tag".to_string()]
    );
    assert_eq!(handoff.aggregate_projection_specs().len(), 2);
    assert_eq!(
        handoff.aggregate_projection_specs()[0].kind(),
        AggregateKind::Count
    );
    assert_eq!(handoff.aggregate_projection_specs()[0].target_field(), None);
    assert_eq!(
        handoff.aggregate_projection_specs()[1].kind(),
        AggregateKind::Max
    );
    assert_eq!(
        handoff.aggregate_projection_specs()[1].target_field(),
        Some("rank")
    );
    assert_eq!(handoff.execution().max_groups(), 11);
    assert_eq!(handoff.execution().max_group_bytes(), 2048);
    assert_eq!(handoff.projection_layout().group_field_positions(), &[0, 1]);
    assert_eq!(handoff.projection_layout().aggregate_positions(), &[2, 3]);
    assert_eq!(
        handoff.grouped_fold_path(),
        GroupedFoldPath::GenericReducers,
        "non-count grouped handoff shapes must stay on the generic grouped reducer path",
    );
    assert!(matches!(
        handoff.distinct_execution_strategy(),
        GroupedDistinctExecutionStrategy::None
    ));
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "grouped handoff should not project DISTINCT policy violations for non-DISTINCT grouped plans",
    );
    assert_eq!(
        handoff.base().scalar_plan().consistency,
        grouped.scalar_plan().consistency
    );
}

#[test]
fn grouped_executor_handoff_lowers_global_distinct_execution_strategy() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec![],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(handoff.group_fields().len(), 0);
    assert_eq!(handoff.aggregate_projection_specs().len(), 1);
    assert!(matches!(
        handoff.distinct_execution_strategy(),
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount { target_field, .. }
            if target_field == "tag"
    ));
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "global grouped DISTINCT execution strategy lowering should not project scalar DISTINCT policy violations",
    );
}

#[test]
fn grouped_executor_handoff_projects_dedicated_count_fold_path_for_single_count_rows() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(
        handoff.grouped_fold_path(),
        GroupedFoldPath::CountRowsDedicated,
        "single grouped COUNT(*) shapes must project the dedicated grouped count fold path",
    );
}

#[test]
fn grouped_executor_handoff_lowers_global_distinct_sum_execution_strategy() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec![],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(handoff.group_fields().len(), 0);
    assert_eq!(handoff.aggregate_projection_specs().len(), 1);
    assert!(matches!(
        handoff.distinct_execution_strategy(),
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum { target_field, .. }
            if target_field == "rank"
    ));
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "global grouped DISTINCT SUM strategy lowering should not project scalar DISTINCT policy violations",
    );
}

#[test]
fn grouped_executor_handoff_lowers_global_distinct_avg_execution_strategy() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec![],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(handoff.group_fields().len(), 0);
    assert_eq!(handoff.aggregate_projection_specs().len(), 1);
    assert!(matches!(
        handoff.distinct_execution_strategy(),
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg { target_field, .. }
            if target_field == "rank"
    ));
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "global grouped DISTINCT AVG strategy lowering should not project scalar DISTINCT policy violations",
    );
}

#[test]
fn grouped_executor_handoff_projects_scalar_distinct_policy_violation_for_executor() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let mut grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );
    grouped.scalar_plan_mut().distinct = true;

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        Some(GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired),
        "grouped handoff should project scalar DISTINCT policy violations for executor boundaries",
    );
    assert!(
        matches!(
            handoff.distinct_execution_strategy(),
            GroupedDistinctExecutionStrategy::None
        ),
        "scalar DISTINCT policy violations should remain independent from global DISTINCT execution strategy lowering",
    );
}

#[test]
fn grouped_executor_handoff_preserves_having_clause_contract() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan_with_having(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    let having = handoff
        .having()
        .expect("grouped handoff should preserve having clause payload");
    assert_eq!(having.clauses().len(), 1);
    assert!(matches!(
        having.clauses()[0].symbol(),
        GroupHavingSymbol::AggregateIndex(0)
    ));
    assert_eq!(having.clauses()[0].op(), CompareOp::Gt);
    assert_eq!(having.clauses()[0].value(), &Value::Uint(1));
}

type GroupedExecutorHandoffSnapshotVector = (
    Vec<String>,
    Vec<(AggregateKind, Option<String>)>,
    Vec<usize>,
    Vec<usize>,
    String,
    String,
    u64,
    u64,
);

fn grouped_executor_handoff_snapshot_vector(
    base: &AccessPlannedQuery,
    group: &GroupSpec,
) -> GroupedExecutorHandoffSnapshotVector {
    let grouped = base.clone().into_grouped(group.clone());
    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    let aggregate_vector = handoff
        .aggregate_projection_specs()
        .iter()
        .map(|aggregate| {
            (
                aggregate.kind(),
                aggregate.target_field().map(str::to_string),
            )
        })
        .collect::<Vec<_>>();

    (
        handoff
            .group_fields()
            .iter()
            .map(|field| field.field().to_string())
            .collect::<Vec<_>>(),
        aggregate_vector,
        handoff.projection_layout().group_field_positions().to_vec(),
        handoff.projection_layout().aggregate_positions().to_vec(),
        format!("{:?}", handoff.grouped_fold_path()),
        format!("{:?}", handoff.distinct_execution_strategy()),
        handoff.execution().max_groups(),
        handoff.execution().max_group_bytes(),
    )
}

#[test]
fn grouped_executor_handoff_contract_matrix_vectors_are_frozen() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped_cases = [
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                    .expect("rank field must resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "tag")
                    .expect("tag field must resolve"),
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                    .expect("rank field must resolve"),
            ],
            aggregates: vec![
                GroupAggregateSpec {
                    kind: AggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                },
                GroupAggregateSpec {
                    kind: AggregateKind::Min,
                    target_field: None,
                    distinct: false,
                },
            ],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        },
    ];

    let actual_vectors: Vec<GroupedExecutorHandoffSnapshotVector> = grouped_cases
        .iter()
        .map(|group| grouped_executor_handoff_snapshot_vector(&base, group))
        .collect();
    let expected_vectors: Vec<GroupedExecutorHandoffSnapshotVector> = vec![
        (
            vec!["rank".to_string()],
            vec![(AggregateKind::Count, None::<String>)],
            vec![0],
            vec![1],
            "CountRowsDedicated".to_string(),
            "None".to_string(),
            u64::MAX,
            u64::MAX,
        ),
        (
            vec!["tag".to_string(), "rank".to_string()],
            vec![
                (AggregateKind::Max, Some("rank".to_string())),
                (AggregateKind::Min, None::<String>),
            ],
            vec![0, 1],
            vec![2, 3],
            "GenericReducers".to_string(),
            "None".to_string(),
            11,
            2048,
        ),
    ];

    assert_eq!(actual_vectors, expected_vectors);
}

#[test]
fn grouped_invalid_spec_does_not_change_scalar_plan_validation_outcome() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base.clone(),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    validate_query_semantics(schema, model, &base)
        .expect("scalar plan validation must not require grouped spec");
    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped validation must enforce grouped spec");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_validation_preserves_scalar_policy_errors_on_base_plan() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    base.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(1),
        offset: 0,
    });
    let grouped = grouped_plan(
        base.clone(),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let scalar_err = validate_query_semantics(schema, model, &base)
        .expect_err("invalid scalar base plan must fail scalar policy validation");
    assert!(is_policy_plan_error(&scalar_err, |inner| matches!(
        inner,
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
    let grouped_err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped validation must preserve scalar base-plan policy failures");
    assert!(is_policy_plan_error(&grouped_err, |inner| matches!(
        inner,
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
}

#[test]
fn grouped_validation_rejects_delete_mode_grouped_shape_as_policy_error() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("delete grouped shape must fail planner policy validation");
    assert!(is_policy_plan_error(&err, |inner| matches!(
        inner,
        PolicyPlanError::DeletePlanWithGrouping
    )));
}

#[test]
fn grouped_projection_expr_compatibility_accepts_group_fields_and_aggregates_with_alias_wrapping() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Field(FieldId::new("rank"))),
                name: Alias::new("group_key"),
            },
            alias: Some(Alias::new("group_key_out")),
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Aggregate(crate::db::count())),
                name: Alias::new("count_alias"),
            },
            alias: None,
        },
    ]);

    validate_group_projection_expr_compatibility(&group, &projection).expect(
        "grouped projection compatibility should allow grouped fields, aliases, and aggregates",
    );
}

#[test]
fn grouped_projection_expr_compatibility_rejects_non_group_field_reference() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("tag"))),
            name: Alias::new("not_grouped"),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection)
        .expect_err("grouped projection compatibility should reject non-group field references");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index } if *index == 0
    )));
}

#[test]
fn grouped_projection_expr_compatibility_rejects_non_group_field_in_mixed_aggregate_expression() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Field(FieldId::new("tag"))),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection).expect_err(
        "mixed expressions must still reject non-group field references outside aggregate nodes",
    );
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index } if *index == 0
    )));
}
