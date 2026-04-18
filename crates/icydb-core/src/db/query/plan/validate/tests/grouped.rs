//! Module: query::plan::validate::tests::grouped
//! Covers grouped validation policy and cursor behavior owned by the planner validate boundary.
//! Does not own: purely local helper invariants inside grouped leaf modules.
//! Boundary: validates grouped planner rejection/acceptance behavior across grouped validate slices.

use crate::{
    db::{
        predicate::{CompareOp, MissingRowPolicy},
        query::plan::{
            AggregateKind, DeleteSpec, FieldSlot, GroupAggregateSpec, GroupHavingClause,
            GroupHavingSymbol, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan,
            OrderDirection, OrderSpec, OrderTerm, PageSpec, QueryMode, ScalarPlan,
            expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
            grouped_having_clause_expr_for_group,
            validate::{
                ExprPlanError, GroupPlanError, PlanError, PlanPolicyError, PlanUserError,
                grouped::{
                    validate_group_cursor_constraints, validate_group_policy,
                    validate_group_projection_expr_compatibility, validate_group_structure,
                    validate_projection_expr_types,
                },
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::generated(
    "query::plan::validate::tests::grouped::idx_empty",
    "query::plan::validate::tests::grouped::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = GroupedPolicyValidateEntity,
    id = Ulid,
    entity_name = "GroupedPolicyValidateEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("team", FieldKind::Text),
        ("region", FieldKind::Text),
        ("score", FieldKind::Uint),
    ],
    indexes = [&EMPTY_INDEX],
}

fn model() -> &'static EntityModel {
    <GroupedPolicyValidateEntity as EntitySchema>::MODEL
}

fn grouped_spec() -> GroupSpec {
    GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "team").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    }
}

fn grouped_spec_with_avg_score() -> GroupSpec {
    GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "team").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("score".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    }
}

fn scalar_with_group_order(order_fields: Vec<OrderTerm>) -> ScalarPlan {
    ScalarPlan {
        mode: QueryMode::Load(LoadSpec {
            limit: Some(10),
            offset: 0,
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: order_fields,
        }),
        distinct: false,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    }
}

fn scalar_plan(distinct: bool) -> ScalarPlan {
    ScalarPlan {
        mode: QueryMode::Load(LoadSpec {
            limit: None,
            offset: 0,
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![crate::db::query::plan::OrderTerm::field(
                "id",
                OrderDirection::Asc,
            )],
        }),
        distinct,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    }
}

fn schema() -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(model())
}

fn having_compare(symbol: GroupHavingSymbol, op: CompareOp, value: Value) -> GroupHavingClause {
    GroupHavingClause { symbol, op, value }
}

fn having_expr(group: &GroupSpec, clause: GroupHavingClause) -> Expr {
    grouped_having_clause_expr_for_group(group, &clause)
        .expect("grouped HAVING test clause should resolve against grouped aggregate context")
}

fn is_group_policy_error(err: &PlanError, predicate: impl FnOnce(&GroupPlanError) -> bool) -> bool {
    match err {
        PlanError::Policy(inner) => match inner.as_ref() {
            PlanPolicyError::Group(group) => predicate(group.as_ref()),
            PlanPolicyError::Policy(_) => false,
        },
        PlanError::User(_) | PlanError::Cursor(_) => false,
    }
}

fn is_group_user_error(err: &PlanError, predicate: impl FnOnce(&GroupPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Group(group) => predicate(group.as_ref()),
            PlanUserError::PredicateInvalid(_)
            | PlanUserError::Order(_)
            | PlanUserError::Access(_)
            | PlanUserError::Expr(_) => false,
        },
        PlanError::Policy(_) | PlanError::Cursor(_) => false,
    }
}

fn is_expr_user_error(err: &PlanError, predicate: impl FnOnce(&ExprPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Expr(expr) => predicate(expr.as_ref()),
            PlanUserError::PredicateInvalid(_)
            | PlanUserError::Order(_)
            | PlanUserError::Access(_)
            | PlanUserError::Group(_) => false,
        },
        PlanError::Policy(_) | PlanError::Cursor(_) => false,
    }
}

#[test]
fn grouped_order_requires_limit_in_planner_cursor_policy() {
    let logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "team",
        OrderDirection::Asc,
    )]);
    let group = grouped_spec();

    let err = validate_group_cursor_constraints(&logical, &group)
        .expect_err("grouped ORDER BY without LIMIT must fail in planner cursor policy");

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderRequiresLimit
    )));
}

#[test]
fn grouped_order_prefix_must_align_with_group_keys_in_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "id",
        OrderDirection::Asc,
    )]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = grouped_spec();

    let err = validate_group_cursor_constraints(&logical, &group)
        .expect_err("grouped ORDER BY not prefixed by GROUP BY keys must fail in planner");

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
    )));
}

#[test]
fn grouped_order_prefix_alignment_with_limit_passes_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![
        crate::db::query::plan::OrderTerm::field("team", OrderDirection::Asc),
        crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
    ]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = grouped_spec();

    validate_group_cursor_constraints(&logical, &group).expect(
        "grouped ORDER BY with LIMIT and group-key-aligned prefix should pass planner policy",
    );
}

#[test]
fn grouped_additive_group_key_order_with_limit_passes_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "score + 1",
        OrderDirection::Asc,
    )]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "score").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    };

    validate_group_cursor_constraints(&logical, &group).expect(
        "grouped ORDER BY additive offsets over grouped keys should pass planner cursor policy",
    );
}

#[test]
fn grouped_subtractive_group_key_order_with_limit_passes_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "score - 2",
        OrderDirection::Asc,
    )]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "score").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    };

    validate_group_cursor_constraints(&logical, &group).expect(
        "grouped ORDER BY subtractive offsets over grouped keys should pass planner cursor policy",
    );
}

#[test]
fn grouped_non_preserving_computed_order_stays_fail_closed_in_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "score + score",
        OrderDirection::Asc,
    )]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "score").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig {
            max_groups: 128,
            max_group_bytes: 8 * 1024,
        },
    };

    let err = validate_group_cursor_constraints(&logical, &group).expect_err(
        "grouped ORDER BY expressions that do not preserve grouped-key order must stay fail-closed",
    );

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderExpressionNotAdmissible { term } if term == "score + score"
    )));
}

#[test]
fn grouped_aggregate_order_with_limit_passes_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![
        crate::db::query::plan::OrderTerm::field("AVG(score)", OrderDirection::Desc),
        crate::db::query::plan::OrderTerm::field("team", OrderDirection::Asc),
    ]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });

    validate_group_cursor_constraints(&logical, &grouped_spec_with_avg_score()).expect(
        "aggregate-driven grouped ORDER BY with LIMIT should reserve the bounded Top-K lane",
    );
}

#[test]
fn grouped_aggregate_order_with_offset_stays_rejected_in_planner_cursor_policy() {
    let mut logical = scalar_with_group_order(vec![crate::db::query::plan::OrderTerm::field(
        "AVG(score)",
        OrderDirection::Desc,
    )]);
    logical.page = Some(PageSpec {
        limit: Some(10),
        offset: 1,
    });

    let err = validate_group_cursor_constraints(&logical, &grouped_spec_with_avg_score())
        .expect_err("aggregate-driven grouped ORDER BY with OFFSET must stay fail-closed");

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderOffsetNotSupported
    )));
}

#[test]
fn grouped_distinct_without_adjacency_proof_fails_in_planner_policy() {
    let err = validate_group_policy(schema(), &scalar_plan(true), &grouped_spec(), None)
        .expect_err("grouped DISTINCT without adjacency proof must fail in planner policy");

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctAdjacencyEligibilityRequired
    )));
}

#[test]
fn grouped_distinct_with_having_fails_in_planner_policy() {
    let having = having_compare(
        GroupHavingSymbol::AggregateIndex(0),
        CompareOp::Gt,
        Value::Uint(1),
    );

    let err = validate_group_policy(
        schema(),
        &scalar_plan(true),
        &grouped_spec(),
        Some(&having_expr(&grouped_spec(), having)),
    )
    .expect_err("grouped DISTINCT + HAVING must fail in planner policy");

    assert!(is_group_policy_error(&err, |inner| matches!(
        inner,
        GroupPlanError::DistinctHavingUnsupported
    )));
}

#[test]
fn grouped_non_distinct_shape_passes_planner_distinct_policy_gate() {
    validate_group_policy(schema(), &scalar_plan(false), &grouped_spec(), None)
        .expect("non-distinct grouped shapes should pass planner distinct policy gate");
}

#[test]
fn grouped_policy_allows_widened_having_exprs_on_shared_post_aggregate_seam() {
    let expr = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Gt,
        left: Box::new(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        }),
        right: Box::new(Expr::Literal(Value::Uint(5))),
    };

    validate_group_policy(schema(), &scalar_plan(false), &grouped_spec(), Some(&expr))
        .expect("widened grouped HAVING expressions should stay admissible at planner-policy time");
}

#[test]
fn grouped_policy_tests_track_planner_logical_mode_contract() {
    // Keep grouped-policy tests compile-time linked to logical mode contracts.
    let _ = LogicalPlan::Scalar(ScalarPlan {
        mode: QueryMode::Delete(DeleteSpec {
            limit: Some(1),
            offset: 0,
        }),
        predicate: None,
        order: None,
        distinct: false,
        delete_limit: None,
        page: None,
        consistency: MissingRowPolicy::Ignore,
    });
}

#[test]
fn grouped_structure_rejects_projection_expr_referencing_non_group_field() {
    let group = grouped_spec();
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("score")),
        alias: None,
    }]);

    let err = validate_group_structure(schema(), model(), &group, &projection, None)
        .expect_err("projection references outside GROUP BY keys must fail in planner");

    assert!(is_expr_user_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index: 0 }
    )));
}

#[test]
fn grouped_structure_rejects_having_group_field_symbol_outside_group_keys() {
    let group = grouped_spec();
    let projection = ProjectionSpec::default();
    let having = having_compare(
        GroupHavingSymbol::GroupField(
            FieldSlot::resolve(model(), "region").expect("field slot should resolve"),
        ),
        CompareOp::Eq,
        Value::Text("eu".to_string()),
    );

    let err = validate_group_structure(
        schema(),
        model(),
        &group,
        &projection,
        Some(&having_expr(&group, having)),
    )
    .expect_err("HAVING group-field symbols outside GROUP BY keys must fail in planner");

    assert!(is_group_user_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingNonGroupFieldReference { index: 0, .. }
    )));
}

#[test]
fn grouped_structure_rejects_having_aggregate_index_out_of_bounds() {
    let group = grouped_spec();
    let projection = ProjectionSpec::default();
    let having = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Gt,
        left: Box::new(Expr::Aggregate(crate::db::sum("score"))),
        right: Box::new(Expr::Literal(Value::Uint(5))),
    };

    let err = validate_group_structure(schema(), model(), &group, &projection, Some(&having))
        .expect_err("HAVING aggregate expressions outside declared aggregate set must fail");

    assert!(is_group_user_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingAggregateIndexOutOfBounds {
            index: 0,
            aggregate_count: 1,
            ..
        }
    )));
}

#[test]
fn grouped_projection_compatibility_accepts_alias_wrapped_group_field() {
    let group = grouped_spec();
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("team"))),
            name: crate::db::query::plan::expr::Alias::new("team_alias"),
        },
        alias: None,
    }]);

    validate_group_projection_expr_compatibility(&group, &projection)
        .expect("alias-wrapped group fields must remain compatible");
}

#[test]
fn grouped_projection_compatibility_rejects_binary_expr_with_non_group_field() {
    let group = grouped_spec();
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("team"))),
            right: Box::new(Expr::Field(FieldId::new("score"))),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection)
        .expect_err("binary expressions referencing non-group fields must fail in planner");

    assert!(is_expr_user_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index: 0 }
    )));
}

#[test]
fn projection_expr_type_validation_rejects_unknown_fields() {
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("unknown")),
        alias: None,
    }]);

    let err = validate_projection_expr_types(schema(), &projection)
        .expect_err("expression typing must fail for unknown schema fields");

    assert!(is_expr_user_error(&err, |inner| matches!(
        inner,
        ExprPlanError::UnknownExprField { field } if field == "unknown"
    )));
}
