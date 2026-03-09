use super::*;
use crate::{
    db::{
        predicate::SchemaInfo,
        query::plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupSpec, GroupedExecutionConfig,
            expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
            validate::{ExprPlanError, PlanUserError},
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::new(
    "query::plan::validate::grouped::projection_expr::idx_empty",
    "query::plan::validate::grouped::projection_expr::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = GroupProjectionValidateEntity,
    id = Ulid,
    entity_name = "GroupProjectionValidateEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("team", FieldKind::Text),
        ("score", FieldKind::Uint),
    ],
    indexes = [&EMPTY_INDEX],
}

fn model() -> &'static EntityModel {
    <GroupProjectionValidateEntity as EntitySchema>::MODEL
}

fn schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(model()).expect("schema should validate")
}

fn grouped_spec() -> GroupSpec {
    GroupSpec {
        group_fields: vec![FieldSlot::resolve(model(), "team").expect("field slot should resolve")],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
    }
}

#[test]
fn grouped_projection_compatibility_accepts_alias_wrapped_group_field() {
    let group = grouped_spec();
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("team"))),
            name: Alias::new("team_alias"),
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
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("team"))),
            right: Box::new(Expr::Field(FieldId::new("score"))),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection)
        .expect_err("binary expressions referencing non-group fields must fail in planner");

    assert!(matches!(
        err,
        PlanError::User(inner)
            if matches!(
                inner.as_ref(),
                PlanUserError::Expr(expr)
                    if matches!(
                        expr.as_ref(),
                        ExprPlanError::GroupedProjectionReferencesNonGroupField { index: 0 }
                    )
            )
    ));
}

#[test]
fn projection_expr_type_validation_rejects_unknown_fields() {
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("unknown")),
        alias: None,
    }]);

    let err = validate_projection_expr_types(&schema(), &projection)
        .expect_err("expression typing must fail for unknown schema fields");

    assert!(matches!(
        err,
        PlanError::User(inner)
            if matches!(
                inner.as_ref(),
                PlanUserError::Expr(expr)
                    if matches!(
                        expr.as_ref(),
                        ExprPlanError::UnknownExprField { field } if field == "unknown"
                    )
            )
    ));
}
