//! Module: db::query::plan::validate::grouped::structure::tests
//! Covers grouped structure validation and grouped-plan shape rules.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::{
    db::{
        predicate::CompareOp,
        query::plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
            GroupHavingSymbol, GroupSpec, GroupedExecutionConfig,
            expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
            validate::{ExprPlanError, PlanUserError},
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
    "query::plan::validate::grouped::structure::idx_empty",
    "query::plan::validate::grouped::structure::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = GroupStructureValidateEntity,
    id = Ulid,
    entity_name = "GroupStructureValidateEntity",
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
    <GroupStructureValidateEntity as EntitySchema>::MODEL
}

fn schema() -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(model())
}

fn grouped_spec() -> GroupSpec {
    GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model(), "team").expect("group field slot should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
    }
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
fn grouped_structure_rejects_having_group_field_symbol_outside_group_keys() {
    let group = grouped_spec();
    let projection = ProjectionSpec::default();
    let having = GroupHavingSpec {
        clauses: vec![GroupHavingClause {
            symbol: GroupHavingSymbol::GroupField(
                FieldSlot::resolve(model(), "region").expect("field slot should resolve"),
            ),
            op: CompareOp::Eq,
            value: Value::Text("eu".to_string()),
        }],
    };

    let err = validate_group_structure(schema(), model(), &group, &projection, Some(&having))
        .expect_err("HAVING group-field symbols outside GROUP BY keys must fail in planner");

    assert!(matches!(
        err,
        PlanError::User(inner)
            if matches!(
                inner.as_ref(),
                PlanUserError::Group(group)
                    if matches!(
                        group.as_ref(),
                        GroupPlanError::HavingNonGroupFieldReference { index: 0, .. }
                    )
            )
    ));
}

#[test]
fn grouped_structure_rejects_having_aggregate_index_out_of_bounds() {
    let group = grouped_spec();
    let projection = ProjectionSpec::default();
    let having = GroupHavingSpec {
        clauses: vec![GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(1),
            op: CompareOp::Gt,
            value: Value::Uint(5),
        }],
    };

    let err = validate_group_structure(schema(), model(), &group, &projection, Some(&having))
        .expect_err("HAVING aggregate symbols outside declared aggregate range must fail");

    assert!(matches!(
        err,
        PlanError::User(inner)
            if matches!(
                inner.as_ref(),
                PlanUserError::Group(group)
                    if matches!(
                        group.as_ref(),
                        GroupPlanError::HavingAggregateIndexOutOfBounds {
                            index: 0,
                            aggregate_index: 1,
                            aggregate_count: 1,
                        }
                    )
            )
    ));
}
