//! Module: query::plan::projection
//! Responsibility: planner-owned projection intent lowering into canonical semantic shape.
//! Does not own: expression evaluation or executor output materialization.
//! Boundary: converts logical query intent into `ProjectionSpec`.

use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            FieldSlot, GroupAggregateSpec, GroupPlan, LogicalPlan,
            expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
        },
    },
    model::entity::EntityModel,
};

/// Lower one logical plan into the canonical planner-owned projection semantic shape.
#[must_use]
pub(crate) fn lower_projection_intent(
    model: &EntityModel,
    logical: &LogicalPlan,
) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => lower_scalar_projection(model),
        LogicalPlan::Grouped(grouped) => lower_grouped_projection_from_plan(grouped),
    }
}

/// Lower scalar plans to one explicit field projection per declared entity field.
fn lower_scalar_projection(model: &EntityModel) -> ProjectionSpec {
    let fields = model
        .fields
        .iter()
        .map(|field| ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new(field.name)),
            alias: None,
        })
        .collect();

    ProjectionSpec::new(fields)
}

/// Lower one logical plan into the identity projection used by hash/fingerprint
/// surfaces when a full schema model is not available at the call boundary.
#[must_use]
pub(crate) fn lower_projection_identity(logical: &LogicalPlan) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => ProjectionSpec::new(vec![ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("__icydb_scalar_projection_default_v1__")),
            alias: None,
        }]),
        LogicalPlan::Grouped(grouped) => lower_grouped_projection_from_plan(grouped),
    }
}

fn lower_grouped_projection_from_plan(grouped: &GroupPlan) -> ProjectionSpec {
    lower_grouped_projection(
        grouped.group.group_fields.as_slice(),
        grouped.group.aggregates.as_slice(),
    )
}

/// Lower grouped plans to one explicit projection of grouped keys followed by
/// grouped aggregates, preserving declaration order.
fn lower_grouped_projection(
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> ProjectionSpec {
    let mut fields = Vec::with_capacity(group_fields.len().saturating_add(aggregates.len()));
    for group_field in group_fields {
        fields.push(ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new(group_field.field())),
            alias: None,
        });
    }
    for aggregate in aggregates {
        fields.push(ProjectionField::Scalar {
            expr: Expr::Aggregate(lower_group_aggregate_expr(aggregate)),
            alias: None,
        });
    }

    ProjectionSpec::new(fields)
}

/// Lower one grouped aggregate semantic spec into one canonical aggregate expression.
fn lower_group_aggregate_expr(aggregate: &GroupAggregateSpec) -> AggregateExpr {
    AggregateExpr::from_semantic_parts(
        aggregate.kind(),
        aggregate.target_field().map(str::to_string),
        aggregate.distinct(),
    )
}
