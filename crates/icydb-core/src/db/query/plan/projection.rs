//! Module: query::plan::projection
//! Responsibility: planner-owned projection intent lowering into canonical semantic shape.
//! Does not own: expression evaluation or executor output materialization.
//! Boundary: converts logical query intent into `ProjectionSpec`.

use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            FieldSlot, GroupAggregateSpec, GroupPlan, LogicalPlan,
            expr::{
                Expr, FieldId, ProjectionField, ProjectionSelection, ProjectionSpec,
                direct_projection_expr_field_name,
            },
        },
    },
    model::entity::{EntityModel, resolve_field_slot},
};

/// Lower one logical plan into the canonical planner-owned projection semantic shape.
#[must_use]
pub(crate) fn lower_projection_intent(
    model: &EntityModel,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => lower_scalar_projection(model, selection),
        LogicalPlan::Grouped(grouped) => lower_grouped_projection_from_plan(grouped),
    }
}

/// Lower scalar plans to one explicit field projection per declared entity field.
fn lower_scalar_projection(model: &EntityModel, selection: &ProjectionSelection) -> ProjectionSpec {
    let fields = match selection {
        ProjectionSelection::All => model
            .fields
            .iter()
            .map(|field| ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new(field.name)),
                alias: None,
            })
            .collect(),
        ProjectionSelection::Fields(field_ids) => field_ids
            .iter()
            .map(|field_id| ProjectionField::Scalar {
                expr: Expr::Field(field_id.clone()),
                alias: None,
            })
            .collect(),
        ProjectionSelection::Expression(expr) => vec![ProjectionField::Scalar {
            expr: expr.clone(),
            alias: None,
        }],
    };

    ProjectionSpec::new(fields)
}

/// Lower one logical plan into one direct slot projection layout when every
/// output remains a unique canonical field reference.
#[must_use]
pub(crate) fn lower_direct_projection_slots(
    model: &EntityModel,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> Option<Vec<usize>> {
    match logical {
        LogicalPlan::Scalar(_) => lower_scalar_direct_projection_slots(model, selection),
        LogicalPlan::Grouped(_) => None,
    }
}

// Lower one scalar logical plan into a unique direct field-slot layout when
// the projection never leaves canonical field references.
fn lower_scalar_direct_projection_slots(
    model: &EntityModel,
    selection: &ProjectionSelection,
) -> Option<Vec<usize>> {
    match selection {
        ProjectionSelection::All => Some((0..model.fields.len()).collect()),
        ProjectionSelection::Fields(field_ids) => {
            let mut slots = Vec::with_capacity(field_ids.len());

            for field_id in field_ids {
                let slot = resolve_field_slot(model, field_id.as_str())?;
                if slots.iter().any(|existing_slot| *existing_slot == slot) {
                    return None;
                }
                slots.push(slot);
            }

            Some(slots)
        }
        ProjectionSelection::Expression(expr) => {
            let field_name = direct_projection_expr_field_name(expr)?;
            let slot = resolve_field_slot(model, field_name)?;

            Some(vec![slot])
        }
    }
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
