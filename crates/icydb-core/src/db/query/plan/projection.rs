//! Module: query::plan::projection
//! Responsibility: planner-owned projection intent lowering into canonical semantic shape.
//! Does not own: expression evaluation or executor output materialization.
//! Boundary: converts logical query intent into `ProjectionSpec`.

use crate::db::{
    QueryError,
    query::{
        builder::aggregate::AggregateExpr,
        plan::{
            GroupAggregateSpec, LogicalPlan,
            expr::{
                Expr, FieldId, FieldPath, ProjectionField, ProjectionSelection, ProjectionSpec,
            },
            semantics::group_aggregate_spec_expr,
        },
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Lower one accepted-schema logical plan into canonical projection semantics.
pub(in crate::db::query) fn lower_projection_intent_with_schema(
    schema: &SchemaInfo,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
    work: &PreparationWork<'_>,
) -> Result<ProjectionSpec, QueryError> {
    let fields = match logical {
        LogicalPlan::Scalar(_) => match selection {
            ProjectionSelection::All => work.copy_slice(
                &schema.field_names_in_slot_order_for_preparation(work)?,
                |field| {
                    Ok(direct_field_projection(FieldId::new(
                        work.copy_text(field)?,
                    )))
                },
            )?,
            ProjectionSelection::Fields(field_ids) => work.copy_slice(field_ids, |field| {
                Ok(direct_field_projection(FieldId::new(
                    work.copy_text(field.as_str())?,
                )))
            })?,
            ProjectionSelection::Exprs(fields) => {
                work.copy_slice(fields, |field| field.copy_for_preparation(work))?
            }
        },
        LogicalPlan::Grouped(grouped) => match selection {
            ProjectionSelection::Exprs(fields) => {
                work.copy_slice(fields, |field| field.copy_for_preparation(work))?
            }
            ProjectionSelection::All | ProjectionSelection::Fields(_) => {
                let group = &grouped.group;
                let mut fields = work.vec_with_capacity(
                    group
                        .group_fields
                        .len()
                        .saturating_add(group.aggregates.len()),
                )?;
                for field in group.group_fields.iter() {
                    work.charge(Resource::PredicateExpressionSteps, 1)?;
                    let expr = if let Some(path) = field.as_scalar_path() {
                        Expr::FieldPath(FieldPath::new(
                            work.copy_text(path.path().root().as_str())?,
                            work.copy_slice(path.path().segments(), |segment| {
                                work.copy_text(segment)
                            })?,
                        ))
                    } else {
                        Expr::Field(FieldId::new(work.copy_text(field.field())?))
                    };
                    fields.push(ProjectionField::Scalar { expr, alias: None });
                }
                for aggregate in &group.aggregates {
                    fields.push(aggregate_projection(AggregateExpr::from_shape(
                        aggregate
                            .shape()
                            .copy_for_preparation(work)?
                            .with_raw_distinct(aggregate.semantic_distinct()),
                    )));
                }
                fields
            }
        },
    };
    Ok(ProjectionSpec::new(fields))
}

/// Lower one already-validated global aggregate output field list into the
/// canonical planner-owned projection semantic shape.
#[must_use]
pub(in crate::db) const fn lower_global_aggregate_projection(
    fields: Vec<ProjectionField>,
) -> ProjectionSpec {
    ProjectionSpec::new(fields)
}

/// Unique consuming-reader slots, followed by duplicate-preserving raw-row slots.
type DirectProjectionLayouts = (Option<Vec<usize>>, Option<Vec<usize>>);

/// Resolve both direct layouts from the already-lowered projection in one pass.
/// Grouped/computed projections are unavailable, separately from budget failure.
pub(in crate::db) fn lower_direct_projection_layouts_with_schema(
    schema: &SchemaInfo,
    logical: &LogicalPlan,
    projection: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<DirectProjectionLayouts, QueryError> {
    if matches!(logical, LogicalPlan::Grouped(_)) {
        return Ok((None, None));
    }
    let mut slots = work.vec_with_capacity(projection.len())?;
    let mut unique = true;
    for field in projection.fields() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(name) = field.direct_field_name() else {
            return Ok((None, None));
        };
        work.charge(Resource::PredicateExpressionSteps, name.len() as u64)?;
        let Some(slot) = schema.field_slot_index(name) else {
            return Ok((None, None));
        };
        // Once a duplicate disables the consuming-reader layout, only the
        // raw-row layout remains; do not repeat unnecessary uniqueness checks.
        if unique {
            for previous in &slots {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                if *previous == slot {
                    unique = false;
                    break;
                }
            }
        }
        slots.push(slot);
    }
    let consuming = if unique {
        Some(work.copy_slice(&slots, |slot| {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            Ok(*slot)
        })?)
    } else {
        None
    };

    Ok((consuming, Some(slots)))
}

/// Lower one logical plan into the identity projection used by hash/fingerprint
/// surfaces when a full schema model is not available at the call boundary.
#[must_use]
pub(in crate::db::query) fn lower_projection_identity(
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => match selection {
            ProjectionSelection::All => ProjectionSpec::new(vec![direct_field_projection(
                FieldId::new("__icydb_scalar_projection_default__"),
            )]),
            ProjectionSelection::Fields(field_ids) => ProjectionSpec::new(
                field_ids
                    .iter()
                    .cloned()
                    .map(direct_field_projection)
                    .collect(),
            ),
            ProjectionSelection::Exprs(fields) => ProjectionSpec::new(fields.clone()),
        },
        LogicalPlan::Grouped(grouped) => lower_grouped_projection(
            &grouped.group.group_fields,
            grouped.group.aggregates.as_slice(),
        ),
    }
}

/// Lower grouped plans to one explicit projection of grouped keys followed by
/// grouped aggregates, preserving declaration order.
fn lower_grouped_projection(
    group_fields: &crate::db::query::plan::GroupFieldSet,
    aggregates: &[GroupAggregateSpec],
) -> ProjectionSpec {
    let mut fields = Vec::with_capacity(group_fields.len().saturating_add(aggregates.len()));
    for group_field in group_fields.iter() {
        fields.push(ProjectionField::Scalar {
            expr: group_field.projection_expr(),
            alias: None,
        });
    }
    for aggregate in aggregates {
        fields.push(aggregate_projection(group_aggregate_spec_expr(aggregate)));
    }

    ProjectionSpec::new(fields)
}

// Build one direct-field projection node so scalar, grouped, and identity
// lowering keep the same projection-field shape in one place.
const fn direct_field_projection(field_id: FieldId) -> ProjectionField {
    ProjectionField::Scalar {
        expr: Expr::Field(field_id),
        alias: None,
    }
}

// Build one grouped aggregate projection node so grouped projection lowering
// does not restate the scalar aggregate projection envelope inline.
const fn aggregate_projection(aggregate_expr: AggregateExpr) -> ProjectionField {
    ProjectionField::Scalar {
        expr: Expr::Aggregate(aggregate_expr),
        alias: None,
    }
}
