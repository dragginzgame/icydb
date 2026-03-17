//! Module: db::executor::projection::materialize
//! Responsibility: module-local ownership and contracts for db::executor::projection::materialize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutablePlan, pipeline::contracts::LoadExecutor},
        query::plan::expr::{Expr, ProjectionField, ProjectionSpec},
        response::{ProjectedRow, ProjectionResponse},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

use crate::db::executor::projection::{
    eval::{ProjectionEvalError, eval_expr_grouped, eval_expr_with_slot_reader},
    grouped::GroupedRowView,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one scalar load plan and return projection-shaped response rows.
    pub(in crate::db) fn execute_projection(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<ProjectionResponse<E>, InternalError> {
        // Phase 1: derive projection semantics from the planned query contract.
        let planned = plan.into_inner();
        let projection = planned.projection_spec(E::MODEL);

        // Phase 2: execute canonical scalar load to preserve existing route/runtime semantics.
        let rows = self.execute(ExecutablePlan::new(planned))?;
        let rows = rows
            .rows()
            .into_iter()
            .map(crate::db::response::Row::into_parts)
            .collect::<Vec<_>>();

        // Phase 3: materialize projection payloads in declaration order.
        let projected = project_rows_from_projection::<E>(&projection, rows.as_slice())
            .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;

        Ok(ProjectionResponse::new(projected))
    }
}

/// Validate projection expressions over one row-domain that can expose values
/// by `(row_index, field_slot)` without forcing typed projection materialization.
pub(in crate::db::executor) fn validate_projection_over_slot_rows(
    model: &EntityModel,
    projection: &ProjectionSpec,
    row_count: usize,
    read_slot_for_row: &mut dyn FnMut(usize, usize) -> Option<Value>,
) -> Result<(), InternalError> {
    if projection_is_model_identity_for_model(model, projection) {
        return Ok(());
    }

    // Phase 1: evaluate every projection expression against each row.
    for row_index in 0..row_count {
        let mut read_slot = |slot| read_slot_for_row(row_index, slot);
        for field in projection.fields() {
            match field {
                ProjectionField::Scalar { expr, .. } => {
                    let _ =
                        eval_expr_with_slot_reader(expr, model, &mut read_slot).map_err(|err| {
                            crate::db::error::query_invalid_logical_plan(err.to_string())
                        })?;
                }
            }
        }
    }

    Ok(())
}

/// Evaluate one grouped projection spec into ordered projected values.
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    projection: &ProjectionSpec,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ProjectionEvalError> {
    let mut projected_values = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                projected_values.push(eval_expr_grouped(expr, grouped_row)?);
            }
        }
    }

    Ok(projected_values)
}

pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        for field in projection.fields() {
            match field {
                ProjectionField::Scalar { expr, .. } => {
                    values.push(eval_expr_with_slot_reader(expr, E::MODEL, &mut read_slot)?);
                }
            }
        }
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

fn projection_is_model_identity_for_model(
    model: &EntityModel,
    projection: &ProjectionSpec,
) -> bool {
    if projection.len() != model.fields.len() {
        return false;
    }

    for (field_model, projected_field) in model.fields.iter().zip(projection.fields()) {
        match projected_field {
            ProjectionField::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } if field_id.as_str() == field_model.name => {}
            ProjectionField::Scalar { .. } => return false,
        }
    }

    true
}
