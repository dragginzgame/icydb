use crate::{
    db::{
        executor::load::LoadExecutor,
        query::plan::{
            AccessPlannedQuery,
            expr::{Expr, ProjectionField, ProjectionSpec},
        },
        response::ProjectedRow,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

use crate::db::executor::load::projection::{
    eval::{ExecutionError, eval_expr, eval_expr_grouped},
    grouped::GroupedRowView,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Evaluate scalar projection semantics over materialized rows when the
    /// projection is no longer identity (`SELECT *`).
    pub(in crate::db::executor) fn project_materialized_rows_if_needed(
        plan: &AccessPlannedQuery<E::Key>,
        rows: &[(Id<E>, E)],
    ) -> Result<Option<Vec<ProjectedRow<E>>>, InternalError> {
        let projection = plan.projection_spec(E::MODEL);
        if projection_is_model_identity::<E>(&projection) {
            return Ok(None);
        }

        let projected = project_rows_from_projection::<E>(&projection, rows)
            .map_err(|err| InternalError::query_invalid_logical_plan(err.to_string()))?;

        Ok(Some(projected))
    }
}

/// Evaluate one grouped projection spec into ordered projected values.
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    projection: &ProjectionSpec,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ExecutionError> {
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

pub(in crate::db::executor::load::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ExecutionError>
where
    E: EntityKind + EntityValue,
{
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        for field in projection.fields() {
            match field {
                ProjectionField::Scalar { expr, .. } => {
                    values.push(eval_expr(expr, entity)?);
                }
            }
        }
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

fn projection_is_model_identity<E>(projection: &ProjectionSpec) -> bool
where
    E: EntityKind,
{
    if projection.len() != E::MODEL.fields.len() {
        return false;
    }

    for (field_model, projected_field) in E::MODEL.fields.iter().zip(projection.fields()) {
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
