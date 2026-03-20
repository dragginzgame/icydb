//! Module: db::executor::projection::materialize
//! Responsibility: module-local ownership and contracts for db::executor::projection::materialize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::{
    db::{
        data::{DataKey, DataRow},
        executor::ExecutablePlan,
        executor::terminal::{RowDecoder, RowLayout},
        response::{ProjectedRow, ProjectionResponse},
    },
    types::Id,
};
use crate::{
    db::{
        executor::pipeline::contracts::LoadExecutor,
        query::plan::expr::{Expr, ProjectionField, ProjectionSpec},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
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
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_projection(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<ProjectionResponse<E>, InternalError> {
        let plan = plan.into_prepared_load_plan();
        let authority = plan.authority();

        // Phase 1: derive projection semantics from the planned query contract.
        let projection = plan.logical_plan().projection_spec(authority.model());

        // Phase 2: execute the scalar path structurally so projection does not
        // rebuild typed entity rows before expression evaluation.
        let prepared = self.prepare_scalar_materialized_boundary(plan)?;
        let page = self.execute_scalar_materialized_page_boundary(prepared)?;

        // Phase 3: materialize projection payloads in declaration order.
        let projected = project_data_rows_from_projection_structural(
            authority.model(),
            &projection,
            page.data_rows(),
        )?;
        let projected = projected
            .into_iter()
            .map(
                |(data_key, values)| -> Result<ProjectedRow<E>, InternalError> {
                    let id = Id::from_key(data_key.try_key::<E>()?);

                    Ok(ProjectedRow::new(id, values))
                },
            )
            .collect::<Result<Vec<_>, InternalError>>()?;

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
        visit_projection_values_with_slot_reader(projection, model, &mut read_slot, &mut |_| {})
            .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;
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

#[cfg(all(feature = "sql", test))]
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
        visit_projection_values_with_slot_reader(
            projection,
            E::MODEL,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

// Walk one projection spec through one slot-reader boundary so validation and
// row materialization share the same expression-evaluation spine.
fn visit_projection_values_with_slot_reader(
    projection: &ProjectionSpec,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), ProjectionEvalError> {
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                on_value(eval_expr_with_slot_reader(expr, model, read_slot)?);
            }
        }
    }

    Ok(())
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

#[cfg(feature = "sql")]
fn project_data_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: &[DataRow],
) -> Result<Vec<(DataKey, Vec<Value>)>, InternalError> {
    let row_layout = RowLayout::from_model(model);
    let row_decoder = RowDecoder::structural();
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: decode each materialized row structurally and evaluate the
    // projection expressions without introducing typed entity rows.
    for (data_key, raw_row) in rows {
        let row = row_decoder.decode(&row_layout, (data_key.clone(), raw_row.clone()))?;
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| row.slot(slot);
        visit_projection_values_with_slot_reader(projection, model, &mut read_slot, &mut |value| {
            values.push(value);
        })
        .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;
        projected_rows.push((data_key.clone(), values));
    }

    Ok(projected_rows)
}
