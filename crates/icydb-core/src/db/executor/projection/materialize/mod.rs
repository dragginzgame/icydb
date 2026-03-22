//! Module: db::executor::projection::materialize
//! Responsibility: shared projection materialization helpers that are used by both structural and typed row flows.
//! Does not own: the structural SQL row loop itself or expression evaluation semantics.
//! Boundary: keeps validation, grouped projection materialization, and shared row-walk helpers behind one executor-owned boundary.

mod structural;

use crate::{
    db::query::plan::expr::{Expr, ProjectionField, ProjectionSpec},
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};
#[cfg(all(feature = "sql", test))]
use crate::{
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};

use crate::db::executor::projection::{
    eval::{ProjectionEvalError, eval_expr_grouped, eval_expr_with_slot_reader},
    grouped::GroupedRowView,
};
#[cfg(feature = "sql")]
pub(in crate::db) use structural::execute_sql_projection_rows_for_canister;

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

// Walk one projection spec through one slot-reader boundary so validation and
// row materialization share the same expression-evaluation spine.
#[cfg_attr(not(feature = "sql"), expect(dead_code))]
pub(super) fn visit_projection_values_with_slot_reader(
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
