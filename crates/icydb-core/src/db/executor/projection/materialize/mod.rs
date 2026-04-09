//! Module: db::executor::projection::materialize
//! Responsibility: shared projection materialization helpers that are used by both structural and typed row flows.
//! Does not own: the structural SQL row loop itself or expression evaluation semantics.
//! Boundary: keeps validation, grouped projection materialization, and shared row-walk helpers behind one executor-owned boundary.

#[cfg(feature = "sql")]
mod structural;

use crate::{
    db::query::plan::expr::{
        Expr, ProjectionField, ProjectionSpec, collect_unique_direct_projection_slots,
        projection_field_direct_field_name,
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};
#[cfg(all(feature = "sql", test))]
use crate::{
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};
#[cfg(feature = "sql")]
use std::borrow::Cow;

use crate::db::executor::projection::eval::{
    ProjectionEvalError, ScalarProjectionExpr, compile_scalar_projection_expr,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    eval_expr_with_required_value_reader_cow, eval_expr_with_slot_reader,
    eval_scalar_projection_expr_with_value_reader,
};
#[cfg(all(feature = "sql", any(test, feature = "structural-read-metrics")))]
pub(in crate::db::executor) use structural::record_sql_projection_full_row_decode_materialization;
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
pub use structural::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "sql")]
pub(in crate::db) use structural::{
    execute_sql_projection_rows_for_canister, execute_sql_projection_text_rows_for_canister,
};

///
/// PreparedProjectionPlan
///
/// PreparedProjectionPlan is the executor-owned projection materialization plan
/// shared by typed row projection, slot-row validation, and structural SQL
/// row projection. It keeps the compiled-scalar versus generic evaluation
/// split behind one materialization owner.
///

pub(super) enum PreparedProjectionPlan {
    Generic,
    Scalar(Vec<ScalarProjectionExpr>),
}

/// Validate projection expressions over one row-domain that can expose values
/// by `(row_index, field_slot)` without forcing typed projection materialization.
pub(in crate::db::executor) fn validate_projection_over_slot_rows(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    row_count: usize,
    read_slot_for_row: &mut dyn FnMut(usize, usize) -> Option<Value>,
) -> Result<(), InternalError> {
    if projection_is_model_identity_for_model(model, projection) {
        return Ok(());
    }
    let prepared = prepare_projection_plan(model, projection);

    // Phase 1: evaluate every projection expression against each row.
    for row_index in 0..row_count {
        let mut read_slot = |slot| read_slot_for_row(row_index, slot);
        visit_prepared_projection_values_with_value_reader(
            &prepared,
            projection,
            model,
            &mut read_slot,
            &mut |_| {},
        )
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
    }

    Ok(())
}

/// Resolve one direct field-slot projection layout when every output stays on
/// one unique canonical field reference.
///
/// SQL structural fast paths use this to detect projection shapes that can
/// copy values directly from retained slots without reopening generic scalar
/// expression evaluation.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn direct_projection_slots(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
) -> Option<Vec<usize>> {
    collect_unique_direct_projection_slots(
        model,
        projection
            .fields()
            .map(projection_field_direct_field_name)
            .collect::<Option<Vec<_>>>()?,
    )
}

/// Resolve one direct field-slot projection layout when every output stays on
/// one unique canonical field reference.
///
/// SQL structural fast paths use this to detect projection shapes that can
/// copy values directly from retained slots without reopening generic scalar
/// expression evaluation.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn direct_projection_field_slots(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
) -> Option<Vec<(String, usize)>> {
    let slot_indexes = direct_projection_slots(model, projection)?;
    let mut field_slots = Vec::with_capacity(slot_indexes.len());

    for (field, slot) in projection.fields().zip(slot_indexes) {
        let field_name = projection_field_direct_field_name(field)?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

/// Mark every structural field slot referenced by one projection spec.
///
/// This helper keeps retained-slot SQL materialization explicit: callers can
/// compute the exact slot set needed for projection validation/materialization
/// without widening back to full row-slot images.
pub(in crate::db::executor) fn mark_projection_referenced_slots(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    required_slots: &mut [bool],
) -> Result<(), InternalError> {
    // Phase 1: walk each projection expression and resolve every referenced
    // field leaf into the canonical model slot set.
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                mark_projection_expr_referenced_slots(model, expr, required_slots)?;
            }
        }
    }

    Ok(())
}

// Mark every field leaf referenced by one projection expression.
fn mark_projection_expr_referenced_slots(
    model: &'static EntityModel,
    expr: &Expr,
    required_slots: &mut [bool],
) -> Result<(), InternalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let slot = resolve_field_slot(model, field_name).ok_or_else(|| {
                InternalError::query_invalid_logical_plan(format!(
                    "projection expression references unknown field '{field_name}'",
                ))
            })?;
            if let Some(required) = required_slots.get_mut(slot) {
                *required = true;
            }
        }
        Expr::Literal(_) | Expr::Aggregate(_) => {}
        Expr::Unary { expr, .. } | Expr::Alias { expr, .. } => {
            mark_projection_expr_referenced_slots(model, expr.as_ref(), required_slots)?;
        }
        Expr::Binary { left, right, .. } => {
            mark_projection_expr_referenced_slots(model, left.as_ref(), required_slots)?;
            mark_projection_expr_referenced_slots(model, right.as_ref(), required_slots)?;
        }
    }

    Ok(())
}

#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let prepared = prepare_projection_plan(E::MODEL, projection);
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        visit_prepared_projection_values_with_value_reader(
            &prepared,
            projection,
            E::MODEL,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

pub(super) fn prepare_projection_plan(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
) -> PreparedProjectionPlan {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                let Some(compiled) = compile_scalar_projection_expr(model, expr) else {
                    return PreparedProjectionPlan::Generic;
                };
                compiled_fields.push(compiled);
            }
        }
    }

    PreparedProjectionPlan::Scalar(compiled_fields)
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

// Walk one projection spec through one required-value reader that can borrow
// from the structural row cache until the caller needs an owned output cell.
#[cfg(feature = "sql")]
pub(super) fn visit_projection_values_with_required_value_reader_cow<'a>(
    projection: &ProjectionSpec,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                on_value(
                    eval_expr_with_required_value_reader_cow(expr, model, read_slot)?.into_owned(),
                );
            }
        }
    }

    Ok(())
}

pub(super) fn visit_prepared_projection_values_with_value_reader(
    prepared: &PreparedProjectionPlan,
    projection: &ProjectionSpec,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), ProjectionEvalError> {
    match prepared {
        PreparedProjectionPlan::Generic => {
            visit_projection_values_with_slot_reader(projection, model, read_slot, on_value)
        }
        PreparedProjectionPlan::Scalar(compiled_fields) => {
            for compiled in compiled_fields {
                on_value(eval_scalar_projection_expr_with_value_reader(
                    compiled, read_slot,
                )?);
            }

            Ok(())
        }
    }
}

// Walk one prepared projection plan through one reader that can borrow slot
// values from retained structural rows until an expression needs ownership.
#[cfg(feature = "sql")]
pub(super) fn visit_prepared_projection_values_with_required_value_reader_cow<'a>(
    prepared: &PreparedProjectionPlan,
    projection: &ProjectionSpec,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    match prepared {
        PreparedProjectionPlan::Generic => visit_projection_values_with_required_value_reader_cow(
            projection, model, read_slot, on_value,
        ),
        PreparedProjectionPlan::Scalar(compiled_fields) => {
            for compiled in compiled_fields {
                on_value(
                    eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                        compiled, read_slot,
                    )?
                    .into_owned(),
                );
            }

            Ok(())
        }
    }
}
