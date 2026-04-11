//! Module: db::executor::projection::eval::scalar
//! Responsibility: compiled scalar-only projection expression evaluation on top of the shared scalar-expression seam.
//! Does not own: grouped projection execution, generic `Expr` evaluation, or planner validation.
//! Boundary: structural projection materialization calls into this file when a projection stays entirely on the scalar seam.

#[cfg(test)]
use crate::db::executor::projection::eval::operators;
#[cfg(test)]
use crate::db::scalar_expr::scalar_expr_value_into_value;
#[cfg(test)]
use crate::db::{data::CanonicalSlotReader, scalar_expr::eval_canonical_scalar_value_program};
#[cfg(test)]
use crate::db::{data::SlotReader, scalar_expr::eval_scalar_value_program};
use crate::{
    db::{
        executor::projection::eval::ProjectionEvalError,
        query::plan::expr::{ScalarProjectionExpr, ScalarProjectionField},
    },
    error::InternalError,
    value::Value,
};
#[cfg(any(test, feature = "sql"))]
use std::borrow::Cow;

#[cfg(test)]
/// Evaluate one compiled scalar projection expression against one slot reader.
pub(in crate::db::executor) fn eval_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &mut dyn SlotReader,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| eval_scalar_projection_field(field, slots),
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression against one canonical
/// slot reader where declared slots must already exist.
#[cfg(test)]
pub(in crate::db::executor) fn eval_canonical_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| eval_canonical_scalar_projection_field(field, slots),
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression through one canonical
/// reader that can borrow decoded slot values from the structural row cache.
/// This keeps repeated field references on the structural SQL path from
/// cloning cached `Value`s before an operator actually needs ownership.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_canonical_scalar_projection_expr_with_required_value_reader_cow<
    'a,
>(
    expr: &'a ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
) -> Result<Cow<'a, Value>, InternalError> {
    #[cfg(not(test))]
    {
        eval_scalar_projection_expr_core(expr, &mut |field| read_slot(field.slot()))
    }

    #[cfg(test)]
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| read_slot(field.slot()),
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
}

/// Evaluate one compiled scalar projection expression through one pure value
/// reader that resolves slots directly into runtime `Value`s.
#[cfg(test)]
pub(in crate::db::executor) fn eval_scalar_projection_expr_with_value_reader(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let Some(value) = read_slot(field.slot()) else {
                return Err(missing_field_value(field));
            };

            Ok(Cow::Owned(value))
        },
        &mut |err| err,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression through one borrowed
/// value reader and materialize the final runtime `Value`.
pub(in crate::db::executor) fn eval_scalar_projection_expr_with_value_ref_reader<'a>(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Value, ProjectionEvalError> {
    #[cfg(not(test))]
    let value = eval_scalar_projection_expr_core(expr, &mut |field| {
        let Some(value) = read_slot(field.slot()) else {
            return Err(missing_field_value(field));
        };

        Ok(Cow::Borrowed(value))
    });

    #[cfg(test)]
    let value = eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let Some(value) = read_slot(field.slot()) else {
                return Err(missing_field_value(field));
            };

            Ok(Cow::Borrowed(value))
        },
        &mut |err| err,
    );

    value.map(Cow::into_owned)
}

#[cfg(not(test))]
fn eval_scalar_projection_expr_core<'a, E>(
    expr: &'a ScalarProjectionExpr,
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Cow<'a, Value>, E>,
) -> Result<Cow<'a, Value>, E> {
    match expr {
        ScalarProjectionExpr::Field(field) => eval_field(field),
    }
}

#[cfg(test)]
fn eval_scalar_projection_expr_core<'a, E>(
    expr: &'a ScalarProjectionExpr,
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Cow<'a, Value>, E>,
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Cow<'a, Value>, E> {
    eval_scalar_projection_expr_core_impl(expr, eval_field, map_projection_error)
}

#[cfg(test)]
fn eval_scalar_projection_expr_core_impl<'a, E>(
    expr: &'a ScalarProjectionExpr,
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Cow<'a, Value>, E>,
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Cow<'a, Value>, E> {
    match expr {
        ScalarProjectionExpr::Field(field) => eval_field(field),
        ScalarProjectionExpr::Literal(value) => Ok(Cow::Borrowed(value)),
        ScalarProjectionExpr::Unary { op, expr } => {
            let operand =
                eval_scalar_projection_expr_core_impl(expr, eval_field, map_projection_error)?;

            operators::eval_unary_expr(*op, operand.as_ref())
                .map(Cow::Owned)
                .map_err(map_projection_error)
        }
        ScalarProjectionExpr::Binary { op, left, right } => {
            let left =
                eval_scalar_projection_expr_core_impl(left, eval_field, map_projection_error)?;
            let right =
                eval_scalar_projection_expr_core_impl(right, eval_field, map_projection_error)?;

            operators::eval_binary_expr(*op, left.as_ref(), right.as_ref())
                .map(Cow::Owned)
                .map_err(map_projection_error)
        }
    }
}

// Build one stable missing-field diagnostic from one compiled scalar field.
fn missing_field_value(field: &ScalarProjectionField) -> ProjectionEvalError {
    ProjectionEvalError::MissingFieldValue {
        field: field.field().to_string(),
        index: field.slot(),
    }
}

#[cfg(test)]
fn eval_scalar_projection_field(
    field: &ScalarProjectionField,
    slots: &mut dyn SlotReader,
) -> Result<Cow<'static, Value>, InternalError> {
    // Scalar fields keep the fast scalar-expression seam in tests. Non-scalar
    // projected fields still need to compile for planner/executor contract
    // tests, so fall back to slot-contract decoding there.
    let value = if let Some(program) = field.program() {
        let Some(value) = eval_scalar_value_program(program, slots)? else {
            return Err(missing_field_value(field).into_invalid_logical_plan_internal_error());
        };

        scalar_expr_value_into_value(value)
    } else {
        let Some(value) = slots.get_value(field.slot())? else {
            return Err(missing_field_value(field).into_invalid_logical_plan_internal_error());
        };

        value
    };

    Ok(Cow::Owned(value))
}

#[cfg(test)]
fn eval_canonical_scalar_projection_field(
    field: &ScalarProjectionField,
    slots: &dyn CanonicalSlotReader,
) -> Result<Cow<'static, Value>, InternalError> {
    let value = if let Some(program) = field.program() {
        scalar_expr_value_into_value(eval_canonical_scalar_value_program(program, slots)?)
    } else {
        slots.required_value_by_contract(field.slot())?
    };

    Ok(Cow::Owned(value))
}
