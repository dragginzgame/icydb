//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while test
//! helpers exercise the same compiled evaluator paths directly.

mod operators;
mod scalar;

use crate::{
    db::{
        data::CanonicalSlotReader,
        query::plan::{EffectiveRuntimeFilterProgram, expr::admit_true_only_boolean_value},
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

pub(in crate::db) use crate::db::query::plan::expr::{ProjectionEvalError, ScalarProjectionExpr};
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_reader;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_ref_reader;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use scalar::{
    eval_canonical_scalar_filter_expr_with_required_slot_reader_cow,
    eval_canonical_scalar_projection_expr_with_required_slot_reader_cow,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    try_eval_field_path_literal_filter_expr,
};

// Evaluate one compiled scalar boolean filter expression through one required
// borrowed slot reader and collapse it through the shared TRUE-only admission
// boundary used by WHERE-style residual filtering.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_scalar_filter_expr_with_required_value_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<std::borrow::Cow<'a, Value>, InternalError>,
) -> Result<bool, InternalError> {
    let value =
        eval_canonical_scalar_projection_expr_with_required_value_reader_cow(expr, read_slot)?;

    admit_true_only_boolean_value(value.as_ref(), |found| {
        InternalError::query_invalid_logical_plan(format!(
            "scalar filter expression produced non-boolean value {found:?}",
        ))
    })
}

// Evaluate one compiled scalar boolean filter expression through one canonical
// slot reader so field-path leaves can borrow raw value-storage bytes during
// scan-time filtering.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_scalar_filter_expr_with_required_slot_reader(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    if let Some(admitted) = try_eval_field_path_literal_filter_expr(expr, slots)? {
        return Ok(admitted);
    }

    let Some(value) = eval_canonical_scalar_filter_expr_with_required_slot_reader_cow(expr, slots)?
    else {
        return Ok(false);
    };

    admit_true_only_boolean_value(value.as_ref(), |found| {
        InternalError::query_invalid_logical_plan(format!(
            "scalar filter expression produced non-boolean value {found:?}",
        ))
    })
}

// Evaluate one planner-selected effective runtime filter program directly
// against a canonical structural slot reader. This is the scan-time lane used
// before raw rows are reduced to retained `Value` slots, so field-path filters
// can resolve nested value-storage payloads without planner/index pushdown.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_slot_reader(
    filter_program: &EffectiveRuntimeFilterProgram,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    filter_program.eval_with_slot_reader(slots)
}

// Evaluate one planner-selected effective runtime filter program through one
// borrowed slot reader. Predicate-backed filters stay on the predicate hot
// loop while expression-backed residual filters reuse the shared scalar
// TRUE-only boolean admission boundary.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_value_ref_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    filter_program.eval_with_value_ref_reader(read_slot, missing_slot_context)
}

// Evaluate one planner-selected effective runtime filter program through one
// slot reader that may return borrowed or owned values. This keeps predicate
// evaluation on the canonical cow-reader path while letting expression-backed
// residual filters reuse the same TRUE-only boolean admission seam.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_effective_runtime_filter_program_with_value_cow_reader<'a, F>(
    filter_program: &EffectiveRuntimeFilterProgram,
    read_slot: &mut F,
    missing_slot_context: &str,
) -> Result<bool, InternalError>
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    filter_program.eval_with_value_cow_reader(read_slot, missing_slot_context)
}
