//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while test
//! helpers exercise the same compiled evaluator paths directly.

mod operators;
mod scalar;
mod scalar_function;

use crate::{
    db::query::plan::{EffectiveRuntimeFilterProgram, expr::collapse_true_only_boolean_admission},
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;
use thiserror::Error as ThisError;

pub(in crate::db) use crate::db::query::plan::expr::ScalarProjectionExpr;
pub(in crate::db) use operators::eval_binary_expr;
pub(in crate::db) use operators::eval_unary_expr;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use scalar::eval_canonical_scalar_projection_expr_with_required_value_reader_cow;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_reader;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_ref_reader;
pub(in crate::db) use scalar_function::{
    eval_builder_expr_for_value_preview, eval_projection_function_call,
};

///
/// ProjectionEvalError
///
/// Pure expression-evaluation failures for scalar projection execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum ProjectionEvalError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

    #[error("projection CASE condition produced non-boolean value {found:?}")]
    InvalidCaseCondition { found: Box<Value> },

    #[error(
        "projection binary operator '{op}' is incompatible with operand values ({left:?}, {right:?})"
    )]
    InvalidBinaryOperands {
        op: String,
        left: Box<Value>,
        right: Box<Value>,
    },

    #[error(
        "grouped projection expression references unknown aggregate expression kind={kind} target_field={target_field:?} distinct={distinct}"
    )]
    UnknownGroupedAggregateExpression {
        kind: String,
        target_field: Option<String>,
        distinct: bool,
    },

    #[error(
        "grouped projection expression references aggregate output index={aggregate_index} but only {aggregate_count} outputs are available"
    )]
    MissingGroupedAggregateValue {
        aggregate_index: usize,
        aggregate_count: usize,
    },

    #[error("projection function '{function}' failed evaluation: {message}")]
    InvalidFunctionCall { function: String, message: String },

    #[error("grouped HAVING expression produced non-boolean value {found:?}")]
    InvalidGroupedHavingResult { found: Box<Value> },
}

impl ProjectionEvalError {
    /// Map one projection evaluation failure into the executor invalid-logical-plan boundary.
    pub(in crate::db) fn into_invalid_logical_plan_internal_error(self) -> InternalError {
        InternalError::query_invalid_logical_plan(self.to_string())
    }

    /// Map one grouped projection evaluation failure into the grouped-output
    /// invalid-logical-plan boundary while preserving grouped context.
    pub(in crate::db::executor) fn into_grouped_projection_internal_error(self) -> InternalError {
        InternalError::query_invalid_logical_plan(format!(
            "grouped projection evaluation failed: {self}",
        ))
    }
}

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

    collapse_true_only_boolean_admission(value.into_owned(), |found| {
        InternalError::query_invalid_logical_plan(format!(
            "scalar filter expression produced non-boolean value {found:?}",
        ))
    })
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
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => {
            Ok(predicate_program.eval_with_slot_value_ref_reader(read_slot))
        }
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_value_reader_cow(filter_expr, &mut |slot| {
                let Some(value) = read_slot(slot) else {
                    return Err(InternalError::query_invalid_logical_plan(format!(
                        "{missing_slot_context} {slot}",
                    )));
                };

                Ok(Cow::Borrowed(value))
            })
        }
    }
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
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => {
            Ok(predicate_program.eval_with_slot_value_cow_reader(read_slot))
        }
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_value_reader_cow(filter_expr, &mut |slot| {
                let Some(value) = read_slot(slot) else {
                    return Err(InternalError::query_invalid_logical_plan(format!(
                        "{missing_slot_context} {slot}",
                    )));
                };

                Ok(value)
            })
        }
    }
}
