//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while test
//! helpers exercise the same compiled evaluator paths directly.

mod operators;
mod scalar;
mod scalar_function;

use crate::error::InternalError;
use crate::value::Value;
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
    eval_builder_expr_for_value_preview, eval_projection_function_call, projection_function_name,
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

// Collapse one evaluated boolean-context value through the shared TRUE-only
// admission boundary used by WHERE-style row filtering, grouped HAVING, and
// aggregate FILTER semantics.
pub(in crate::db) fn collapse_true_only_boolean_admission<E>(
    value: Value,
    invalid: impl FnOnce(Box<Value>) -> E,
) -> Result<bool, E> {
    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(invalid(Box::new(other))),
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
