//! Module: executor::projection::eval
//! Responsibility: shared projection expression error taxonomy and scalar evaluation helpers.
//! Does not own: generic test evaluators, expression type inference, or planner semantic validation policy.
//! Boundary: production execution stays on compiled scalar programs while tests build any generic evaluators locally.

mod operators;
mod scalar;

use crate::error::InternalError;
#[cfg(test)]
use crate::value::Value;
use thiserror::Error as ThisError;

pub(in crate::db) use crate::db::query::plan::expr::ScalarProjectionExpr;
#[cfg(test)]
pub(in crate::db::executor) use operators::{eval_binary_expr, eval_unary_expr};
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_canonical_scalar_projection_expr;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use scalar::eval_canonical_scalar_projection_expr_with_required_value_reader_cow;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr;
#[cfg(test)]
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_reader;
pub(in crate::db::executor) use scalar::eval_scalar_projection_expr_with_value_ref_reader;

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

    #[cfg(test)]
    #[error("projection expression cannot evaluate aggregate '{kind}' in scalar row context")]
    AggregateNotEvaluable { kind: String },

    #[cfg(test)]
    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

    #[cfg(test)]
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
