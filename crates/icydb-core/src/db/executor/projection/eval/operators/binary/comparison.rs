//! Module: db::executor::projection::eval::operators::binary::comparison
//! Implements binary comparison operators for scalar projection evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError, operators::binary::invalid_binary_operands,
        },
        predicate::{CoercionId, CoercionSpec, compare_eq, compare_order},
        query::plan::expr::BinaryOp,
    },
    value::Value,
};
use std::cmp::Ordering;

pub(super) fn eval_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    let numeric_widen_enabled =
        left.supports_numeric_coercion() || right.supports_numeric_coercion();
    let coercion = if numeric_widen_enabled {
        CoercionSpec::new(CoercionId::NumericWiden)
    } else {
        CoercionSpec::new(CoercionId::Strict)
    };
    let value = match op {
        BinaryOp::Eq => {
            if let Some(are_equal) = compare_eq(left, right, &coercion) {
                are_equal
            } else if !numeric_widen_enabled {
                // Preserve projection behavior for non-numeric cross-variant comparisons.
                left == right
            } else {
                return Err(invalid_binary_operands(op, left, right));
            }
        }
        BinaryOp::Ne => {
            if let Some(are_equal) = compare_eq(left, right, &coercion) {
                !are_equal
            } else if !numeric_widen_enabled {
                left != right
            } else {
                return Err(invalid_binary_operands(op, left, right));
            }
        }
        BinaryOp::Lt => eval_order_comparison(op, left, right, &coercion, Ordering::is_lt)?,
        BinaryOp::Lte => eval_order_comparison(op, left, right, &coercion, Ordering::is_le)?,
        BinaryOp::Gt => eval_order_comparison(op, left, right, &coercion, Ordering::is_gt)?,
        BinaryOp::Gte => eval_order_comparison(op, left, right, &coercion, Ordering::is_ge)?,
        _ => unreachable!("comparison evaluator called with non-comparison operator"),
    };

    Ok(Value::Bool(value))
}

fn eval_order_comparison(
    op: BinaryOp,
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<bool, ProjectionEvalError> {
    let Some(ordering) = compare_order(left, right, coercion) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(predicate(ordering))
}
