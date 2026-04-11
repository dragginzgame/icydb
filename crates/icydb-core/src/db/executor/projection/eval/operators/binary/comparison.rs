//! Module: db::executor::projection::eval::operators::binary::comparison
//! Responsibility: module-local ownership and contracts for db::executor::projection::eval::operators::binary::comparison.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError,
            operators::binary::{binary_op_name, invalid_binary_operands},
        },
        predicate::{CoercionId, CoercionSpec, compare_eq, compare_order},
        query::plan::expr::BinaryOp,
    },
    value::Value,
};

pub(super) fn eval_equality_binary_expr(
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
    let are_equal = if let Some(are_equal) = compare_eq(left, right, &coercion) {
        are_equal
    } else if !numeric_widen_enabled {
        // Preserve projection behavior for non-numeric cross-variant comparisons.
        left == right
    } else {
        return Err(invalid_binary_operands(op, left, right));
    };

    let result = match op {
        BinaryOp::Eq => are_equal,
        BinaryOp::Ne => !are_equal,
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => unreachable!("equality evaluator called with non-equality op"),
    };

    Ok(Value::Bool(result))
}

pub(super) fn eval_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    let ordering = compare_ordering(op, left, right)
        .ok_or_else(|| invalid_binary_operands(op, left, right))?;

    let result = match op {
        BinaryOp::Lt => ordering.is_lt(),
        BinaryOp::Lte => ordering.is_le(),
        BinaryOp::Gt => ordering.is_gt(),
        BinaryOp::Gte => ordering.is_ge(),
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Eq
        | BinaryOp::Ne => unreachable!("comparison evaluator called with non-comparison op"),
    };

    Ok(Value::Bool(result))
}

fn compare_ordering(op: BinaryOp, left: &Value, right: &Value) -> Option<Ordering> {
    let _ = binary_op_name(op);
    let coercion = if left.supports_numeric_coercion() && right.supports_numeric_coercion() {
        CoercionSpec::new(CoercionId::NumericWiden)
    } else {
        CoercionSpec::new(CoercionId::Strict)
    };

    compare_order(left, right, &coercion)
}
