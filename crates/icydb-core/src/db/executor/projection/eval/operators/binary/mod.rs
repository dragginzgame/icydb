//! Module: db::executor::projection::eval::operators::binary
//! Re-exports binary scalar projection operators used by expression
//! evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(test)]
mod comparison;

use crate::{
    db::{
        executor::projection::eval::ProjectionEvalError,
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        query::plan::expr::BinaryOp,
    },
    value::Value,
};

pub(in crate::db) fn eval_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        BinaryOp::Add => eval_numeric_binary_expr(op, left, right),
        #[cfg(test)]
        BinaryOp::Mul => eval_numeric_binary_expr(op, left, right),
        #[cfg(test)]
        BinaryOp::And => eval_boolean_binary_expr(op, left, right),
        #[cfg(test)]
        BinaryOp::Eq => comparison::eval_equality_binary_expr(op, left, right),
    }
}

fn eval_numeric_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    let arithmetic_op = numeric_arithmetic_op(op);
    let Some(result) = apply_numeric_arithmetic(arithmetic_op, left, right) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Decimal(result))
}

#[cfg(test)]
fn eval_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    let (Value::Bool(left_bool), Value::Bool(right_bool)) = (left, right) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    debug_assert!(
        matches!(op, BinaryOp::And),
        "boolean binary evaluator called with non-boolean operator",
    );

    Ok(Value::Bool(*left_bool && *right_bool))
}

pub(super) fn invalid_binary_operands(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> ProjectionEvalError {
    ProjectionEvalError::InvalidBinaryOperands {
        op: binary_op_name(op).to_string(),
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
    }
}

const fn numeric_arithmetic_op(op: BinaryOp) -> NumericArithmeticOp {
    match op {
        BinaryOp::Add => NumericArithmeticOp::Add,
        #[cfg(test)]
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        #[cfg(test)]
        BinaryOp::And | BinaryOp::Eq => NumericArithmeticOp::Add,
    }
}

pub(super) const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        #[cfg(test)]
        BinaryOp::Mul => "mul",
        #[cfg(test)]
        BinaryOp::And => "and",
        #[cfg(test)]
        BinaryOp::Eq => "eq",
    }
}
