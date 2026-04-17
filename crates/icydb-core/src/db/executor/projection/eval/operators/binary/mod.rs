//! Module: db::executor::projection::eval::operators::binary
//! Re-exports binary scalar projection operators used by expression
//! evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
        BinaryOp::Or | BinaryOp::And => eval_boolean_binary_expr(op, left, right),
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => comparison::eval_compare_binary_expr(op, left, right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            eval_numeric_binary_expr(op, left, right)
        }
    }
}

fn eval_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    let (Value::Bool(left_bool), Value::Bool(right_bool)) = (left, right) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    let value = match op {
        BinaryOp::Or => *left_bool || *right_bool,
        BinaryOp::And => *left_bool && *right_bool,
        _ => unreachable!("boolean evaluator called with non-boolean operator"),
    };

    Ok(Value::Bool(value))
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
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
    }
}

pub(super) const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "or",
        BinaryOp::And => "and",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
    }
}
