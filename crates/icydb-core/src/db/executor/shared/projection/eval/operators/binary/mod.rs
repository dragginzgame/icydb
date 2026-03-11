//! Module: db::executor::shared::projection::eval::operators::binary
//! Responsibility: module-local ownership and contracts for db::executor::shared::projection::eval::operators::binary.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod comparison;

use crate::{
    db::{
        executor::shared::projection::eval::ProjectionEvalError,
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        query::plan::expr::BinaryOp,
    },
    value::Value,
};

pub(in crate::db::executor) fn eval_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            eval_numeric_binary_expr(op, left, right)
        }
        BinaryOp::And | BinaryOp::Or => eval_boolean_binary_expr(op, left, right),
        BinaryOp::Eq | BinaryOp::Ne => comparison::eval_equality_binary_expr(op, left, right),
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            comparison::eval_compare_binary_expr(op, left, right)
        }
    }
}

fn eval_numeric_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let Some(arithmetic_op) = numeric_arithmetic_op(op) else {
        return Err(invalid_binary_operands(op, left, right));
    };
    let Some(result) = apply_numeric_arithmetic(arithmetic_op, &left, &right) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Decimal(result))
}

fn eval_boolean_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let (Value::Bool(left_bool), Value::Bool(right_bool)) = (&left, &right) else {
        return Err(invalid_binary_operands(op, left, right));
    };

    let result = match op {
        BinaryOp::And => *left_bool && *right_bool,
        BinaryOp::Or => *left_bool || *right_bool,
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => unreachable!("boolean binary evaluator called with non-boolean op"),
    };

    Ok(Value::Bool(result))
}

pub(super) fn invalid_binary_operands(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> ProjectionEvalError {
    ProjectionEvalError::InvalidBinaryOperands {
        op: binary_op_name(op).to_string(),
        left: Box::new(left),
        right: Box::new(right),
    }
}

const fn numeric_arithmetic_op(op: BinaryOp) -> Option<NumericArithmeticOp> {
    match op {
        BinaryOp::Add => Some(NumericArithmeticOp::Add),
        BinaryOp::Sub => Some(NumericArithmeticOp::Sub),
        BinaryOp::Mul => Some(NumericArithmeticOp::Mul),
        BinaryOp::Div => Some(NumericArithmeticOp::Div),
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => None,
    }
}

pub(super) const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
    }
}
