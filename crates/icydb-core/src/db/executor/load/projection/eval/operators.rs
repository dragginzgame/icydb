//! Module: executor::load::projection::eval::operators
//! Responsibility: unary/binary expression operator evaluation for projection eval.
//! Does not own: row field resolution or grouped aggregate index resolution.
//! Boundary: pure operator semantics for scalar and grouped projection evaluation.

use crate::{
    db::{
        executor::load::projection::eval::ProjectionEvalError,
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::{CoercionId, CoercionSpec, compare_eq, compare_order},
        query::plan::expr::{BinaryOp, UnaryOp},
    },
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;

pub(in crate::db::executor) fn eval_unary_expr(
    op: UnaryOp,
    value: Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Neg => {
            let Some(result) = apply_numeric_arithmetic(
                NumericArithmeticOp::Sub,
                &Value::Decimal(Decimal::ZERO),
                &value,
            ) else {
                return Err(ProjectionEvalError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value),
                });
            };

            Ok(Value::Decimal(result))
        }
        UnaryOp::Not => {
            let Value::Bool(v) = value else {
                return Err(ProjectionEvalError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value),
                });
            };

            Ok(Value::Bool(!v))
        }
    }
}

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
        BinaryOp::Eq | BinaryOp::Ne => eval_equality_binary_expr(op, left, right),
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            eval_compare_binary_expr(op, left, right)
        }
    }
}

fn eval_numeric_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let Some(arithmetic_op) = numeric_arithmetic_op(op) else {
        return Err(ProjectionEvalError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
    };
    let Some(result) = apply_numeric_arithmetic(arithmetic_op, &left, &right) else {
        return Err(ProjectionEvalError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
    };

    Ok(Value::Decimal(result))
}

fn eval_boolean_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let (Value::Bool(left_bool), Value::Bool(right_bool)) = (&left, &right) else {
        return Err(ProjectionEvalError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
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

fn eval_equality_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let numeric_widen_enabled =
        left.supports_numeric_coercion() || right.supports_numeric_coercion();
    let coercion = if numeric_widen_enabled {
        CoercionSpec::new(CoercionId::NumericWiden)
    } else {
        CoercionSpec::new(CoercionId::Strict)
    };
    let are_equal = if let Some(are_equal) = compare_eq(&left, &right, &coercion) {
        are_equal
    } else if !numeric_widen_enabled {
        // Preserve projection behavior for non-numeric cross-variant comparisons.
        left == right
    } else {
        return Err(ProjectionEvalError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
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

fn eval_compare_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ProjectionEvalError> {
    let ordering = compare_ordering(op, &left, &right).ok_or_else(|| {
        ProjectionEvalError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
        }
    })?;

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
    let _ = op;
    let coercion = if left.supports_numeric_coercion() && right.supports_numeric_coercion() {
        CoercionSpec::new(CoercionId::NumericWiden)
    } else {
        CoercionSpec::new(CoercionId::Strict)
    };

    compare_order(left, right, &coercion)
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

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "neg",
        UnaryOp::Not => "not",
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
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
