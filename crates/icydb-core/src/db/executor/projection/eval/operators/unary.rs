//! Module: db::executor::projection::eval::operators::unary
//! Implements unary scalar projection operators used by expression evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{executor::projection::eval::ProjectionEvalError, query::plan::expr::UnaryOp},
    value::Value,
};

pub(in crate::db) fn eval_unary_expr(
    op: UnaryOp,
    value: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let Value::Bool(v) = value else {
                return Err(ProjectionEvalError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value.clone()),
                });
            };

            Ok(Value::Bool(!*v))
        }
    }
}

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
    }
}
