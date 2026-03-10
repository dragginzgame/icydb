use crate::{
    db::{
        executor::load::projection::eval::ProjectionEvalError,
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        query::plan::expr::UnaryOp,
    },
    types::Decimal,
    value::Value,
};

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

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "neg",
        UnaryOp::Not => "not",
    }
}
