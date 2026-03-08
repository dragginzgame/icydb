use crate::{
    db::executor::load::projection::grouped::GroupedRowView,
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::{CoercionId, CoercionSpec, compare_eq, compare_order},
        query::plan::expr::{BinaryOp, Expr, UnaryOp},
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// ProjectionEvalError
///
/// Pure expression-evaluation failures for scalar projection execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum ProjectionEvalError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

    #[error("projection expression cannot evaluate aggregate '{kind}' in scalar row context")]
    AggregateNotEvaluable { kind: String },

    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

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

/// Evaluate one projection expression against one entity row.
pub(in crate::db::executor) fn eval_expr<E>(
    expr: &Expr,
    row: &E,
) -> Result<Value, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot(E::MODEL, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };
            let Some(value) = row.get_value_by_index(field_index) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_name.to_string(),
                    index: field_index,
                });
            };

            Ok(value)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr(expr.as_ref(), row)?;
            eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr(left.as_ref(), row)?;
            let right_value = eval_expr(right.as_ref(), row)?;

            eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }),
        Expr::Alias { expr, .. } => eval_expr(expr.as_ref(), row),
    }
}

/// Evaluate one projection expression against one grouped output row view.
pub(in crate::db::executor) fn eval_expr_grouped(
    expr: &Expr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let Some(group_field_offset) =
                super::grouped::resolve_group_field_offset(grouped_row, field_id.as_str())
            else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_id.as_str().to_string(),
                });
            };
            let Some(value) = grouped_row.key_values.get(group_field_offset) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_id.as_str().to_string(),
                    index: group_field_offset,
                });
            };

            Ok(value.clone())
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_grouped(expr.as_ref(), grouped_row)?;
            eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_grouped(left.as_ref(), grouped_row)?;
            let right_value = eval_expr_grouped(right.as_ref(), grouped_row)?;

            eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate_expr) => {
            let Some(aggregate_index) =
                super::grouped::resolve_grouped_aggregate_index(grouped_row, aggregate_expr)
            else {
                return Err(ProjectionEvalError::UnknownGroupedAggregateExpression {
                    kind: format!("{:?}", aggregate_expr.kind()),
                    target_field: aggregate_expr.target_field().map(str::to_string),
                    distinct: aggregate_expr.is_distinct(),
                });
            };
            let Some(value) = grouped_row.aggregate_values.get(aggregate_index) else {
                return Err(ProjectionEvalError::MissingGroupedAggregateValue {
                    aggregate_index,
                    aggregate_count: grouped_row.aggregate_values.len(),
                });
            };

            Ok(value.clone())
        }
        Expr::Alias { expr, .. } => eval_expr_grouped(expr.as_ref(), grouped_row),
    }
}

fn eval_unary_expr(op: UnaryOp, value: Value) -> Result<Value, ProjectionEvalError> {
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

fn eval_binary_expr(op: BinaryOp, left: Value, right: Value) -> Result<Value, ProjectionEvalError> {
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
