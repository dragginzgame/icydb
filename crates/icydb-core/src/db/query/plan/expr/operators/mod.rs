//! Value-level scalar operators shared by folding and execution.
//! Traversal, lazy branches, accounting and error projection remain caller-owned.

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests;

use crate::{
    db::{
        numeric::{
            NumericEvalError, apply_value_arithmetic_checked, compare_numeric_eq,
            compare_numeric_or_strict_order,
        },
        query::plan::expr::BinaryOp,
    },
    value::Value,
};
use std::cmp::Ordering;

/// Apply the scalar three-valued truth table, including absorbing booleans.
pub(super) const fn boolean(op: BinaryOp, left: &Value, right: &Value) -> Option<Value> {
    match (op, left, right) {
        (BinaryOp::And, Value::Bool(false), _) | (BinaryOp::And, _, Value::Bool(false)) => {
            Some(Value::Bool(false))
        }
        (BinaryOp::And, Value::Bool(true), Value::Bool(true)) => Some(Value::Bool(true)),
        (BinaryOp::Or, Value::Bool(true), _) | (BinaryOp::Or, _, Value::Bool(true)) => {
            Some(Value::Bool(true))
        }
        (BinaryOp::Or, Value::Bool(false), Value::Bool(false)) => Some(Value::Bool(false)),
        (BinaryOp::And, Value::Bool(true) | Value::Null, Value::Null)
        | (BinaryOp::And, Value::Null, Value::Bool(true))
        | (BinaryOp::Or, Value::Bool(false) | Value::Null, Value::Null)
        | (BinaryOp::Or, Value::Null, Value::Bool(false)) => Some(Value::Null),
        _ => None,
    }
}

/// NULL propagation precedes the existing checked arithmetic authority.
pub(super) fn arithmetic(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Option<Value>, NumericEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Some(Value::Null));
    }
    let Some(op) = op.numeric_arithmetic_op() else {
        return Ok(None);
    };
    apply_value_arithmetic_checked(op, left, right)
}

/// Dispatch comparisons without choosing the caller's numeric policy.
/// Equality and ordering remain separate: an ordering fallback cannot prove equality.
pub(super) fn compare(
    op: BinaryOp,
    left: &Value,
    right: &Value,
    equality: impl FnOnce(&Value, &Value) -> Option<bool>,
    ordering: impl FnOnce(&Value, &Value) -> Option<Ordering>,
) -> Option<bool> {
    match op {
        BinaryOp::Eq => equality(left, right),
        BinaryOp::Ne => equality(left, right).map(|equal| !equal),
        BinaryOp::Lt => ordering(left, right).map(Ordering::is_lt),
        BinaryOp::Lte => ordering(left, right).map(Ordering::is_le),
        BinaryOp::Gt => ordering(left, right).map(Ordering::is_gt),
        BinaryOp::Gte => ordering(left, right).map(Ordering::is_ge),
        _ => None,
    }
}

/// Planner folding and function helpers retain database numeric coercion.
/// Compiled expressions deliberately supply Value-local comparators instead.
pub(super) fn compare_query(op: BinaryOp, left: &Value, right: &Value) -> Option<bool> {
    compare(
        op,
        left,
        right,
        |left, right| {
            compare_numeric_eq(left, right).or_else(|| {
                (!left.supports_numeric_coercion() && !right.supports_numeric_coercion())
                    .then(|| left == right)
            })
        },
        compare_numeric_or_strict_order,
    )
}
