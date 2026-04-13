//! Module: db::executor::projection::eval::operators::binary::comparison
//! Implements binary comparison operators for scalar projection evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError, operators::binary::invalid_binary_operands,
        },
        predicate::{CoercionId, CoercionSpec, compare_eq},
        query::plan::expr::BinaryOp,
    },
    value::Value,
};

#[cfg(test)]
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

    debug_assert!(
        matches!(op, BinaryOp::Eq),
        "equality evaluator called with non-equality operator",
    );

    Ok(Value::Bool(are_equal))
}
