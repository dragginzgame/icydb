//! Module: index::key::expression
//! Responsibility: canonical expression key derivation/value transforms for index keys.
//! Does not own: index key byte framing, planner eligibility, or store mutation policy.
//! Boundary: index-key build and planner/explain key-item lowering consume this authority.

use crate::{
    db::scalar_expr::{
        ScalarExprValue, derive_non_null_scalar_expression_value, scalar_expr_value_into_value,
        scalar_index_expression_op,
    },
    db::schema::PersistedIndexExpressionOp,
    value::Value,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexExpressionSourceClass {
    Text,
    DateOrTimestamp,
}

fn derive_text_expression_value(
    expression: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Text(value) => ScalarExprValue::Text(value.into()),
        _ => return Err(IndexExpressionSourceClass::Text),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::Text)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn derive_temporal_expression_value(
    expression: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Date(value) => ScalarExprValue::Date(value),
        Value::Timestamp(value) => ScalarExprValue::Timestamp(value),
        _ => return Err(IndexExpressionSourceClass::DateOrTimestamp),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::DateOrTimestamp)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

/// Apply one canonical index expression to one source field value.
///
/// Returns:
/// - `Ok(Some(...))` for one derived indexable value
/// - `Ok(None)` for `NULL` source values (non-indexable)
/// - `Err(expected_source_class)` for type-mismatched sources
pub(in crate::db) fn derive_index_expression_value(
    expression: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    match expression {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => derive_text_expression_value(expression, source),
        PersistedIndexExpressionOp::Date
        | PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => derive_temporal_expression_value(expression, source),
    }
}
