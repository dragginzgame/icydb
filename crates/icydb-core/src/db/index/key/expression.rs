//! Module: index::key::expression
//! Responsibility: canonical expression key derivation/value transforms for index keys.
//! Does not own: index key byte framing, planner eligibility, or store mutation policy.
//! Boundary: index-key build and planner/explain key-item lowering consume this authority.

use crate::{
    db::scalar_expr::{
        ScalarExprValue, derive_non_null_scalar_expression_value, scalar_index_expression_op,
    },
    model::index::IndexExpression,
    value::Value,
};

const EXPECTED_TEXT: &str = "Text";
const EXPECTED_DATE_OR_TIMESTAMP: &str = "Date/Timestamp";

fn derive_text_expression_value(
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Text(value) => ScalarExprValue::Text(value.into()),
        _ => return Err(EXPECTED_TEXT),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn derive_temporal_expression_value(
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Date(value) => ScalarExprValue::Date(value),
        Value::Timestamp(value) => ScalarExprValue::Timestamp(value),
        _ => return Err(EXPECTED_DATE_OR_TIMESTAMP),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn scalar_expr_value_into_value(value: ScalarExprValue<'_>) -> Value {
    match value {
        ScalarExprValue::Null => Value::Null,
        ScalarExprValue::Blob(value) => Value::Blob(value.into_owned()),
        ScalarExprValue::Bool(value) => Value::Bool(value),
        ScalarExprValue::Date(value) => Value::Date(value),
        ScalarExprValue::Duration(value) => Value::Duration(value),
        ScalarExprValue::Float32(value) => Value::Float32(value),
        ScalarExprValue::Float64(value) => Value::Float64(value),
        ScalarExprValue::Int(value) => Value::Int(value),
        ScalarExprValue::Principal(value) => Value::Principal(value),
        ScalarExprValue::Subaccount(value) => Value::Subaccount(value),
        ScalarExprValue::Text(value) => Value::Text(value.into_owned()),
        ScalarExprValue::Timestamp(value) => Value::Timestamp(value),
        ScalarExprValue::Uint(value) => Value::Uint(value),
        ScalarExprValue::Ulid(value) => Value::Ulid(value),
        ScalarExprValue::Unit => Value::Unit,
    }
}

/// Apply one canonical index expression to one source field value.
///
/// Returns:
/// - `Ok(Some(...))` for one derived indexable value
/// - `Ok(None)` for `NULL` source values (non-indexable)
/// - `Err(expected_type)` for type-mismatched sources
pub(in crate::db) fn derive_index_expression_value(
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    match expression {
        IndexExpression::Lower(_)
        | IndexExpression::Upper(_)
        | IndexExpression::Trim(_)
        | IndexExpression::LowerTrim(_) => derive_text_expression_value(expression, source),
        IndexExpression::Date(_)
        | IndexExpression::Year(_)
        | IndexExpression::Month(_)
        | IndexExpression::Day(_) => derive_temporal_expression_value(expression, source),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::index::derive_index_expression_value, model::index::IndexExpression, types::Timestamp,
        value::Value,
    };

    #[test]
    fn derive_lower_expression_value_casefolds_text() {
        let value = derive_index_expression_value(
            IndexExpression::Lower("email"),
            Value::Text("ALICE@Example.Com".to_string()),
        )
        .expect("lower(text) should derive one value");

        assert_eq!(value, Some(Value::Text("alice@example.com".to_string())));
    }

    #[test]
    fn derive_date_expression_value_buckets_timestamp() {
        let ts = Timestamp::from_millis(86_400_000 * 3 + 12_345);
        let value = derive_index_expression_value(
            IndexExpression::Date("created_at"),
            Value::Timestamp(ts),
        )
        .expect("date(timestamp) should derive one value");
        let Value::Date(date) = value.expect("date(timestamp) should be indexable") else {
            panic!("expected date bucket");
        };

        assert_eq!(date.as_days_since_epoch(), 3);
    }
}
