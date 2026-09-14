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
    source: &Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Text(value) => ScalarExprValue::Text(value.as_str().into()),
        _ => return Err(IndexExpressionSourceClass::Text),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::Text)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn derive_temporal_expression_value(
    expression: PersistedIndexExpressionOp,
    source: &Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let op = scalar_index_expression_op(expression);
    let source = match source {
        Value::Null => return Ok(None),
        Value::Date(value) => ScalarExprValue::Date(*value),
        Value::Timestamp(value) => ScalarExprValue::Timestamp(*value),
        _ => return Err(IndexExpressionSourceClass::DateOrTimestamp),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::DateOrTimestamp)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

/// Apply one canonical index expression to one source field value.
/// Borrow the source through the shared scalar evaluator; only the derived
/// result needs ownership for key encoding or predicate compilation.
///
/// Returns:
/// - `Ok(Some(...))` for one derived indexable value
/// - `Ok(None)` for `NULL` source values (non-indexable)
/// - `Err(expected_source_class)` for type-mismatched sources
pub(in crate::db) fn derive_index_expression_value(
    expression: PersistedIndexExpressionOp,
    source: &Value,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Date, Timestamp};

    #[test]
    fn borrowed_text_sources_preserve_canonical_transforms_and_rejections() {
        let source = Value::Text(" \u{2003}Straße İ\u{2003} ".to_string());
        for (op, expected) in [
            (
                PersistedIndexExpressionOp::Lower,
                " \u{2003}straße i\u{307}\u{2003} ",
            ),
            (
                PersistedIndexExpressionOp::Upper,
                " \u{2003}STRASSE İ\u{2003} ",
            ),
            (PersistedIndexExpressionOp::Trim, "Straße İ"),
            (PersistedIndexExpressionOp::LowerTrim, "straße i\u{307}"),
        ] {
            assert_eq!(
                derive_index_expression_value(op, &source),
                Ok(Some(Value::Text(expected.to_string()))),
            );
            assert_eq!(derive_index_expression_value(op, &Value::Null), Ok(None));
            for invalid in [Value::Int64(1), Value::List(vec![source.clone()])] {
                assert_eq!(
                    derive_index_expression_value(op, &invalid),
                    Err(IndexExpressionSourceClass::Text),
                );
            }
        }
        assert_eq!(
            source,
            Value::Text(" \u{2003}Straße İ\u{2003} ".to_string())
        );
    }

    #[test]
    fn borrowed_temporal_sources_preserve_calendar_boundaries_and_rejections() {
        let date = Date::MIN;
        let millis = i64::from(date.as_days_since_epoch()) * 86_400_000;
        let sources = [
            Value::Date(date),
            Value::Timestamp(Timestamp::from_millis(millis)),
        ];
        for (op, expected) in [
            (PersistedIndexExpressionOp::Date, Value::Date(date)),
            (
                PersistedIndexExpressionOp::Year,
                Value::Int64(i64::from(date.year())),
            ),
            (
                PersistedIndexExpressionOp::Month,
                Value::Int64(i64::from(date.month())),
            ),
            (
                PersistedIndexExpressionOp::Day,
                Value::Int64(i64::from(date.day())),
            ),
        ] {
            for source in &sources {
                assert_eq!(
                    derive_index_expression_value(op, source),
                    Ok(Some(expected.clone())),
                );
            }
            assert_eq!(derive_index_expression_value(op, &Value::Null), Ok(None));
            for invalid in [
                Value::Text("0000-01-01".to_string()),
                Value::Timestamp(Timestamp::from_millis(millis - 1)),
            ] {
                assert_eq!(
                    derive_index_expression_value(op, &invalid),
                    Err(IndexExpressionSourceClass::DateOrTimestamp),
                );
            }
        }
    }
}
