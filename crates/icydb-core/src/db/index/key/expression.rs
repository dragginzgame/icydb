//! Module: index::key::expression
//! Responsibility: canonical expression key derivation/value transforms for index keys.
//! Does not own: index key byte framing, planner eligibility, or store mutation policy.
//! Boundary: index-key build and planner/explain key-item lowering consume this authority.

use crate::{model::index::IndexExpression, types::Date, value::Value};

const MILLIS_PER_DAY: i64 = 86_400_000;
const EXPECTED_TEXT: &str = "Text";
const EXPECTED_DATE_OR_TIMESTAMP: &str = "Date/Timestamp";

// Canonically normalize one text input under expression-casefold semantics.
#[must_use]
fn normalize_text_casefold(input: &str) -> String {
    if input.is_ascii() {
        input.to_ascii_lowercase()
    } else {
        input.to_lowercase()
    }
}

fn normalize_text_upper(input: &str) -> String {
    if input.is_ascii() {
        input.to_ascii_uppercase()
    } else {
        input.to_uppercase()
    }
}

fn timestamp_to_bucket_date(timestamp_millis: i64) -> Date {
    let days = timestamp_millis.div_euclid(MILLIS_PER_DAY);
    let days = if let Ok(days) = i32::try_from(days) {
        days
    } else if days < 0 {
        i32::MIN
    } else {
        i32::MAX
    };

    Date::from_days_since_epoch(days)
}

fn derive_text_expression_value(
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    match (expression, source) {
        (_, Value::Null) => Ok(None),
        (IndexExpression::Lower(_), Value::Text(value)) => {
            Ok(Some(Value::Text(normalize_text_casefold(&value))))
        }
        (IndexExpression::Upper(_), Value::Text(value)) => {
            Ok(Some(Value::Text(normalize_text_upper(&value))))
        }
        (IndexExpression::Trim(_), Value::Text(value)) => {
            Ok(Some(Value::Text(value.trim().to_string())))
        }
        (IndexExpression::LowerTrim(_), Value::Text(value)) => {
            Ok(Some(Value::Text(normalize_text_casefold(value.trim()))))
        }
        _ => Err(EXPECTED_TEXT),
    }
}

fn derive_temporal_expression_value(
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, &'static str> {
    match (expression, source) {
        (_, Value::Null) => Ok(None),
        (IndexExpression::Date(_), Value::Date(value)) => Ok(Some(Value::Date(value))),
        (IndexExpression::Date(_), Value::Timestamp(value)) => Ok(Some(Value::Date(
            timestamp_to_bucket_date(value.as_millis()),
        ))),
        (IndexExpression::Year(_), Value::Date(value)) => {
            Ok(Some(Value::Int(i64::from(value.year()))))
        }
        (IndexExpression::Year(_), Value::Timestamp(value)) => {
            let bucket = timestamp_to_bucket_date(value.as_millis());
            Ok(Some(Value::Int(i64::from(bucket.year()))))
        }
        (IndexExpression::Month(_), Value::Date(value)) => {
            Ok(Some(Value::Int(i64::from(value.month()))))
        }
        (IndexExpression::Month(_), Value::Timestamp(value)) => {
            let bucket = timestamp_to_bucket_date(value.as_millis());
            Ok(Some(Value::Int(i64::from(bucket.month()))))
        }
        (IndexExpression::Day(_), Value::Date(value)) => {
            Ok(Some(Value::Int(i64::from(value.day()))))
        }
        (IndexExpression::Day(_), Value::Timestamp(value)) => {
            let bucket = timestamp_to_bucket_date(value.as_millis());
            Ok(Some(Value::Int(i64::from(bucket.day()))))
        }
        _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
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
