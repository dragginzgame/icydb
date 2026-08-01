//! Module: db::scalar_expr
//! Responsibility: shared scalar-only expression compilation and evaluation.
//! Does not own: predicate boolean trees or index key framing.
//! Boundary: predicate and index runtimes use this to avoid `Value` fallback for scalar work.

use crate::{
    db::schema::PersistedIndexExpressionOp,
    types::Date,
    value::{Value, lower_text, upper_text},
};
use std::borrow::Cow;

const MILLIS_PER_DAY: i64 = 86_400_000;
const EXPECTED_TEXT: &str = "Text";
const EXPECTED_DATE_OR_TIMESTAMP: &str = "Date/Timestamp";
const DATE_OR_TIMESTAMP_OUT_OF_RANGE: &str = "Date/Timestamp within 0000-01-01..=9999-12-31";

///
/// ScalarIndexExpressionOp
///
/// ScalarIndexExpressionOp is the shared transform opcode for scalar index
/// expressions.
/// Runtime slot evaluation and value-based planner lowering both route through
/// this operator so expression semantics stay aligned.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarIndexExpressionOp {
    Lower,
    Upper,
    Trim,
    LowerTrim,
    Date,
    Year,
    Month,
    Day,
}

///
/// ScalarExprValue
///
/// ScalarExprValue is the result container for the accepted index-expression
/// transforms supported by this module.
/// It preserves borrowed field payloads where possible and only allocates for
/// derived text transforms.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarExprValue<'a> {
    Date(crate::types::Date),
    Int(i64),
    Text(Cow<'a, str>),
    Timestamp(crate::types::Timestamp),
}

/// Convert one shared scalar expression value into the runtime `Value` enum.
#[must_use]
pub(in crate::db) fn scalar_expr_value_into_value(value: ScalarExprValue<'_>) -> Value {
    match value {
        ScalarExprValue::Date(value) => Value::Date(value),
        ScalarExprValue::Int(value) => Value::Int64(value),
        ScalarExprValue::Text(value) => Value::Text(value.into_owned()),
        ScalarExprValue::Timestamp(value) => Value::Timestamp(value),
    }
}

/// Map one index expression shape to its shared scalar transform opcode.
#[must_use]
pub(in crate::db) const fn scalar_index_expression_op(
    expression: PersistedIndexExpressionOp,
) -> ScalarIndexExpressionOp {
    match expression {
        PersistedIndexExpressionOp::Lower => ScalarIndexExpressionOp::Lower,
        PersistedIndexExpressionOp::Upper => ScalarIndexExpressionOp::Upper,
        PersistedIndexExpressionOp::Trim => ScalarIndexExpressionOp::Trim,
        PersistedIndexExpressionOp::LowerTrim => ScalarIndexExpressionOp::LowerTrim,
        PersistedIndexExpressionOp::Date => ScalarIndexExpressionOp::Date,
        PersistedIndexExpressionOp::Year => ScalarIndexExpressionOp::Year,
        PersistedIndexExpressionOp::Month => ScalarIndexExpressionOp::Month,
        PersistedIndexExpressionOp::Day => ScalarIndexExpressionOp::Day,
    }
}

/// Apply one shared scalar expression opcode to one non-null scalar input.
pub(in crate::db) fn derive_non_null_scalar_expression_value(
    op: ScalarIndexExpressionOp,
    source: ScalarExprValue<'_>,
) -> Result<ScalarExprValue<'_>, &'static str> {
    match op {
        ScalarIndexExpressionOp::Lower => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(lower_text(text.as_ref()))))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Upper => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(upper_text(text.as_ref()))))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Trim => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(text.trim().to_string())))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::LowerTrim => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(lower_text(text.trim()))))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Date => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Date(value)),
            ScalarExprValue::Timestamp(value) => timestamp_to_bucket_date(value.as_millis())
                .map(ScalarExprValue::Date)
                .ok_or(DATE_OR_TIMESTAMP_OUT_OF_RANGE),
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Year => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.year()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis())
                    .ok_or(DATE_OR_TIMESTAMP_OUT_OF_RANGE)?;
                Ok(ScalarExprValue::Int(i64::from(bucket.year())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Month => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.month()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis())
                    .ok_or(DATE_OR_TIMESTAMP_OUT_OF_RANGE)?;
                Ok(ScalarExprValue::Int(i64::from(bucket.month())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Day => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.day()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis())
                    .ok_or(DATE_OR_TIMESTAMP_OUT_OF_RANGE)?;
                Ok(ScalarExprValue::Int(i64::from(bucket.day())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
    }
}

fn timestamp_to_bucket_date(timestamp_millis: i64) -> Option<Date> {
    let days = timestamp_millis.div_euclid(MILLIS_PER_DAY);
    Date::try_from_i64(days)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Timestamp;

    #[test]
    fn index_text_expressions_preserve_canonical_unicode_transforms() {
        assert_eq!(
            derive_non_null_scalar_expression_value(
                ScalarIndexExpressionOp::Lower,
                ScalarExprValue::Text(Cow::Borrowed("Straße")),
            ),
            Ok(ScalarExprValue::Text(Cow::Owned("straße".to_string()))),
        );
        assert_eq!(
            derive_non_null_scalar_expression_value(
                ScalarIndexExpressionOp::Upper,
                ScalarExprValue::Text(Cow::Borrowed("ßeta")),
            ),
            Ok(ScalarExprValue::Text(Cow::Owned("SSETA".to_string()))),
        );
        assert_eq!(
            derive_non_null_scalar_expression_value(
                ScalarIndexExpressionOp::LowerTrim,
                ScalarExprValue::Text(Cow::Borrowed("  Straße  ")),
            ),
            Ok(ScalarExprValue::Text(Cow::Owned("straße".to_string()))),
        );
    }

    #[test]
    fn timestamp_date_expression_rejects_values_outside_bounded_calendar() {
        let minimum_millis = i64::from(Date::MIN.as_days_since_epoch()) * MILLIS_PER_DAY;
        let before_minimum = minimum_millis - MILLIS_PER_DAY;

        assert_eq!(
            derive_non_null_scalar_expression_value(
                ScalarIndexExpressionOp::Date,
                ScalarExprValue::Timestamp(Timestamp::from_millis(minimum_millis)),
            ),
            Ok(ScalarExprValue::Date(Date::MIN)),
        );
        assert_eq!(
            derive_non_null_scalar_expression_value(
                ScalarIndexExpressionOp::Date,
                ScalarExprValue::Timestamp(Timestamp::from_millis(before_minimum)),
            ),
            Err(DATE_OR_TIMESTAMP_OUT_OF_RANGE),
        );
    }
}
