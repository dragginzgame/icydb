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
