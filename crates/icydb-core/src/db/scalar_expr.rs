//! Module: db::scalar_expr
//! Responsibility: shared scalar-only expression compilation and evaluation.
//! Does not own: predicate boolean trees or index key framing.
//! Boundary: predicate and index runtimes use this to avoid `Value` fallback for scalar work.

use crate::{db::schema::PersistedIndexExpressionOp, types::Date, value::Value};
use std::borrow::Cow;

const MILLIS_PER_DAY: i64 = 86_400_000;
const EXPECTED_TEXT: &str = "Text";
const EXPECTED_DATE_OR_TIMESTAMP: &str = "Date/Timestamp";

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
/// ScalarExprValue is the shared scalar result container for compiled scalar
/// expressions.
/// It preserves borrowed field payloads where possible and only allocates for
/// derived text transforms.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    dead_code,
    reason = "scalar expression results intentionally cover every scalar value kind even when a build uses only a subset"
)]
pub(in crate::db) enum ScalarExprValue<'a> {
    Null,
    Blob(Cow<'a, [u8]>),
    Bool(bool),
    Date(crate::types::Date),
    Duration(crate::types::Duration),
    Float32(crate::types::Float32),
    Float64(crate::types::Float64),
    Int(i64),
    Principal(crate::types::Principal),
    Subaccount(crate::types::Subaccount),
    Text(Cow<'a, str>),
    Timestamp(crate::types::Timestamp),
    Nat(u64),
    Ulid(crate::types::Ulid),
    Unit,
}

/// Convert one shared scalar expression value into the runtime `Value` enum.
#[must_use]
pub(in crate::db) fn scalar_expr_value_into_value(value: ScalarExprValue<'_>) -> Value {
    match value {
        ScalarExprValue::Null => Value::Null,
        ScalarExprValue::Blob(value) => Value::Blob(value.into_owned()),
        ScalarExprValue::Bool(value) => Value::Bool(value),
        ScalarExprValue::Date(value) => Value::Date(value),
        ScalarExprValue::Duration(value) => Value::Duration(value),
        ScalarExprValue::Float32(value) => Value::Float32(value),
        ScalarExprValue::Float64(value) => Value::Float64(value),
        ScalarExprValue::Int(value) => Value::Int64(value),
        ScalarExprValue::Principal(value) => Value::Principal(value),
        ScalarExprValue::Subaccount(value) => Value::Subaccount(value),
        ScalarExprValue::Text(value) => Value::Text(value.into_owned()),
        ScalarExprValue::Timestamp(value) => Value::Timestamp(value),
        ScalarExprValue::Nat(value) => Value::Nat64(value),
        ScalarExprValue::Ulid(value) => Value::Ulid(value),
        ScalarExprValue::Unit => Value::Unit,
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
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_casefold(text.as_ref()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Upper => match source {
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_upper(text.as_ref()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Trim => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(text.trim().to_string())))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::LowerTrim => match source {
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_casefold(text.trim()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Date => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Date(value)),
            ScalarExprValue::Timestamp(value) => Ok(ScalarExprValue::Date(
                timestamp_to_bucket_date(value.as_millis()),
            )),
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Year => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.year()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.year())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Month => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.month()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.month())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Day => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.day()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.day())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
    }
}

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
