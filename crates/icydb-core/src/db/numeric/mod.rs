//! Module: db::numeric
//! Responsibility: shared runtime numeric semantics for database values.
//! Does not own: numeric value representation, storage encoding, or query
//! function taxonomy.
//! Boundary: centralizes broad numeric coercion, arithmetic, and comparison
//! rules used across predicate, projection, aggregate, and ordering paths.

#[cfg(test)]
mod tests;

use crate::{error::InternalError, types::Decimal, value::Value};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// NumericArithmeticOp
///
/// Canonical runtime arithmetic operator set for numeric expression evaluation.
///
/// This enum is intentionally independent from planner expression operators so
/// numeric runtime semantics stay reusable at executor boundaries.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum NumericArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

///
/// NumericEvalError
///
/// NumericEvalError is the checked SQL numeric-evaluation error contract.
/// Primitive numeric types may retain saturating behavior, but SQL-facing
/// evaluation uses this type to fail explicitly on overflow or values that
/// cannot be represented in the exact numeric result domain.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum NumericEvalError {
    #[error("numeric overflow")]
    Overflow,

    #[error("numeric result is not representable")]
    NotRepresentable,
}

impl NumericEvalError {
    /// Convert this numeric evaluation failure into the query execution error
    /// taxonomy used by executor paths that cannot return `QueryError`
    /// directly.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Overflow => InternalError::query_numeric_overflow(),
            Self::NotRepresentable => InternalError::query_numeric_not_representable(),
        }
    }
}

/// Apply one checked arithmetic operation on already-coerced decimal operands.
///
/// This reports overflow and non-representable results instead of inheriting
/// the primitive decimal type's saturating operators.
pub(in crate::db) fn apply_decimal_arithmetic_checked(
    op: NumericArithmeticOp,
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, NumericEvalError> {
    match op {
        NumericArithmeticOp::Add => left.checked_add(right).ok_or(NumericEvalError::Overflow),
        NumericArithmeticOp::Sub => left.checked_sub(right).ok_or(NumericEvalError::Overflow),
        NumericArithmeticOp::Mul => left.checked_mul(right).ok_or(NumericEvalError::Overflow),
        NumericArithmeticOp::Div => {
            if right.is_zero() {
                return Err(NumericEvalError::NotRepresentable);
            }

            left.checked_div(right).ok_or(NumericEvalError::Overflow)
        }
        NumericArithmeticOp::Rem => {
            if right.is_zero() {
                return Err(NumericEvalError::NotRepresentable);
            }

            left.checked_rem(right).ok_or(NumericEvalError::Overflow)
        }
    }
}

/// Add two decimal numeric terms under checked SQL numeric evaluation semantics.
pub(in crate::db) fn add_decimal_terms_checked(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, NumericEvalError> {
    apply_decimal_arithmetic_checked(NumericArithmeticOp::Add, left, right)
}

/// Divide one decimal term by another under checked SQL numeric evaluation semantics.
pub(in crate::db) fn divide_decimal_terms_checked(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, NumericEvalError> {
    apply_decimal_arithmetic_checked(NumericArithmeticOp::Div, left, right)
}

/// Compute decimal AVG from one `(sum, count)` pair under checked SQL numeric
/// evaluation semantics.
pub(in crate::db) fn average_decimal_terms_checked(
    sum: Decimal,
    count: u64,
) -> Result<Decimal, NumericEvalError> {
    let divisor = Decimal::from_num(count).ok_or(NumericEvalError::NotRepresentable)?;
    divide_decimal_terms_checked(sum, divisor)
}

/// Coerce one value into decimal under the shared numeric coercion contract.
///
/// Returns `None` when the value is outside numeric coercion domain or cannot
/// be represented as a decimal under current runtime numeric rules.
#[must_use]
pub(in crate::db) fn coerce_numeric_decimal(value: &Value) -> Option<Decimal> {
    if !value.supports_numeric_coercion() {
        return None;
    }

    value.to_numeric_decimal()
}

/// Return the SQL-style sign of one decimal as `-1`, `0`, or `1`.
#[must_use]
pub(in crate::db) fn decimal_sign(decimal: Decimal) -> Decimal {
    let sign = match decimal.cmp(&Decimal::ZERO) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };

    Decimal::from_i64(sign).expect("small sign values must fit decimal")
}

/// Compute decimal square root under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_sqrt_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    if decimal.is_sign_negative() {
        return Err(NumericEvalError::NotRepresentable);
    }

    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .sqrt(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal cube root under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_cbrt_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .cbrt(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal exponent under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_exp_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .exp(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal natural logarithm under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_ln_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    if decimal <= Decimal::ZERO {
        return Err(NumericEvalError::NotRepresentable);
    }

    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .ln(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal base-2 logarithm under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_log2_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    if decimal <= Decimal::ZERO {
        return Err(NumericEvalError::NotRepresentable);
    }

    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .log2(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal base-10 logarithm under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_log10_checked(decimal: Decimal) -> Result<Decimal, NumericEvalError> {
    if decimal <= Decimal::ZERO {
        return Err(NumericEvalError::NotRepresentable);
    }

    Decimal::from_f64_lossy(
        decimal
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .log10(),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal logarithm with an explicit base under checked SQL numeric
/// evaluation semantics.
pub(in crate::db) fn decimal_log_base_checked(
    base: Decimal,
    value: Decimal,
) -> Result<Decimal, NumericEvalError> {
    if base <= Decimal::ZERO || base == Decimal::from_i64(1).expect("one fits decimal") {
        return Err(NumericEvalError::NotRepresentable);
    }
    if value <= Decimal::ZERO {
        return Err(NumericEvalError::NotRepresentable);
    }

    Decimal::from_f64_lossy(
        value
            .to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .log(base.to_f64().ok_or(NumericEvalError::NotRepresentable)?),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Compute decimal power under checked SQL numeric evaluation semantics.
pub(in crate::db) fn decimal_power_checked(
    base: Decimal,
    exponent: Decimal,
) -> Result<Decimal, NumericEvalError> {
    if let Some(power) = exponent.to_u64() {
        return base.checked_powu(power).ok_or(NumericEvalError::Overflow);
    }

    Decimal::from_f64_lossy(
        base.to_f64()
            .ok_or(NumericEvalError::NotRepresentable)?
            .powf(
                exponent
                    .to_f64()
                    .ok_or(NumericEvalError::NotRepresentable)?,
            ),
    )
    .ok_or(NumericEvalError::NotRepresentable)
}

/// Apply one numeric arithmetic operation under checked SQL numeric evaluation semantics.
///
/// `Ok(None)` means the operands are outside the numeric coercion domain.
/// `Err(_)` means the operands were numeric but the exact numeric result failed
/// the checked SQL evaluation contract.
pub(in crate::db) fn apply_numeric_arithmetic_checked(
    op: NumericArithmeticOp,
    left: &Value,
    right: &Value,
) -> Result<Option<Decimal>, NumericEvalError> {
    let Some(left) = coerce_numeric_decimal(left) else {
        return Ok(None);
    };
    let Some(right) = coerce_numeric_decimal(right) else {
        return Ok(None);
    };

    apply_decimal_arithmetic_checked(op, left, right).map(Some)
}

/// Compare two values under numeric-widen coercion semantics.
///
/// Returns `None` when either side is outside numeric coercion domain or when
/// conversion cannot produce a comparable numeric representation.
#[must_use]
pub(in crate::db) fn compare_numeric_order(left: &Value, right: &Value) -> Option<Ordering> {
    let left = coerce_numeric_decimal(left)?;
    let right = coerce_numeric_decimal(right)?;
    left.partial_cmp(&right)
}

/// Compare values with numeric widening first, then strict same-variant ordering.
///
/// This helper centralizes common "numeric if possible, strict otherwise"
/// comparator behavior used across planner/executor boundaries.
#[must_use]
pub(in crate::db) fn compare_numeric_or_strict_order(
    left: &Value,
    right: &Value,
) -> Option<Ordering> {
    compare_numeric_order(left, right).or_else(|| Value::strict_order_cmp(left, right))
}

///
/// OrderingSemantics
///
/// Explicit ordering contract used by sort/range/continuation paths.
/// Callers must not reuse this ordering surface for deduplication equality.
///

trait OrderingSemantics<T: ?Sized> {
    /// Compare two values under this semantics contract.
    fn compare(left: &T, right: &T) -> Ordering;
}

///
/// CanonicalValueOrderingSemantics
///
/// Canonical total ordering for `Value` sort/range/cursor boundaries.
/// Numeric-capable pairs delegate to shared numeric-or-strict comparison first.
///

struct CanonicalValueOrderingSemantics;

impl OrderingSemantics<Value> for CanonicalValueOrderingSemantics {
    fn compare(left: &Value, right: &Value) -> Ordering {
        if let Some(ordering) = compare_numeric_or_strict_order(left, right) {
            return ordering;
        }

        Value::canonical_cmp(left, right)
    }
}

/// Compare two values with canonical ordering semantics.
#[must_use]
pub(in crate::db) fn canonical_value_compare(left: &Value, right: &Value) -> Ordering {
    CanonicalValueOrderingSemantics::compare(left, right)
}

/// Compare two values for numeric equality under numeric-widen semantics.
#[must_use]
pub(in crate::db) fn compare_numeric_eq(left: &Value, right: &Value) -> Option<bool> {
    compare_numeric_order(left, right).map(|ordering| ordering == Ordering::Equal)
}
