//! Module: db::numeric
//! Responsibility: shared runtime numeric semantics for database values.
//! Does not own: numeric value representation, storage encoding, or query
//! function taxonomy.
//! Boundary: centralizes broad numeric coercion, arithmetic, and comparison
//! rules used across predicate, projection, aggregate, and ordering paths.

#[cfg(test)]
mod tests;

use crate::{types::Decimal, value::Value};
use std::cmp::Ordering;

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

/// Apply one arithmetic operation on already-coerced decimal operands.
///
/// This is the canonical arithmetic primitive for all runtime numeric
/// arithmetic surfaces (projection expressions and aggregate reducers).
#[must_use]
pub(in crate::db) fn apply_decimal_arithmetic(
    op: NumericArithmeticOp,
    left: Decimal,
    right: Decimal,
) -> Decimal {
    match op {
        NumericArithmeticOp::Add => left + right,
        NumericArithmeticOp::Sub => left - right,
        NumericArithmeticOp::Mul => left * right,
        NumericArithmeticOp::Div => left / right,
        NumericArithmeticOp::Rem => left % right,
    }
}

/// Add two decimal numeric terms under canonical runtime arithmetic semantics.
#[must_use]
pub(in crate::db) fn add_decimal_terms(left: Decimal, right: Decimal) -> Decimal {
    apply_decimal_arithmetic(NumericArithmeticOp::Add, left, right)
}

/// Divide one decimal term by another under canonical runtime arithmetic semantics.
#[must_use]
pub(in crate::db) fn divide_decimal_terms(left: Decimal, right: Decimal) -> Decimal {
    apply_decimal_arithmetic(NumericArithmeticOp::Div, left, right)
}

/// Compute decimal AVG from one `(sum, count)` pair under canonical arithmetic semantics.
///
/// Returns `None` when `count` cannot be represented in decimal.
#[must_use]
pub(in crate::db) fn average_decimal_terms(sum: Decimal, count: u64) -> Option<Decimal> {
    let divisor = Decimal::from_num(count)?;
    Some(divide_decimal_terms(sum, divisor))
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

/// Compute a lossy decimal square root for scalar SQL math.
///
/// The fixed-point decimal core intentionally stays exact. Functions like
/// `SQRT` need approximate math, so this boundary performs the explicit f64
/// round-trip and rejects negative or non-finite results.
#[must_use]
pub(in crate::db) fn decimal_sqrt(decimal: Decimal) -> Option<Decimal> {
    if decimal.is_sign_negative() {
        return None;
    }

    Decimal::from_f64_lossy(decimal.to_f64()?.sqrt())
}

/// Compute decimal power for scalar SQL math.
///
/// Non-negative integral exponents use the exact decimal exponent path. Other
/// exponents use the same explicit f64 bridge as `SQRT` and fail closed when
/// the result cannot be represented by the current decimal parser.
#[must_use]
pub(in crate::db) fn decimal_power(base: Decimal, exponent: Decimal) -> Option<Decimal> {
    if let Some(power) = exponent.to_u64() {
        return Some(base.powu(power));
    }

    Decimal::from_f64_lossy(base.to_f64()?.powf(exponent.to_f64()?))
}

/// Apply one numeric arithmetic operation under the shared numeric runtime contract.
///
/// Promotion and boundary rules:
/// - all supported numeric operands are coerced to `Decimal` first
/// - `Int`/`Uint`/`Int128`/`Uint128`/`Float32`/`Float64`/`Decimal`/`Duration`
///   /`Timestamp` participate when decimal coercion succeeds
/// - `IntBig` and `UintBig` are numeric-eligible at planning boundaries but are
///   rejected at runtime arithmetic boundaries when decimal coercion fails
///
/// Decimal operation semantics are inherited from `types::Decimal`:
/// - add/mul saturate on overflow
/// - div rounds half-away-from-zero at runtime division precision
/// - div by zero returns `Decimal::ZERO`
/// - div overflow saturates with division-scale output
/// - remainder by zero returns `Decimal::ZERO`
#[must_use]
pub(in crate::db) fn apply_numeric_arithmetic(
    op: NumericArithmeticOp,
    left: &Value,
    right: &Value,
) -> Option<Decimal> {
    let left = coerce_numeric_decimal(left)?;
    let right = coerce_numeric_decimal(right)?;

    let result = apply_decimal_arithmetic(op, left, right);

    Some(result)
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
