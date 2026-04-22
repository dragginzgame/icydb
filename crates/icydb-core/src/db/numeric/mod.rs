//! Module: db::numeric
//! Responsibility: shared numeric capability classification for planning/execution.
//! Does not own: numeric expression evaluation or aggregate fold arithmetic.
//! Boundary: centralizes numeric field-kind domain predicates to reduce drift.

#[cfg(test)]
mod tests;

use crate::{
    db::query::plan::expr::classify_field_kind, model::field::FieldKind, types::Decimal,
    value::Value,
};
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

/// Return true when one field kind is accepted by numeric aggregate terminals.
///
/// Relation key kinds recurse so relation-backed numeric keys remain eligible.
#[must_use]
pub(in crate::db) const fn field_kind_supports_aggregate_numeric(kind: &FieldKind) -> bool {
    classify_field_kind(kind).supports_aggregate_numeric()
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

/// Compare two values for numeric equality under numeric-widen semantics.
#[must_use]
pub(in crate::db) fn compare_numeric_eq(left: &Value, right: &Value) -> Option<bool> {
    compare_numeric_order(left, right).map(|ordering| ordering == Ordering::Equal)
}
