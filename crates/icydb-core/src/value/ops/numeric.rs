//! Module: value::ops::numeric
//!
//! Responsibility: representation-local numeric conversion and comparison.
//! Does not own: predicate-level numeric policy or planner coercion legality.
//! Boundary: low-level helpers consumed by database numeric semantics.

use crate::{
    traits::{NumericValue, Repr},
    types::Decimal,
    value::{Value, semantics},
};
use std::cmp::Ordering;

const F64_SAFE_I64: i64 = 1i64 << 53;
const F64_SAFE_U64: u64 = 1u64 << 53;
const F64_SAFE_I128: i128 = 1i128 << 53;
const F64_SAFE_U128: u128 = 1u128 << 53;

///
/// NumericRepr
///
/// Represents the comparable numeric form available for one `Value`. Decimal
/// is preferred when exact conversion is available; otherwise a lossless `f64`
/// is used only for values inside the well-defined integer safety envelope.
///

enum NumericRepr {
    Decimal(Decimal),
    F64(f64),
    None,
}

///
/// NumericArithmeticError
///
/// Reports checked numeric arithmetic failures from value-local arithmetic
/// helpers. The grouped executor maps these variants into its SQL-facing
/// projection error taxonomy without duplicating arithmetic rules.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericArithmeticError {
    Overflow,
    NotRepresentable,
}

fn numeric_repr(value: &Value) -> NumericRepr {
    // Numeric comparison eligibility is registry-authoritative.
    if !semantics::supports_numeric_coercion(value) {
        return NumericRepr::None;
    }

    if let Some(decimal) = to_decimal(value) {
        return NumericRepr::Decimal(decimal);
    }
    if let Some(float) = to_f64_lossless(value) {
        return NumericRepr::F64(float);
    }
    NumericRepr::None
}

fn to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::Decimal(value) => value.try_to_decimal(),
        Value::Duration(value) => value.try_to_decimal(),
        Value::Float64(value) => value.try_to_decimal(),
        Value::Float32(value) => value.try_to_decimal(),
        Value::Int(value) => value.try_to_decimal(),
        Value::Int128(value) => value.try_to_decimal(),
        Value::IntBig(value) => value.try_to_decimal(),
        Value::Timestamp(value) => value.try_to_decimal(),
        Value::Nat(value) => value.try_to_decimal(),
        Value::Nat128(value) => value.try_to_decimal(),
        Value::NatBig(value) => value.try_to_decimal(),

        _ => None,
    }
}

// Internal numeric coercion helper for aggregate arithmetic.
pub(crate) fn to_numeric_decimal(value: &Value) -> Option<Decimal> {
    to_decimal(value)
}

// This helper only returns `Some` inside the integer range exactly representable
// by `f64`, or for finite float wrappers that already own their precision.
#[expect(clippy::cast_precision_loss)]
fn to_f64_lossless(value: &Value) -> Option<f64> {
    match value {
        Value::Duration(value) if value.repr() <= F64_SAFE_U64 => Some(value.repr() as f64),
        Value::Float64(value) => Some(value.get()),
        Value::Float32(value) => Some(f64::from(value.get())),
        Value::Int(value) if (-F64_SAFE_I64..=F64_SAFE_I64).contains(value) => Some(*value as f64),
        Value::Int128(value) if (-F64_SAFE_I128..=F64_SAFE_I128).contains(&value.get()) => {
            Some(value.get() as f64)
        }
        Value::IntBig(value) => value.to_i128().and_then(|integer| {
            (-F64_SAFE_I128..=F64_SAFE_I128)
                .contains(&integer)
                .then_some(integer as f64)
        }),
        Value::Timestamp(value) if (-F64_SAFE_I64..=F64_SAFE_I64).contains(&value.repr()) => {
            Some(value.repr() as f64)
        }
        Value::Nat(value) if *value <= F64_SAFE_U64 => Some(*value as f64),
        Value::Nat128(value) if value.get() <= F64_SAFE_U128 => Some(value.get() as f64),
        Value::NatBig(value) => value
            .to_u128()
            .and_then(|integer| (integer <= F64_SAFE_U128).then_some(integer as f64)),

        _ => None,
    }
}

/// Compare two runtime values under value-local numeric coercion semantics.
#[must_use]
pub fn cmp_numeric(left: &Value, right: &Value) -> Option<Ordering> {
    if !semantics::supports_numeric_coercion(left) || !semantics::supports_numeric_coercion(right) {
        return None;
    }

    match (numeric_repr(left), numeric_repr(right)) {
        (NumericRepr::Decimal(left), NumericRepr::Decimal(right)) => left.partial_cmp(&right),
        (NumericRepr::F64(left), NumericRepr::F64(right)) => left.partial_cmp(&right),
        _ => None,
    }
}

/// Compare two values after exact decimal numeric coercion.
#[must_use]
pub(crate) fn compare_decimal_order(left: &Value, right: &Value) -> Option<Ordering> {
    if !semantics::supports_numeric_coercion(left) || !semantics::supports_numeric_coercion(right) {
        return None;
    }

    let left = to_decimal(left)?;
    let right = to_decimal(right)?;

    left.partial_cmp(&right)
}

/// Add two numeric values under checked decimal arithmetic semantics.
pub(crate) fn add(left: &Value, right: &Value) -> Result<Option<Decimal>, NumericArithmeticError> {
    apply_decimal_arithmetic(left, right, Decimal::checked_add, false)
}

/// Subtract two numeric values under checked decimal arithmetic semantics.
pub(crate) fn sub(left: &Value, right: &Value) -> Result<Option<Decimal>, NumericArithmeticError> {
    apply_decimal_arithmetic(left, right, Decimal::checked_sub, false)
}

/// Multiply two numeric values under checked decimal arithmetic semantics.
pub(crate) fn mul(left: &Value, right: &Value) -> Result<Option<Decimal>, NumericArithmeticError> {
    apply_decimal_arithmetic(left, right, Decimal::checked_mul, false)
}

/// Divide two numeric values under checked decimal arithmetic semantics.
pub(crate) fn div(left: &Value, right: &Value) -> Result<Option<Decimal>, NumericArithmeticError> {
    apply_decimal_arithmetic(left, right, Decimal::checked_div, true)
}

fn apply_decimal_arithmetic(
    left: &Value,
    right: &Value,
    apply: impl FnOnce(Decimal, Decimal) -> Option<Decimal>,
    division: bool,
) -> Result<Option<Decimal>, NumericArithmeticError> {
    if !semantics::supports_numeric_coercion(left) || !semantics::supports_numeric_coercion(right) {
        return Ok(None);
    }

    let Some(left) = to_decimal(left) else {
        return Ok(None);
    };
    let Some(right) = to_decimal(right) else {
        return Ok(None);
    };
    if division && right.is_zero() {
        return Err(NumericArithmeticError::NotRepresentable);
    }

    apply(left, right)
        .map(Some)
        .ok_or(NumericArithmeticError::Overflow)
}

impl Value {
    // Internal numeric coercion helper for aggregate arithmetic.
    pub(crate) fn to_numeric_decimal(&self) -> Option<Decimal> {
        to_numeric_decimal(self)
    }

    /// Compare two runtime values under value-local numeric coercion semantics.
    ///
    /// Database execution code should use `db::numeric` helpers as the
    /// canonical runtime boundary; this method remains the representation-local
    /// comparison primitive that those higher-level helpers are tested against.
    #[must_use]
    pub fn cmp_numeric(&self, other: &Self) -> Option<Ordering> {
        cmp_numeric(self, other)
    }
}
