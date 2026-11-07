//! Module: value::ops::numeric
//!
//! Responsibility: representation-local numeric conversion and comparison.
//! Does not own: predicate-level numeric policy or planner coercion legality.
//! Boundary: low-level helpers consumed by database numeric semantics.

use crate::{
    types::{Decimal, NumericValue, U256},
    value::{Value, semantics},
};
use std::cmp::Ordering;

fn to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::Decimal(value) => value.try_to_decimal(),
        Value::Duration(value) => value.try_to_decimal(),
        Value::Float64(value) => value.try_to_decimal(),
        Value::Float32(value) => value.try_to_decimal(),
        Value::Int64(value) => value.try_to_decimal(),
        Value::Int128(value) => value.try_to_decimal(),
        Value::Timestamp(value) => value.try_to_decimal(),
        Value::Nat64(value) => value.try_to_decimal(),
        Value::Nat128(value) => value.try_to_decimal(),

        _ => None,
    }
}

// Internal numeric coercion helper for aggregate arithmetic.
pub(crate) fn to_numeric_decimal(value: &Value) -> Option<Decimal> {
    to_decimal(value)
}

/// Compare the exact same-type numeric domains before widening. Database and
/// Value callers share this representation fact, retaining their own mixed-type
/// coercion policy. Time values use the same millisecond inputs as NumericValue.
pub(crate) fn compare_native_numeric_pair(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Int128(left), Value::Int128(right)) => Some(left.cmp(right)),
        (Value::Nat64(left), Value::Nat64(right)) => Some(left.cmp(right)),
        (Value::Nat128(_), Value::Nat128(_))
        | (Value::Float32(_), Value::Float32(_))
        | (Value::Float64(_), Value::Float64(_)) => Value::strict_order_cmp(left, right),
        (Value::Duration(left), Value::Duration(right)) => {
            Some(left.as_millis().cmp(&right.as_millis()))
        }
        (Value::Timestamp(left), Value::Timestamp(right)) => {
            Some(left.as_millis().cmp(&right.as_millis()))
        }
        (Value::Decimal(left), Value::Decimal(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

// A finite numeric magnitude is coefficient * 2^exponent / 5^scale.
// Decimal scale is at most 28; cross coefficients fit in 194 bits.
struct ExactNumeric {
    negative: bool,
    coefficient: u128,
    exponent: i32,
    scale: u32,
}

impl ExactNumeric {
    fn signed(value: i128, scale: u32) -> Option<Self> {
        Some(Self {
            negative: value < 0,
            coefficient: value.unsigned_abs(),
            exponent: -i32::try_from(scale).ok()?,
            scale,
        })
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int64(value) => Self::signed(i128::from(*value), 0),
            Value::Int128(value) => Self::signed(*value, 0),
            Value::Nat64(value) => Some(Self::unsigned(u128::from(*value))),
            Value::Nat128(value) => Some(Self::unsigned(*value)),
            Value::Timestamp(value) => Self::signed(i128::from(value.as_millis()), 0),
            Value::Duration(value) => Some(Self::unsigned(u128::from(value.as_millis()))),
            Value::Decimal(value) => Self::signed(value.mantissa(), value.scale()),
            Value::Float32(value) => Some(Self::float(f64::from(value.get()))),
            Value::Float64(value) => Some(Self::float(value.get())),
            _ => None,
        }
    }

    const fn unsigned(coefficient: u128) -> Self {
        Self {
            negative: false,
            coefficient,
            exponent: 0,
            scale: 0,
        }
    }

    fn float(value: f64) -> Self {
        let bits = value.to_bits();
        let encoded_exponent = ((bits >> 52) & 0x7ff) as u16;
        let fraction = bits & ((1_u64 << 52) - 1);
        let coefficient = if encoded_exponent == 0 {
            fraction
        } else {
            fraction | (1_u64 << 52)
        };
        Self {
            negative: value < 0.0,
            coefficient: u128::from(coefficient),
            exponent: if encoded_exponent == 0 {
                -1074
            } else {
                i32::from(encoded_exponent) - 1075
            },
            scale: 0,
        }
    }
}

// Normalize a nonzero fixed-width coefficient without materializing its binary
// exponent. This handles subnormals and extreme floats without oversized shifts.
fn normalized_magnitude(value: U256, exponent: i32) -> (i64, U256) {
    let (high, low) = value.into_words();
    let zeros = if high == 0 {
        128 + low.leading_zeros()
    } else {
        high.leading_zeros()
    };
    let normalized = match zeros {
        0 => value,
        1..=127 => U256::from_words((high << zeros) | (low >> (128 - zeros)), low << zeros),
        128..=255 => U256::from_words(low << (zeros - 128), 0),
        _ => U256::ZERO,
    };
    (i64::from(exponent) + 256 - i64::from(zeros), normalized)
}

/// Compare the exact represented value of finite broad numeric operands.
#[must_use]
pub(crate) fn compare_numeric_order(left: &Value, right: &Value) -> Option<Ordering> {
    if !semantics::supports_numeric_coercion(left) || !semantics::supports_numeric_coercion(right) {
        return None;
    }
    if let Some(ordering) = compare_native_numeric_pair(left, right) {
        return Some(ordering);
    }
    let left = ExactNumeric::from_value(left)?;
    let right = ExactNumeric::from_value(right)?;
    if left.negative != right.negative {
        return Some(right.negative.cmp(&left.negative));
    }
    let magnitude = if left.coefficient == 0 || right.coefficient == 0 {
        left.coefficient.cmp(&right.coefficient)
    } else {
        let left_coefficient = U256::from(left.coefficient)
            .checked_mul(U256::from(5_u128.checked_pow(right.scale)?))?;
        let right_coefficient = U256::from(right.coefficient)
            .checked_mul(U256::from(5_u128.checked_pow(left.scale)?))?;
        normalized_magnitude(left_coefficient, left.exponent)
            .cmp(&normalized_magnitude(right_coefficient, right.exponent))
    };
    Some(if left.negative {
        magnitude.reverse()
    } else {
        magnitude
    })
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
        compare_numeric_order(self, other)
    }
}
