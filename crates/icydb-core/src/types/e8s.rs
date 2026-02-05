use crate::{
    traits::{
        FieldValue, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto, SanitizeCustom,
        UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::Decimal,
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, FromStr, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    ops::{Div, DivAssign, Mul, MulAssign},
};

///
/// E8s
///
/// Fixed‑point with 8 fractional digits.
/// Stores numbers as `u64` representing value × 1e8 (e.g., 1.25 → 125_000_000).
///
/// Constructors:
/// - `from_atomic(raw)`: raw scaled integer (no scaling)
/// - `from_units(units)`: scales by 1e8 (saturating on overflow)
/// - `try_from_decimal_exact(d)`: fails if more than 8 fractional digits
/// - `from_decimal_round(d)`: rounds to 8dp
///

#[repr(transparent)]
#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    FromStr,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Sub,
    SubAssign,
    Sum,
)]
pub struct E8s(u64);

impl E8s {
    const DECIMALS: u32 = 8;
    const SCALE: u64 = 100_000_000; // 10^8

    ///
    /// CONSTRUCTORS
    ///

    /// Construct from **atomics** (raw scaled integer). No scaling applied.
    #[must_use]
    pub const fn from_atomic(raw: u64) -> Self {
        Self(raw)
    }

    /// Construct from **whole units** (tokens). Scales by 1e8.
    #[must_use]
    pub const fn from_units(units: u64) -> Self {
        Self(units.saturating_mul(Self::SCALE))
    }

    /// Exact decimal → fixed-point, fails if more than 8 fractional digits.
    #[must_use]
    pub fn try_from_decimal_exact(d: Decimal) -> Option<Self> {
        let parts = d.parts();

        // Reject negative values
        if parts.mantissa < 0 {
            return None;
        }

        // Reject excess fractional precision
        if parts.scale > 8 {
            return None;
        }

        // Scale mantissa to fixed-point
        let factor = 10u64.checked_pow(8 - parts.scale)?;
        let scaled = u64::try_from(parts.mantissa).ok()?.checked_mul(factor)?;

        Some(Self(scaled))
    }

    /// Decimal → fixed-point with rounding to 8dp.
    #[must_use]
    pub fn from_decimal_round(d: Decimal) -> Option<Self> {
        let parts = d.parts();

        // Reject negative values
        if parts.mantissa < 0 {
            return None;
        }

        let target_scale = 8;

        let scaled_mantissa = if parts.scale <= target_scale {
            // Scale up exactly
            let factor = 10i128.checked_pow(target_scale - parts.scale)?;
            parts.mantissa.checked_mul(factor)?
        } else {
            // Scale down with rounding
            let divisor = 10i128.checked_pow(parts.scale - target_scale)?;
            let half = divisor / 2;

            // round half up
            (parts.mantissa + half) / divisor
        };

        let value = u64::try_from(scaled_mantissa).ok()?;
        Some(Self(value))
    }

    ///
    /// METHODS
    ///

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_sign_loss)]
    #[doc = "⚠️ Use only for non-critical float conversions. Prefer try_from_decimal."]
    pub fn from_f64(value: f64) -> Option<Self> {
        if value.is_nan() || value.is_infinite() || value < 0.0 {
            return None;
        }

        Some(Self((value * Self::SCALE as f64).round() as u64))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Saturating addition.
    #[must_use]
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    #[must_use]
    /// Convert the fixed-point value into a normalized `Decimal`.
    pub fn to_decimal(self) -> Decimal {
        Decimal::from_i128_with_scale(self.0.into(), Self::DECIMALS).normalize()
    }
}

impl Mul for E8s {
    type Output = Self;

    // Fixed-point multiplication:
    // (a / SCALE) * (b / SCALE), rescaled back to SCALE with round-to-nearest.
    // Saturates on overflow.
    fn mul(self, other: Self) -> Self::Output {
        let a = <u128 as From<u64>>::from(self.0);
        let b = <u128 as From<u64>>::from(other.0);
        let scale = <u128 as From<u64>>::from(Self::SCALE);

        // Fixed-point multiply with round-to-nearest
        let raw = (a * b + scale / 2) / scale;

        // Saturate on overflow back to u64
        let value = u64::try_from(raw).unwrap_or(u64::MAX);

        Self(value)
    }
}

impl MulAssign for E8s {
    fn mul_assign(&mut self, other: Self) {
        *self = *self * other;
    }
}

impl Div for E8s {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        if other.0 == 0 {
            return Self(u64::MAX);
        }

        let a = <u128 as From<u64>>::from(self.0);
        let b = <u128 as From<u64>>::from(other.0);
        let scale = <u128 as From<u64>>::from(Self::SCALE);

        // Fixed-point division with round-to-nearest:
        // (a / SCALE) ÷ (b / SCALE) = (a * SCALE) / b
        let raw = (a * scale + b / 2) / b;

        let value = u64::try_from(raw).unwrap_or(u64::MAX);
        Self(value)
    }
}

impl DivAssign for E8s {
    fn div_assign(&mut self, other: Self) {
        *self = *self / other;
    }
}

impl Display for E8s {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_decimal().fmt(f)
    }
}

impl FieldValue for E8s {
    fn to_value(&self) -> Value {
        Value::E8s(*self)
    }
}

#[allow(clippy::cast_possible_wrap)]
impl From<E8s> for Decimal {
    fn from(v: E8s) -> Self {
        // Use i128 mantissa to avoid overflow for large u64 values
        Self::from_i128_with_scale(v.get().into(), 8)
    }
}

impl TryFrom<i32> for E8s {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self(u64::try_from(n)?);
        Ok(v)
    }
}

impl From<u64> for E8s {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl Inner<Self> for E8s {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl NumCast for E8s {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u64().map(Self)
    }
}

impl NumFromPrimitive for E8s {
    #[allow(clippy::cast_sign_loss)]
    fn from_i64(n: i64) -> Option<Self> {
        if n < 0 { None } else { Some(Self(n as u64)) }
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self(n))
    }
}

impl NumToPrimitive for E8s {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for E8s {}

impl SanitizeCustom for E8s {}

impl UpdateView for E8s {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for E8s {}

impl ValidateCustom for E8s {}

impl View for E8s {
    type ViewType = u64;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

impl Visitable for E8s {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_display_formatting() {
        let dec = Decimal::from_str("42.5").unwrap();
        let fixed = E8s::try_from_decimal_exact(dec).unwrap();

        assert_eq!(fixed.to_string(), "42.5");
    }

    #[test]
    fn e8s_units_and_display() {
        let one_unit = E8s::from_units(1);

        assert_eq!(one_unit.get(), E8s::SCALE);
        assert_eq!(one_unit.to_string(), "1"); // because normalize() trims zeros
    }

    #[test]
    fn e8s_raw_and_decimal() {
        let one_atomic: E8s = 1u64.into();

        assert_eq!(one_atomic.to_string(), "0.00000001");
    }

    #[test]
    fn e8s_decimal_exact() {
        let x = E8s::try_from_decimal_exact(Decimal::from_str("42.5").unwrap()).unwrap();

        assert_eq!(x.to_string(), "42.5");
        assert_eq!(x.get(), 4_250_000_000);
    }

    #[test]
    fn e8s_decimal_round() {
        // 8dp rounds:
        let x = E8s::from_decimal_round(Decimal::from_str("0.0000000049").unwrap()).unwrap();
        assert_eq!(x.get(), 0); // rounds down
        let y = E8s::from_decimal_round(Decimal::from_str("0.0000000051").unwrap()).unwrap();
        assert_eq!(y.get(), 1); // rounds up
    }

    #[test]
    fn test_default_is_zero() {
        let fixed = E8s::default();
        assert_eq!(fixed.to_decimal(), Decimal::ZERO);
    }

    #[test]
    fn test_nan_and_infinity_rejection_from_f64() {
        assert!(E8s::from_f64(f64::NAN).is_none());
        assert!(E8s::from_f64(f64::INFINITY).is_none());
        assert!(E8s::from_f64(f64::NEG_INFINITY).is_none());
        assert!(E8s::from_f64(-0.1).is_none());
    }

    #[test]
    fn test_from_i64_rejects_negative() {
        let v = <E8s as NumFromPrimitive>::from_i64(-1);
        assert!(v.is_none());
    }

    #[test]
    fn test_from_f64_accuracy_and_rounding() {
        let val = 0.000_000_004_9_f64;
        let e = E8s::from_f64(val).unwrap();
        assert_eq!(e.0, 0);

        let val = 0.000_000_005_1_f64;
        let e = E8s::from_f64(val).unwrap();
        assert_eq!(e.0, 1);
    }
}
