use crate::{
    traits::{
        AsView, FieldValue, FieldValueKind, Inner, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, Visitable,
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
/// E18s
///
/// Ethereum‑style fixed‑point with 18 fractional digits.
/// Stores numbers as `u128` representing value × 1e18 (e.g., 1.25 → 1_250_000_000_000_000_000).
///
/// Constructors:
/// - `from_atomic(raw)`: raw scaled integer (no scaling)
/// - `from_units(units)`: scales by 1e18 (saturating on overflow)
/// - `from_decimal(d)`: exact decimal → fixed‑point (None if negative/out of range)
/// - `from_f64(v)`: rounded, for non‑critical conversions only
///

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
    Sum,
    SubAssign,
)]
pub struct E18s(u128);

impl E18s {
    const DECIMALS: u32 = 18;
    const SCALE: u128 = 1_000_000_000_000_000_000; // 10^18

    ///
    /// CONSTRUCTORS
    ///

    /// Construct from **atomics** (raw scaled integer). No scaling applied.
    #[must_use]
    pub const fn from_atomic(raw: u128) -> Self {
        Self(raw)
    }

    /// Construct from **whole units**. Scales by 10^18 (saturating).
    #[must_use]
    pub const fn from_units(units: u128) -> Self {
        Self(units.saturating_mul(Self::SCALE))
    }

    /// Exact `Decimal` → fixed-point. Returns `None` if value has more than 18 fractional digits,
    /// is negative, or out of range for `u128`.
    #[must_use]
    pub fn from_decimal(value: Decimal) -> Option<Self> {
        let parts = value.parts();

        // Reject negative values
        if parts.mantissa < 0 {
            return None;
        }

        // Reject excess fractional precision
        if parts.scale > 18 {
            return None;
        }

        // Scale mantissa to fixed-point
        let factor = 10u128.checked_pow(18 - parts.scale)?;
        let scaled = u128::try_from(parts.mantissa).ok()?.checked_mul(factor)?;

        Some(Self(scaled))
    }

    /// ⚠️ Non-critical float conversions only. Prefer the Decimal-based API.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    /// Convert from `f64`, rounding to 18 decimal places (lossy).
    pub fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        Some(Self((value * Self::SCALE as f64).round() as u128))
    }

    ///
    /// ACCESSORS
    ///

    #[must_use]
    pub const fn get(self) -> u128 {
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
    #[allow(clippy::cast_possible_wrap)]
    /// Convert the fixed-point value into a normalized `Decimal`.
    /// Returns `None` if the value does not fit in `i128`.
    pub fn to_decimal(self) -> Option<Decimal> {
        if self.0 > i128::MAX as u128 {
            return None;
        }

        Some(Decimal::from_i128_with_scale(self.0 as i128, Self::DECIMALS).normalize())
    }

    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl AsView for E18s {
    type ViewType = u128;

    fn as_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

impl Div for E18s {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        let raw = self.0.saturating_mul(Self::SCALE) / other.0;
        Self(raw)
    }
}

impl DivAssign for E18s {
    fn div_assign(&mut self, other: Self) {
        *self = *self / other;
    }
}

impl Display for E18s {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_decimal() {
            Some(d) => d.fmt(f),
            None => write!(f, "[overflow]"),
        }
    }
}

impl FieldValue for E18s {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::E18s(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::E18s(v) => Some(*v),
            _ => None,
        }
    }
}

impl TryFrom<i32> for E18s {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self(u128::try_from(n)?);
        Ok(v)
    }
}

impl From<u128> for E18s {
    fn from(n: u128) -> Self {
        Self(n)
    }
}

impl Inner<Self> for E18s {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl Mul for E18s {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        let scale = Self::SCALE;

        // Split scaling to reduce overflow risk:
        // (a * b) / scale  ≈  (a / scale) * b  OR  a * (b / scale)
        //
        // Choose the safer direction dynamically.
        let raw = if self.0 >= other.0 {
            let a = self.0 / scale;
            a.checked_mul(other.0)
                .expect("E18s multiplication overflow")
        } else {
            let b = other.0 / scale;
            self.0.checked_mul(b).expect("E18s multiplication overflow")
        };

        Self(raw)
    }
}

impl MulAssign for E18s {
    fn mul_assign(&mut self, other: Self) {
        *self = *self * other;
    }
}

impl NumCast for E18s {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u128().map(Self)
    }
}

impl NumFromPrimitive for E18s {
    #[allow(clippy::cast_sign_loss)]
    fn from_i64(n: i64) -> Option<Self> {
        if n < 0 { None } else { Some(Self(n as u128)) }
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self(n.into()))
    }
}

impl NumToPrimitive for E18s {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for E18s {}

impl SanitizeCustom for E18s {}

impl UpdateView for E18s {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) -> Result<(), crate::traits::Error> {
        *self = v;

        Ok(())
    }
}

impl ValidateAuto for E18s {}

impl ValidateCustom for E18s {}

impl Visitable for E18s {}

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
        let e18s = E18s::from_decimal(dec).unwrap();

        assert_eq!(e18s.to_string(), "42.5");
    }

    #[test]
    fn test_equality_and_ordering() {
        let a = E18s::from_decimal(Decimal::from_str("10.0").unwrap()).unwrap();
        let b = E18s::from_decimal(Decimal::from_str("20.0").unwrap()).unwrap();
        let c = E18s::from_decimal(Decimal::from_str("10.0").unwrap()).unwrap();

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, c);
    }

    #[test]
    fn test_from_u128() {
        let raw = 42 * E18s::SCALE;
        let e18s = <E18s as NumCast>::from(raw).unwrap();

        assert_eq!(e18s.to_decimal(), <Decimal as NumCast>::from(42));
    }

    #[test]
    fn test_default_is_zero() {
        let fixed = E18s::default();

        assert_eq!(fixed.to_decimal(), Some(Decimal::ZERO));
    }

    #[test]
    fn test_to_decimal_overflow_rejected() {
        let too_large = E18s::from_atomic(i128::MAX as u128 + 1);
        assert!(too_large.to_decimal().is_none());
    }

    #[test]
    fn test_nan_and_infinity_rejection() {
        assert!(E18s::from_f64(f64::NAN).is_none());
        assert!(E18s::from_f64(f64::INFINITY).is_none());
        assert!(E18s::from_f64(f64::NEG_INFINITY).is_none());
        assert!(E18s::from_f64(-0.1).is_none());
    }

    #[test]
    fn test_from_i64_rejects_negative() {
        let v = <E18s as NumFromPrimitive>::from_i64(-1);
        assert!(v.is_none());
    }
}
