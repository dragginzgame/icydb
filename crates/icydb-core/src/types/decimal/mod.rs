//! Module: types::decimal
//! Defines the fixed-point decimal runtime type and its arithmetic,
//! normalization, and value-conversion helpers.

mod arithmetic;
mod compare;
mod text;
mod wire;

#[cfg(test)]
mod tests;

use crate::traits::{
    Atomic, NumericValue, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind,
    RuntimeValueMeta, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

// We cap scale at 28 to keep i128 intermediate math practical while still
// covering common fixed-point workloads (including e8/e18 compatibility).
pub(in crate::types::decimal) const MAX_SUPPORTED_SCALE: u32 = 28;
pub(in crate::types::decimal) const DEFAULT_DIVISION_SCALE: u32 = 18;
pub(in crate::types::decimal) const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;

///
/// DecimalParts
///
/// Canonical decomposition of a `Decimal`.
///
/// `mantissa * 10^-scale` reconstructs the represented value, and the mantissa
/// carries the sign.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalParts {
    mantissa: i128,
    scale: u32,
}

impl DecimalParts {
    /// Return the canonical decimal mantissa component.
    #[must_use]
    pub const fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// Return the canonical decimal scale component.
    #[must_use]
    pub const fn scale(&self) -> u32 {
        self.scale
    }
}

///
/// ParseDecimalError
///
/// User-facing parse failure for decimal text input.
///
/// This keeps text parsing errors explicit without pulling transport or
/// arithmetic semantics into the error surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseDecimalError {
    message: String,
}

impl ParseDecimalError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::error::Error for ParseDecimalError {}

impl Display for ParseDecimalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

///
/// Decimal
///
/// Owned fixed-point decimal with an explicit i128 mantissa and base-10 scale.
///
/// Arithmetic saturates on overflow, division by zero resolves to `ZERO`, and
/// normalization keeps equivalent values on one canonical representation.
///

#[derive(Clone, Copy, Debug, Default)]
pub struct Decimal {
    mantissa: i128,
    scale: u32,
}

impl Decimal {
    pub const ZERO: Self = Self {
        mantissa: 0,
        scale: 0,
    };

    /// Returns the maximum supported decimal scale.
    #[must_use]
    pub const fn max_supported_scale() -> u32 {
        MAX_SUPPORTED_SCALE
    }

    /// Construct a decimal from mantissa and scale.
    ///
    /// Panics when `scale` exceeds the supported range.
    #[must_use]
    pub const fn new(num: i64, scale: u32) -> Self {
        assert!(
            scale <= MAX_SUPPORTED_SCALE,
            "decimal scale exceeds supported range"
        );
        Self::new_unchecked(num, scale)
    }

    /// Fallible constructor from mantissa and scale.
    #[must_use]
    pub const fn try_new(num: i64, scale: u32) -> Option<Self> {
        if scale > MAX_SUPPORTED_SCALE {
            return None;
        }

        Some(Self::new_unchecked(num, scale))
    }

    /// Unchecked constructor from mantissa and scale.
    ///
    /// This constructor may violate the decimal scale invariant and should only
    /// be used when the caller already enforces `scale <= MAX_SUPPORTED_SCALE`.
    #[must_use]
    pub const fn new_unchecked(num: i64, scale: u32) -> Self {
        Self {
            mantissa: num as i128,
            scale,
        }
    }

    /// Fallible conversion from common numeric types.
    ///
    /// This path is lossy for float inputs and may lose precision for large values.
    /// Prefer exact integer constructors (`from_i64`, `from_u64`) or explicit
    /// float constructors (`from_f32_lossy`, `from_f64_lossy`) when possible.
    pub fn from_num<N: NumericValue>(n: N) -> Option<Self> {
        n.try_to_decimal()
    }

    /// Exact conversion from `i64`.
    #[must_use]
    pub const fn from_i64(n: i64) -> Option<Self> {
        Some(Self {
            mantissa: n as i128,
            scale: 0,
        })
    }

    /// Exact conversion from `u64`.
    #[must_use]
    pub const fn from_u64(n: u64) -> Option<Self> {
        Some(Self {
            mantissa: n as i128,
            scale: 0,
        })
    }

    /// Exact conversion from `i128`.
    #[must_use]
    pub const fn from_i128(n: i128) -> Option<Self> {
        Some(Self {
            mantissa: n,
            scale: 0,
        })
    }

    /// Exact conversion from `u128`.
    #[must_use]
    pub fn from_u128(n: u128) -> Option<Self> {
        Some(Self {
            mantissa: i128::try_from(n).ok()?,
            scale: 0,
        })
    }

    /// Explicit lossy conversion from `f32`.
    ///
    /// Uses decimal text round-tripping from the binary float representation.
    /// This is intentionally lossy and should be used only when float input is required.
    #[must_use]
    pub fn from_f32_lossy(n: f32) -> Option<Self> {
        if !n.is_finite() {
            return None;
        }

        Self::from_str(&n.to_string()).ok()
    }

    /// Explicit lossy conversion from `f64`.
    ///
    /// Uses decimal text round-tripping from the binary float representation.
    /// This is intentionally lossy and should be used only when float input is required.
    #[must_use]
    pub fn from_f64_lossy(n: f64) -> Option<Self> {
        if !n.is_finite() {
            return None;
        }

        Self::from_str(&n.to_string()).ok()
    }

    ///
    /// PARTS
    ///

    /// Decompose into mantissa and scale.
    #[must_use]
    pub const fn parts(&self) -> DecimalParts {
        DecimalParts {
            mantissa: self.mantissa,
            scale: self.scale,
        }
    }

    /// Returns true if the decimal has no fractional component.
    #[must_use]
    pub const fn is_integer(&self) -> bool {
        self.scale == 0
    }

    /// Scale by 10^target_scale and require an integer result.
    ///
    /// Returns `None` if:
    /// - fractional precision would be lost
    /// - integer overflow occurs
    #[must_use]
    pub fn scale_to_integer(&self, target_scale: u32) -> Option<i128> {
        if self.scale > target_scale {
            return None;
        }

        let factor = Self::checked_pow10(target_scale - self.scale)?;
        self.mantissa.checked_mul(factor)
    }

    /// Convert to `i32` when the decimal is integral and in range.
    #[must_use]
    pub fn to_i32(&self) -> Option<i32> {
        self.to_i64().and_then(|value| i32::try_from(value).ok())
    }

    /// Convert to `i64` when the decimal is integral and in range.
    #[must_use]
    pub fn to_i64(&self) -> Option<i64> {
        let integer = Self::decimal_integer_value(self.mantissa, self.scale)?;

        i64::try_from(integer).ok()
    }

    /// Convert to `i128` when the decimal is integral.
    #[must_use]
    pub fn to_i128(&self) -> Option<i128> {
        Self::decimal_integer_value(self.mantissa, self.scale)
    }

    /// Convert to `u64` when the decimal is integral and in range.
    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        let integer = Self::decimal_integer_value(self.mantissa, self.scale)?;

        u64::try_from(integer).ok()
    }

    /// Convert to `u128` when the decimal is integral and in range.
    #[must_use]
    pub fn to_u128(&self) -> Option<u128> {
        let integer = Self::decimal_integer_value(self.mantissa, self.scale)?;

        u128::try_from(integer).ok()
    }

    /// Convert to `f32` when the decimal is finite in `f32`.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn to_f32(&self) -> Option<f32> {
        self.to_f64().and_then(|value| {
            let float = value as f32;
            if float.is_finite() { Some(float) } else { None }
        })
    }

    /// Convert to `f64` when the decimal is finite in `f64`.
    #[must_use]
    #[expect(clippy::cast_precision_loss)]
    pub fn to_f64(&self) -> Option<f64> {
        let divisor = 10f64.powi(i32::try_from(self.scale).ok()?);
        let value = (self.mantissa as f64) / divisor;

        if value.is_finite() { Some(value) } else { None }
    }

    /// Build from a raw mantissa and scale.
    #[must_use]
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        Self::checked_from_mantissa_scale(num, scale).unwrap_or(Self::ZERO)
    }

    /// Normalize trailing zeros.
    #[must_use]
    pub const fn normalize(&self) -> Self {
        let (mantissa, scale) = self.normalized_parts();
        Self { mantissa, scale }
    }

    /// Returns `true` if the value is negative.
    #[must_use]
    pub const fn is_sign_negative(&self) -> bool {
        self.mantissa < 0
    }

    /// Returns the number of fractional decimal places.
    #[must_use]
    pub const fn scale(&self) -> u32 {
        self.scale
    }

    /// Returns the mantissa component.
    #[must_use]
    pub const fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// Returns `true` if the value is zero.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    const fn normalized_parts(&self) -> (i128, u32) {
        Self::normalize_parts(self.mantissa, self.scale)
    }

    const fn checked_from_mantissa_scale(mantissa: i128, scale: u32) -> Option<Self> {
        if scale <= MAX_SUPPORTED_SCALE {
            return Some(Self { mantissa, scale });
        }

        let mut m = mantissa;
        let mut s = scale;

        while s > MAX_SUPPORTED_SCALE {
            if m == 0 {
                return Some(Self {
                    mantissa: 0,
                    scale: MAX_SUPPORTED_SCALE,
                });
            }

            if m % 10 != 0 {
                return None;
            }

            m /= 10;
            s -= 1;
        }

        Some(Self {
            mantissa: m,
            scale: s,
        })
    }

    const fn checked_pow10(power: u32) -> Option<i128> {
        10i128.checked_pow(power)
    }

    fn decimal_integer_value(mantissa: i128, scale: u32) -> Option<i128> {
        if scale == 0 {
            return Some(mantissa);
        }

        let divisor = Self::checked_pow10(scale)?;
        if mantissa % divisor != 0 {
            return None;
        }

        Some(mantissa / divisor)
    }

    const fn normalize_parts(mantissa: i128, scale: u32) -> (i128, u32) {
        if mantissa == 0 {
            return (0, 0);
        }

        let mut m = mantissa;
        let mut s = scale;

        while s > 0 {
            if m % 10 != 0 {
                break;
            }

            m /= 10;
            s -= 1;
        }

        (m, s)
    }

    const fn saturating_extreme(scale: u32, negative: bool) -> Self {
        let mantissa = if negative { i128::MIN } else { i128::MAX };
        Self { mantissa, scale }
    }
}

impl Atomic for Decimal {}

impl RuntimeValueMeta for Decimal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Decimal {
    fn to_value(&self) -> crate::value::Value {
        crate::value::Value::Decimal(*self)
    }
}

impl RuntimeValueDecode for Decimal {
    fn from_value(value: &crate::value::Value) -> Option<Self> {
        match value {
            crate::value::Value::Decimal(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumericValue for Decimal {
    fn try_to_decimal(&self) -> Option<Self> {
        Some(*self)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        Some(value)
    }
}

impl SanitizeAuto for Decimal {}

impl SanitizeCustom for Decimal {}

impl ValidateAuto for Decimal {}

impl ValidateCustom for Decimal {}

impl Visitable for Decimal {}
