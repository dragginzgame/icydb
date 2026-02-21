use crate::{
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};
use std::{
    cmp::Ordering,
    convert::From,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    iter::Sum,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
    str::FromStr,
};

// We cap scale at 28 to keep i128 intermediate math practical while still
// covering common fixed-point workloads (including e8/e18 compatibility).
const MAX_SUPPORTED_SCALE: u32 = 28;
const DEFAULT_DIVISION_SCALE: u32 = 18;
const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;

///
/// DecimalParts
///
/// Canonical decomposition of a Decimal.
///
/// Invariant:
/// - value == mantissa * 10^-scale
/// - mantissa carries the sign
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalParts {
    pub mantissa: i128,
    pub scale: u32,
}

///
/// ParseDecimalError
///
/// User-facing parse failure for decimal text input.
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
/// Numeric policy:
/// - add/sub/mul are saturating on overflow
/// - div by zero returns `Decimal::ZERO`
/// - div overflow falls back to signed saturation at `DEFAULT_DIVISION_SCALE`
/// - rem by zero returns `Decimal::ZERO` (and `checked_rem` returns `None`)
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

    #[must_use]
    /// Construct a decimal from mantissa and scale.
    ///
    /// Panics when `scale` exceeds the supported range.
    pub const fn new(num: i64, scale: u32) -> Self {
        assert!(
            scale <= MAX_SUPPORTED_SCALE,
            "decimal scale exceeds supported range"
        );
        Self::new_unchecked(num, scale)
    }

    #[must_use]
    /// Fallible constructor from mantissa and scale.
    pub const fn try_new(num: i64, scale: u32) -> Option<Self> {
        if scale > MAX_SUPPORTED_SCALE {
            return None;
        }

        Some(Self::new_unchecked(num, scale))
    }

    #[must_use]
    /// Unchecked constructor from mantissa and scale.
    ///
    /// This constructor may violate the decimal scale invariant and should only
    /// be used when the caller already enforces `scale <= MAX_SUPPORTED_SCALE`.
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
    pub fn from_num<N: NumCast>(n: N) -> Option<Self> {
        <Self as NumCast>::from(n)
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

        let factor = checked_pow10(target_scale - self.scale)?;
        self.mantissa.checked_mul(factor)
    }

    ///
    /// ARITHMETIC HELPERS
    ///

    const fn normalized_parts(&self) -> (i128, u32) {
        normalize_parts(self.mantissa, self.scale)
    }

    fn checked_add_impl(self, rhs: Self) -> Option<Self> {
        let target_scale = self.scale.max(rhs.scale);
        let lhs = align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

        Some(Self {
            mantissa: lhs.checked_add(rhs)?,
            scale: target_scale,
        })
    }

    fn checked_mul_impl(self, rhs: Self) -> Option<Self> {
        let scale = self.scale.checked_add(rhs.scale)?;
        let mantissa = self.mantissa.checked_mul(rhs.mantissa)?;
        Self::checked_from_mantissa_scale(mantissa, scale)
    }

    fn checked_div_impl(self, rhs: Self) -> Option<Self> {
        if rhs.is_zero() {
            return None;
        }

        let lhs = self.normalize();
        let rhs = rhs.normalize();
        let mut target_scale = DEFAULT_DIVISION_SCALE;

        // Retry at lower precision when intermediate scaling overflows i128.
        loop {
            if let Some((numerator, denominator)) = division_operands(lhs, rhs, target_scale) {
                let mantissa = div_round_half_away_from_zero(numerator, denominator)?;
                if let Some(value) = Self::checked_from_mantissa_scale(mantissa, target_scale) {
                    return Some(value.normalize());
                }
            }

            if target_scale == 0 {
                return None;
            }

            target_scale = target_scale.saturating_sub(1);
        }
    }

    fn checked_rem_impl(self, rhs: Self) -> Option<Self> {
        if rhs.is_zero() {
            return None;
        }

        let target_scale = self.scale.max(rhs.scale);
        let lhs = align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

        Some(Self {
            mantissa: lhs.checked_rem(rhs)?,
            scale: target_scale,
        })
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

    ///
    /// WRAPPED FUNCTIONS
    ///

    #[must_use]
    /// Round to a given number of decimal places.
    pub const fn round_dp(&self, dp: u32) -> Self {
        if self.scale <= dp {
            return *self;
        }

        let diff = self.scale - dp;
        let Some(divisor) = checked_pow10(diff) else {
            return *self;
        };
        let quotient = self.mantissa / divisor;
        let remainder = self.mantissa % divisor;

        // `divisor` is 10^diff and always positive here.
        let should_round = remainder.unsigned_abs() >= divisor.unsigned_abs() / 2;
        let rounded = if should_round {
            if self.mantissa.is_negative() {
                quotient.saturating_sub(1)
            } else {
                quotient.saturating_add(1)
            }
        } else {
            quotient
        };

        Self {
            mantissa: rounded,
            scale: dp,
        }
    }

    #[must_use]
    /// Return the absolute value of the decimal.
    pub const fn abs(&self) -> Self {
        Self {
            mantissa: self.mantissa.saturating_abs(),
            scale: self.scale,
        }
    }

    /// Saturating addition.
    #[must_use]
    pub fn saturating_add(self, rhs: Self) -> Self {
        if let Some(sum) = self.checked_add_impl(rhs) {
            return sum;
        }

        let target_scale = self.scale.max(rhs.scale);

        if self.is_sign_negative() == rhs.is_sign_negative() {
            return Self::saturating_extreme(target_scale, self.is_sign_negative());
        }

        match self.cmp_decimal(&rhs) {
            Ordering::Equal => Self {
                mantissa: 0,
                scale: target_scale,
            },
            Ordering::Greater => Self::saturating_extreme(target_scale, self.is_sign_negative()),
            Ordering::Less => Self::saturating_extreme(target_scale, rhs.is_sign_negative()),
        }
    }

    /// Saturating subtraction.
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        self.saturating_add(Self {
            mantissa: rhs.mantissa.saturating_neg(),
            scale: rhs.scale,
        })
    }

    /// Checked remainder; returns `None` on division by zero.
    #[must_use]
    pub fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem_impl(rhs)
    }

    #[must_use]
    /// Integer exponentiation.
    pub fn powu(&self, exp: u64) -> Self {
        if exp == 0 {
            return Self::new(1, 0);
        }

        let mut base = *self;
        let mut power = exp;
        let mut acc = Self::new(1, 0);

        while power > 0 {
            if power & 1 == 1 {
                acc *= base;
            }

            power >>= 1;

            if power > 0 {
                base = base * base;
            }
        }

        acc
    }

    #[must_use]
    /// Build from a raw mantissa and scale.
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        Self::checked_from_mantissa_scale(num, scale).unwrap_or(Self::ZERO)
    }

    #[must_use]
    /// Normalize trailing zeros.
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

    const fn saturating_extreme(scale: u32, negative: bool) -> Self {
        let mantissa = if negative { i128::MIN } else { i128::MAX };
        Self { mantissa, scale }
    }

    fn saturating_mul(self, rhs: Self) -> Self {
        if self.is_zero() || rhs.is_zero() {
            return Self::ZERO;
        }

        let scale = self
            .scale
            .saturating_add(rhs.scale)
            .min(MAX_SUPPORTED_SCALE);
        let negative = self.is_sign_negative() != rhs.is_sign_negative();
        Self::saturating_extreme(scale, negative)
    }

    fn cmp_decimal(&self, other: &Self) -> Ordering {
        let (lhs_m, lhs_s) = normalize_parts(self.mantissa, self.scale);
        let (rhs_m, rhs_s) = normalize_parts(other.mantissa, other.scale);

        if lhs_m == rhs_m && lhs_s == rhs_s {
            return Ordering::Equal;
        }

        if lhs_m == 0 {
            return if rhs_m.is_negative() {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        if rhs_m == 0 {
            return if lhs_m.is_negative() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        if lhs_m.is_negative() != rhs_m.is_negative() {
            return if lhs_m.is_negative() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let positive = !lhs_m.is_negative();
        let mut lhs_digits = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
        let mut rhs_digits = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
        let lhs_len = write_u128_decimal_digits(lhs_m.unsigned_abs(), &mut lhs_digits);
        let rhs_len = write_u128_decimal_digits(rhs_m.unsigned_abs(), &mut rhs_digits);

        let lhs_exponent = compare_exponent(lhs_s, lhs_len).unwrap_or(i64::MIN);
        let rhs_exponent = compare_exponent(rhs_s, rhs_len).unwrap_or(i64::MIN);

        let exponent_cmp = lhs_exponent.cmp(&rhs_exponent);
        if exponent_cmp != Ordering::Equal {
            return if positive {
                exponent_cmp
            } else {
                exponent_cmp.reverse()
            };
        }

        let significand_cmp =
            cmp_significand_digits(&lhs_digits[..lhs_len], &rhs_digits[..rhs_len]);
        if positive {
            significand_cmp
        } else {
            significand_cmp.reverse()
        }
    }
}

impl AsView for Decimal {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Atomic for Decimal {}

impl CandidType for Decimal {
    fn _ty() -> candid::types::Type {
        candid::types::TypeInner::Text.into()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(&self.to_string())
    }
}

// Serde:
// - Human-readable formats (e.g. JSON) use a decimal string for API ergonomics.
// - Non-human-readable formats (e.g. CBOR persistence) use compact binary parts.
impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            return serializer.serialize_str(&self.to_string());
        }

        let mantissa_bytes = self.mantissa().to_be_bytes();
        (Bytes::new(&mantissa_bytes), self.scale()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum DecimalPayload {
            Binary((ByteBuf, u32)),
            Text(String),
        }

        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            return s.parse::<Self>().map_err(serde::de::Error::custom);
        }

        // Candid currently reports non-human-readable, but Decimal's Candid wire type is `text`.
        // Accept both payloads here so Candid decode remains correct while binary formats
        // continue to use the canonical `(mantissa_bytes, scale)` shape.
        let payload: DecimalPayload = Deserialize::deserialize(deserializer)?;
        let (mantissa_bytes, scale) = match payload {
            DecimalPayload::Binary(parts) => parts,
            DecimalPayload::Text(s) => {
                return s.parse::<Self>().map_err(serde::de::Error::custom);
            }
        };

        if mantissa_bytes.len() != 16 {
            return Err(serde::de::Error::custom(format!(
                "invalid decimal mantissa length: {} bytes (expected 16)",
                mantissa_bytes.len()
            )));
        }

        let mut mantissa_buf = [0u8; 16];
        mantissa_buf.copy_from_slice(mantissa_bytes.as_ref());
        let mantissa = i128::from_be_bytes(mantissa_buf);

        Self::checked_from_mantissa_scale(mantissa, scale)
            .ok_or_else(|| serde::de::Error::custom("invalid decimal binary payload"))
    }
}

impl FieldValue for Decimal {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Decimal(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumFromPrimitive for Decimal {
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self {
            mantissa: <i128 as From<i64>>::from(n),
            scale: 0,
        })
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self {
            mantissa: <i128 as From<u64>>::from(n),
            scale: 0,
        })
    }

    fn from_f32(n: f32) -> Option<Self> {
        Self::from_f32_lossy(n)
    }

    fn from_f64(n: f64) -> Option<Self> {
        Self::from_f64_lossy(n)
    }
}

// lossy f32 done on purpose as these ORM floats aren't designed for NaN etc.
impl From<f32> for Decimal {
    fn from(n: f32) -> Self {
        Self::from_f32(n).unwrap_or(Self::ZERO)
    }
}

impl From<f64> for Decimal {
    fn from(n: f64) -> Self {
        Self::from_f64(n).unwrap_or(Self::ZERO)
    }
}

macro_rules! impl_decimal_from_signed_int {
    ( $( $type:ty ),* ) => {
        $(
            impl From<$type> for Decimal {
                fn from(n: $type) -> Self {
                    Self {
                        mantissa: <i128 as From<$type>>::from(n),
                        scale: 0,
                    }
                }
            }
        )*
    };
}

macro_rules! impl_decimal_from_unsigned_int {
    ( $( $type:ty ),* ) => {
        $(
            impl From<$type> for Decimal {
                fn from(n: $type) -> Self {
                    Self {
                        mantissa: <i128 as From<$type>>::from(n),
                        scale: 0,
                    }
                }
            }
        )*
    };
}

impl_decimal_from_unsigned_int!(u8, u16, u32, u64);
impl_decimal_from_signed_int!(i8, i16, i32, i64, i128);

impl From<u128> for Decimal {
    fn from(n: u128) -> Self {
        let mantissa = i128::try_from(n).unwrap_or(i128::MAX);
        Self { mantissa, scale: 0 }
    }
}

impl Add for Decimal {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.saturating_add(rhs)
    }
}

impl AddAssign for Decimal {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl Sub for Decimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        self.saturating_sub(rhs)
    }
}

impl SubAssign for Decimal {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl Mul for Decimal {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        self.checked_mul_impl(rhs)
            .unwrap_or_else(|| self.saturating_mul(rhs))
    }
}

impl MulAssign for Decimal {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

impl Div for Decimal {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        if rhs.is_zero() {
            return Self::ZERO;
        }

        self.checked_div_impl(rhs).unwrap_or_else(|| {
            let negative = self.is_sign_negative() != rhs.is_sign_negative();
            Self::saturating_extreme(DEFAULT_DIVISION_SCALE, negative)
        })
    }
}

impl DivAssign for Decimal {
    fn div_assign(&mut self, rhs: Self) {
        *self = *self / rhs;
    }
}

impl Rem for Decimal {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        self.checked_rem_impl(rhs).unwrap_or(Self::ZERO)
    }
}

impl Sum for Decimal {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, |acc, value| acc + value)
    }
}

impl NumCast for Decimal {
    // NumCast is kept for ecosystem compatibility but remains lossy for float-ish inputs.
    // Prefer exact integer constructors or explicit `from_f64_lossy` at call sites.
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        Self::from_f64_lossy(n.to_f64()?)
    }
}

impl NumToPrimitive for Decimal {
    fn to_i32(&self) -> Option<i32> {
        self.to_i64().and_then(|v| i32::try_from(v).ok())
    }

    fn to_i64(&self) -> Option<i64> {
        let integer = decimal_integer_value(self.mantissa, self.scale)?;
        i64::try_from(integer).ok()
    }

    fn to_u64(&self) -> Option<u64> {
        let integer = decimal_integer_value(self.mantissa, self.scale)?;
        u64::try_from(integer).ok()
    }

    fn to_u128(&self) -> Option<u128> {
        let integer = decimal_integer_value(self.mantissa, self.scale)?;
        u128::try_from(integer).ok()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn to_f32(&self) -> Option<f32> {
        self.to_f64().and_then(|v| {
            let f = v as f32;
            if f.is_finite() { Some(f) } else { None }
        })
    }

    #[allow(clippy::cast_precision_loss)]
    fn to_f64(&self) -> Option<f64> {
        let divisor = 10f64.powi(i32::try_from(self.scale).ok()?);
        let value = (self.mantissa as f64) / divisor;
        if value.is_finite() { Some(value) } else { None }
    }
}

impl Display for Decimal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (mantissa, scale) = self.normalized_parts();

        if mantissa == 0 {
            return f.write_str("0");
        }

        let negative = mantissa.is_negative();
        let mut digits = mantissa.unsigned_abs().to_string();

        if scale == 0 {
            if negative {
                return write!(f, "-{digits}");
            }

            return f.write_str(&digits);
        }

        let scale_usize = usize::try_from(scale).map_err(|_| std::fmt::Error)?;

        if digits.len() <= scale_usize {
            let zeros = "0".repeat(scale_usize - digits.len());
            let body = format!("0.{zeros}{digits}");
            if negative {
                write!(f, "-{body}")
            } else {
                f.write_str(&body)
            }
        } else {
            let split = digits.len() - scale_usize;
            let frac = digits.split_off(split);
            if negative {
                write!(f, "-{digits}.{frac}")
            } else {
                write!(f, "{digits}.{frac}")
            }
        }
    }
}

impl FromStr for Decimal {
    type Err = ParseDecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Phase 1: parse sign.
        let input = s.trim();
        if input.is_empty() {
            return Err(ParseDecimalError::new("empty decimal string"));
        }

        let (negative, unsigned) = if let Some(rest) = input.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = input.strip_prefix('+') {
            (false, rest)
        } else {
            (false, input)
        };

        // Exponent notation is intentionally unsupported for predictable decimal
        // parsing semantics in 0.23.
        if unsigned.contains(['e', 'E']) {
            return Err(ParseDecimalError::new("exponent notation is not supported"));
        }

        // Phase 2: parse base-10 digits and decimal point.
        let (int_digits, frac_digits) = split_decimal_significand(unsigned)?;
        let combined = format!("{int_digits}{frac_digits}");
        let combined = strip_leading_zeros(&combined);

        let scale_i64 = i64::try_from(frac_digits.len())
            .map_err(|_| ParseDecimalError::new("decimal fractional length overflow"))?;
        let digits = combined.to_string();

        let scale = u32::try_from(scale_i64)
            .map_err(|_| ParseDecimalError::new("decimal scale overflow"))?;

        // Phase 3: materialize mantissa without floating-point fallback.
        let signed_digits = if negative {
            format!("-{digits}")
        } else {
            digits
        };
        let mantissa = signed_digits
            .parse::<i128>()
            .map_err(|_| ParseDecimalError::new("decimal mantissa overflow"))?;

        Self::checked_from_mantissa_scale(mantissa, scale)
            .ok_or_else(|| ParseDecimalError::new("decimal scale exceeds supported range"))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        let (lhs_m, lhs_s) = self.normalized_parts();
        let (rhs_m, rhs_s) = other.normalized_parts();
        lhs_m == rhs_m && lhs_s == rhs_s
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_decimal(other)
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (mantissa, scale) = self.normalized_parts();
        mantissa.hash(state);
        scale.hash(state);
    }
}

impl SanitizeAuto for Decimal {}

impl SanitizeCustom for Decimal {}

impl ValidateAuto for Decimal {}

impl ValidateCustom for Decimal {}

impl Visitable for Decimal {}

const fn checked_pow10(power: u32) -> Option<i128> {
    10i128.checked_pow(power)
}

fn decimal_integer_value(mantissa: i128, scale: u32) -> Option<i128> {
    if scale == 0 {
        return Some(mantissa);
    }

    let divisor = checked_pow10(scale)?;
    if mantissa % divisor != 0 {
        return None;
    }

    Some(mantissa / divisor)
}

fn align_to_scale(mantissa: i128, current_scale: u32, target_scale: u32) -> Option<i128> {
    if current_scale == target_scale {
        return Some(mantissa);
    }

    let factor = checked_pow10(target_scale.checked_sub(current_scale)?)?;
    mantissa.checked_mul(factor)
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

// Prepare integer operands for fixed-scale decimal division.
fn division_operands(lhs: Decimal, rhs: Decimal, target_scale: u32) -> Option<(i128, i128)> {
    let exponent = <i64 as From<u32>>::from(target_scale) + <i64 as From<u32>>::from(rhs.scale)
        - <i64 as From<u32>>::from(lhs.scale);

    if exponent >= 0 {
        let factor = checked_pow10(u32::try_from(exponent).ok()?)?;
        let numerator = lhs.mantissa.checked_mul(factor)?;
        return Some((numerator, rhs.mantissa));
    }

    let factor = checked_pow10(u32::try_from(exponent.unsigned_abs()).ok()?)?;
    let denominator = rhs.mantissa.checked_mul(factor)?;
    Some((lhs.mantissa, denominator))
}

// Divide with round-half-away-from-zero semantics.
fn div_round_half_away_from_zero(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator == 0 {
        return None;
    }

    let quotient = numerator / denominator;
    let remainder = numerator % denominator;

    if remainder == 0 {
        return Some(quotient);
    }

    let twice_remainder = remainder.unsigned_abs().checked_mul(2)?;
    if twice_remainder < denominator.unsigned_abs() {
        return Some(quotient);
    }

    if (numerator < 0) == (denominator < 0) {
        quotient.checked_add(1)
    } else {
        quotient.checked_sub(1)
    }
}

fn write_u128_decimal_digits(mut value: u128, out: &mut [u8; DECIMAL_DIGIT_BUFFER_LEN]) -> usize {
    let mut write_idx = DECIMAL_DIGIT_BUFFER_LEN;

    loop {
        write_idx = write_idx.saturating_sub(1);
        out[write_idx] = match value % 10 {
            0 => b'0',
            1 => b'1',
            2 => b'2',
            3 => b'3',
            4 => b'4',
            5 => b'5',
            6 => b'6',
            7 => b'7',
            8 => b'8',
            9 => b'9',
            _ => unreachable!("decimal digit remainder must be in 0..=9"),
        };
        value /= 10;

        if value == 0 {
            break;
        }
    }

    let len = DECIMAL_DIGIT_BUFFER_LEN.saturating_sub(write_idx);
    out.copy_within(write_idx..DECIMAL_DIGIT_BUFFER_LEN, 0);
    len
}

fn compare_exponent(scale: u32, digit_len: usize) -> Option<i64> {
    let digit_count = i64::try_from(digit_len).ok()?;
    let scale = <i64 as From<u32>>::from(scale);
    digit_count.checked_sub(1)?.checked_sub(scale)
}

fn cmp_significand_digits(lhs: &[u8], rhs: &[u8]) -> Ordering {
    let width = lhs.len().max(rhs.len());
    for idx in 0..width {
        let l = lhs.get(idx).copied().unwrap_or(b'0');
        let r = rhs.get(idx).copied().unwrap_or(b'0');
        let cmp = l.cmp(&r);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    Ordering::Equal
}

fn split_decimal_significand(input: &str) -> Result<(&str, &str), ParseDecimalError> {
    let mut segments = input.split('.');
    let int_digits = segments
        .next()
        .ok_or_else(|| ParseDecimalError::new("invalid decimal significand"))?;
    let frac_digits = segments.next().unwrap_or("");

    if segments.next().is_some() {
        return Err(ParseDecimalError::new("invalid decimal significand"));
    }

    if int_digits.is_empty() && frac_digits.is_empty() {
        return Err(ParseDecimalError::new("invalid decimal significand"));
    }

    if !int_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new("invalid decimal digits"));
    }

    if !frac_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new("invalid decimal digits"));
    }

    Ok((int_digits, frac_digits))
}

fn strip_leading_zeros(digits: &str) -> &str {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() { "0" } else { trimmed }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{decode_one, encode_one};
    use proptest::prelude::*;
    use std::str::FromStr;

    #[test]
    fn decimal_candid_roundtrip() {
        let cases = [
            "0",
            "1",
            "-1",
            "42.5",
            "1234567890.123456789",
            "0.00000001",
            "1000000000000000000.000000000000000001",
        ];

        for s in cases {
            let d1 = Decimal::from_str(s).expect("parse decimal");

            // encode via Candid (should encode as text)
            let bytes = encode_one(d1).expect("candid encode");

            // decode back to Decimal
            let d2: Decimal = decode_one(&bytes).expect("candid decode to Decimal");
            assert_eq!(d2, d1, "roundtrip mismatch for {s}");

            // also ensure the on-wire representation is text by decoding as String
            let wire_str: String = decode_one(&bytes).expect("candid decode to String");
            assert_eq!(wire_str, d1.to_string(), "wire text mismatch for {s}");
        }
    }

    #[test]
    fn decimal_serde_json_string_roundtrip() {
        let cases = [
            "0",
            "1",
            "-1",
            "42.5",
            "1234567890.123456789",
            "0.00000001",
            "1000000000000000000.000000000000000001",
        ];

        for s in cases {
            let d = Decimal::from_str(s).expect("parse decimal");

            // Serialize to JSON: must be a JSON string containing the decimal text
            let json = serde_json::to_string(&d).expect("serde_json serialize");
            let expected = serde_json::to_string(&d.to_string()).unwrap();
            assert_eq!(json, expected, "JSON encoding should be a string for {s}");

            // Deserialize back and compare
            let back: Decimal = serde_json::from_str(&json).expect("serde_json deserialize");
            assert_eq!(back, d, "serde_json roundtrip mismatch for {s}");
        }
    }

    #[test]
    fn decimal_serde_cbor_binary_roundtrip() {
        let cases = [
            "0",
            "1",
            "-1",
            "42.5",
            "1234567890.123456789",
            "0.00000001",
            "1000000000000000000.000000000000000001",
        ];

        for s in cases {
            let d1 = Decimal::from_str(s).expect("parse decimal");

            let bytes = serde_cbor::to_vec(&d1).expect("cbor serialize");
            let d2: Decimal = serde_cbor::from_slice(&bytes).expect("cbor deserialize");
            assert_eq!(d2, d1, "cbor roundtrip mismatch for {s}");

            let wire: serde_cbor::Value =
                serde_cbor::from_slice(&bytes).expect("decode cbor value");
            match wire {
                serde_cbor::Value::Array(values) => {
                    assert_eq!(values.len(), 2, "expected [mantissa_bytes, scale] for {s}");
                    assert!(
                        matches!(values.first(), Some(serde_cbor::Value::Bytes(_))),
                        "expected mantissa bytes in first position for {s}"
                    );
                }
                other => panic!("expected binary decimal array payload for {s}; got {other:?}"),
            }
        }
    }

    #[test]
    fn decimal_serde_cbor_rejects_invalid_binary_mantissa_length() {
        let invalid = serde_cbor::to_vec(&(vec![1u8, 2, 3], 2u32))
            .expect("serialize invalid binary mantissa payload");
        let parsed: Result<Decimal, _> = serde_cbor::from_slice(&invalid);
        assert!(
            parsed.is_err(),
            "invalid binary mantissa length must be rejected"
        );
    }

    #[test]
    fn decimal_serde_cbor_rejects_invalid_binary_scale() {
        let invalid = serde_cbor::to_vec(&(123_i128.to_be_bytes().to_vec(), 29u32))
            .expect("serialize invalid decimal scale payload");
        let parsed: Result<Decimal, _> = serde_cbor::from_slice(&invalid);
        assert!(parsed.is_err(), "invalid binary scale must be rejected");
    }

    #[test]
    fn decimal_division_is_fixed_scale_and_rounded() {
        let one = Decimal::new(1, 0);
        let third = one / Decimal::new(3, 0);
        let sixth = one / Decimal::new(6, 0);
        let neg_sixth = Decimal::new(-1, 0) / Decimal::new(6, 0);

        assert_eq!(third.to_string(), "0.333333333333333333");
        assert_eq!(sixth.to_string(), "0.166666666666666667");
        assert_eq!(neg_sixth.to_string(), "-0.166666666666666667");
    }

    #[test]
    fn decimal_div_by_zero_returns_zero() {
        let value = Decimal::new(123, 2);
        assert_eq!(value / Decimal::ZERO, Decimal::ZERO);
    }

    #[test]
    fn decimal_parse_rejects_mantissa_overflow_without_float_fallback() {
        let too_large = "340282366920938463463374607431768211456";
        assert!(Decimal::from_str(too_large).is_err());
    }

    #[test]
    fn decimal_parse_rejects_exponent_notation() {
        assert!(Decimal::from_str("1e3").is_err());
        assert!(Decimal::from_str("1E3").is_err());
    }

    #[test]
    fn decimal_try_new_rejects_scale_over_max() {
        assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE).is_some());
        assert!(Decimal::try_new(1, MAX_SUPPORTED_SCALE + 1).is_none());
    }

    #[test]
    #[should_panic(expected = "decimal scale exceeds supported range")]
    fn decimal_new_panics_on_scale_over_max() {
        let _ = Decimal::new(1, MAX_SUPPORTED_SCALE + 1);
    }

    #[test]
    fn decimal_new_unchecked_allows_scale_over_max() {
        let d = Decimal::new_unchecked(1, MAX_SUPPORTED_SCALE + 1);
        assert_eq!(d.scale(), MAX_SUPPORTED_SCALE + 1);
    }

    #[test]
    fn decimal_add_overflow_saturates() {
        let max = Decimal::from_i128_with_scale(i128::MAX, 0);
        let min = Decimal::from_i128_with_scale(i128::MIN, 0);

        assert_eq!((max + Decimal::new(1, 0)).mantissa(), i128::MAX);
        assert_eq!((min + Decimal::new(-1, 0)).mantissa(), i128::MIN);
    }

    #[test]
    fn decimal_mul_overflow_saturates() {
        let positive = Decimal::from_i128_with_scale(i128::MAX / 2 + 1, 0);
        let negative = Decimal::from_i128_with_scale(i128::MIN, 0);

        assert_eq!((positive * Decimal::new(2, 0)).mantissa(), i128::MAX);
        assert_eq!((negative * Decimal::new(2, 0)).mantissa(), i128::MIN);
    }

    #[test]
    fn decimal_division_sign_scale_matrix() {
        let sign_cases = [
            (1i128, 1i128, false),
            (1i128, -1i128, true),
            (-1i128, 1i128, true),
            (-1i128, -1i128, false),
        ];
        let scales = [0u32, 1u32, 8u32, 18u32];

        for (lhs_sign, rhs_sign, expected_negative) in sign_cases {
            for lhs_scale in scales {
                for rhs_scale in scales {
                    let lhs = Decimal::from_i128_with_scale(lhs_sign * 25, lhs_scale);
                    let rhs = Decimal::from_i128_with_scale(rhs_sign * 5, rhs_scale);
                    let out = lhs / rhs;

                    assert!(
                        out.scale() <= DEFAULT_DIVISION_SCALE,
                        "lhs={lhs:?}, rhs={rhs:?}, out={out:?}"
                    );
                    assert!(
                        !out.is_zero(),
                        "division matrix should not produce zero for non-zero operands"
                    );
                    assert_eq!(
                        out.is_sign_negative(),
                        expected_negative,
                        "lhs={lhs:?}, rhs={rhs:?}, out={out:?}"
                    );
                }
            }
        }
    }

    proptest! {
        #[test]
        fn decimal_add_saturation_boundary_property(
            lhs_m in any::<i128>(),
            rhs_m in any::<i128>(),
            lhs_scale in 0u32..=18,
            rhs_scale in 0u32..=18,
        ) {
            let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
            let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
            let out = lhs + rhs;
            let target_scale = lhs_scale.max(rhs_scale);

            prop_assert_eq!(
                out.scale(),
                target_scale,
                "addition result scale must stay on max operand scale"
            );

            if let Some(exact) = lhs.checked_add_impl(rhs) {
                prop_assert_eq!(out, exact);
            } else {
                prop_assert!(
                    out.mantissa() == i128::MAX
                        || out.mantissa() == i128::MIN
                        || out.mantissa() == 0,
                    "overflow path must saturate deterministically"
                );
            }
        }

        #[test]
        fn decimal_division_non_zero_sign_property(
            lhs_m in any::<i128>().prop_filter("lhs non-zero", |v| *v != 0),
            rhs_m in any::<i128>().prop_filter("rhs non-zero", |v| *v != 0),
            lhs_scale in 0u32..=18,
            rhs_scale in 0u32..=18,
        ) {
            let lhs = Decimal::from_i128_with_scale(lhs_m, lhs_scale);
            let rhs = Decimal::from_i128_with_scale(rhs_m, rhs_scale);
            let out = lhs / rhs;

            prop_assert!(out.scale() <= DEFAULT_DIVISION_SCALE);

            if !out.is_zero() {
                prop_assert_eq!(
                    out.is_sign_negative(),
                    lhs.is_sign_negative() ^ rhs.is_sign_negative(),
                    "non-zero quotient sign must follow operand signs"
                );
            }
        }
    }
}
