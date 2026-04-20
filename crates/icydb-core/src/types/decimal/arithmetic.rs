use crate::types::decimal::{DEFAULT_DIVISION_SCALE, Decimal, MAX_SUPPORTED_SCALE};
use std::{
    cmp::Ordering,
    iter::Sum,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
};

impl Decimal {
    pub(in crate::types::decimal) fn checked_add_impl(self, rhs: Self) -> Option<Self> {
        let target_scale = self.scale.max(rhs.scale);
        let lhs = Self::align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = Self::align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

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
            if let Some((numerator, denominator)) = Self::division_operands(lhs, rhs, target_scale)
            {
                let mantissa = Self::div_round_half_away_from_zero(numerator, denominator)?;
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
        let lhs = Self::align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = Self::align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

        Some(Self {
            mantissa: lhs.checked_rem(rhs)?,
            scale: target_scale,
        })
    }

    /// Round to a given number of decimal places.
    #[must_use]
    pub const fn round_dp(&self, dp: u32) -> Self {
        if self.scale <= dp {
            return *self;
        }

        let diff = self.scale - dp;
        let Some(divisor) = Self::checked_pow10(diff) else {
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

    /// Return the absolute value of the decimal.
    #[must_use]
    pub const fn abs(&self) -> Self {
        Self {
            mantissa: self.mantissa.saturating_abs(),
            scale: self.scale,
        }
    }

    /// Return the greatest integral decimal less than or equal to the value.
    #[must_use]
    pub const fn floor_dp0(&self) -> Self {
        if self.scale == 0 {
            return *self;
        }

        let Some(divisor) = Self::checked_pow10(self.scale) else {
            return *self;
        };
        let quotient = self.mantissa / divisor;
        let remainder = self.mantissa % divisor;
        let integer = if self.mantissa.is_negative() && remainder != 0 {
            quotient.saturating_sub(1)
        } else {
            quotient
        };

        Self {
            mantissa: integer,
            scale: 0,
        }
    }

    /// Return the least integral decimal greater than or equal to the value.
    #[must_use]
    pub const fn ceil_dp0(&self) -> Self {
        if self.scale == 0 {
            return *self;
        }

        let Some(divisor) = Self::checked_pow10(self.scale) else {
            return *self;
        };
        let quotient = self.mantissa / divisor;
        let remainder = self.mantissa % divisor;
        let integer = if self.mantissa.is_positive() && remainder != 0 {
            quotient.saturating_add(1)
        } else {
            quotient
        };

        Self {
            mantissa: integer,
            scale: 0,
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

    /// Integer exponentiation.
    #[must_use]
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

    fn align_to_scale(mantissa: i128, current_scale: u32, target_scale: u32) -> Option<i128> {
        if current_scale == target_scale {
            return Some(mantissa);
        }

        let factor = Self::checked_pow10(target_scale.checked_sub(current_scale)?)?;
        mantissa.checked_mul(factor)
    }

    // Prepare integer operands for fixed-scale decimal division.
    fn division_operands(lhs: Self, rhs: Self, target_scale: u32) -> Option<(i128, i128)> {
        let exponent = i64::from(target_scale) + i64::from(rhs.scale) - i64::from(lhs.scale);

        if exponent >= 0 {
            let factor = Self::checked_pow10(u32::try_from(exponent).ok()?)?;
            let numerator = lhs.mantissa.checked_mul(factor)?;
            return Some((numerator, rhs.mantissa));
        }

        let factor = Self::checked_pow10(u32::try_from(exponent.unsigned_abs()).ok()?)?;
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
