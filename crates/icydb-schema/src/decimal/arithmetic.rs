use crate::decimal::{DEFAULT_DIVISION_SCALE, Decimal, MAX_SUPPORTED_SCALE};
use ethnum::I256;
use std::{
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Rem, RemAssign, Sub, SubAssign},
};

impl Decimal {
    fn checked_add_impl(self, rhs: Self) -> Option<Self> {
        let target_scale = self.scale.max(rhs.scale);
        let lhs = Self::align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = Self::align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

        Self::fit_wide_mantissa(lhs.checked_add(rhs)?, target_scale)
    }

    /// Checked addition rounds half away from zero to the greatest fitting
    /// scale, starting at the greater operand scale. Returns `None` only when
    /// the rounded magnitude cannot fit even at scale zero.
    #[must_use]
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add_impl(rhs)
    }

    /// Checked subtraction uses the same fitting/rounding policy as addition.
    /// Returns `None` when the rounded magnitude cannot fit at scale zero.
    #[must_use]
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        let target_scale = self.scale.max(rhs.scale);
        let lhs = Self::align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = Self::align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;
        Self::fit_wide_mantissa(lhs.checked_sub(rhs)?, target_scale)
    }

    /// Checked multiplication normalizes operand padding and rounds half away
    /// from zero to the greatest representable scale, at most 28. Returns `None`
    /// when the rounded magnitude cannot fit even at scale zero.
    #[must_use]
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul_impl(rhs)
    }

    /// Checked division rounds half away from zero to the greatest fitting
    /// scale, at most 18. Returns `None` when the divisor is zero or the rounded
    /// magnitude cannot fit even at scale zero.
    #[must_use]
    pub fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div_impl(rhs)
    }

    fn checked_mul_impl(self, rhs: Self) -> Option<Self> {
        // Accepted fixed-scale fields retain padding that must not consume
        // multiplication precision or cause avoidable intermediate overflow.
        let lhs = self.normalize();
        let rhs = rhs.normalize();
        let scale = lhs.scale.checked_add(rhs.scale)?;
        // Two i128 mantissas fit an exact I256 product. Keep that product until
        // the final scale is known, so retries never double-round a value.
        let product = I256::from(lhs.mantissa).checked_mul(I256::from(rhs.mantissa))?;
        Self::fit_wide_mantissa(product, scale)
    }

    // Reduce precision from the original exact mantissa, never a rounded retry.
    // Addition/subtraction and multiplication share this final fitting policy.
    fn fit_wide_mantissa(mantissa: I256, scale: u32) -> Option<Self> {
        let mut result_scale = scale.min(MAX_SUPPORTED_SCALE);
        let mut divisor = I256::new(10).checked_pow(scale - result_scale)?;
        loop {
            if let Some(mantissa) = Self::div_round_half_away_from_zero(mantissa, divisor) {
                return Some(Self {
                    mantissa,
                    scale: result_scale,
                });
            }
            if result_scale == 0 {
                return None;
            }
            result_scale -= 1;
            divisor = divisor.checked_mul(I256::new(10))?;
        }
    }

    fn checked_div_impl(self, rhs: Self) -> Option<Self> {
        if rhs.is_zero() {
            return None;
        }

        let lhs = self.normalize();
        let rhs = rhs.normalize();
        let mut target_scale = DEFAULT_DIVISION_SCALE;

        // An overflowing wide numerator implies the quotient cannot fit at
        // this scale: its unscaled denominator is at most an i128 magnitude.
        // Retry from the original operands when scaling or rounding cannot fit.
        loop {
            if let Some((numerator, denominator)) = Self::division_operands(lhs, rhs, target_scale)
                && let Some(mantissa) = Self::div_round_half_away_from_zero(numerator, denominator)
            {
                return Some(
                    Self {
                        mantissa,
                        scale: target_scale,
                    }
                    .normalize(),
                );
            }

            if target_scale == 0 {
                return None;
            }

            target_scale -= 1;
        }
    }

    fn checked_rem_impl(self, rhs: Self) -> Option<Self> {
        if rhs.is_zero() {
            return None;
        }

        let target_scale = self.scale.max(rhs.scale);
        // Scale differences are at most 28, so both alignment products fit
        // I256. One operand stays unscaled; the remainder magnitude is bounded
        // by both operands and therefore still fits the i128 representation.
        let lhs = Self::align_to_scale(self.mantissa, self.scale, target_scale)?;
        let rhs = Self::align_to_scale(rhs.mantissa, rhs.scale, target_scale)?;

        Some(Self {
            mantissa: i128::try_from(lhs.checked_rem(rhs)?).ok()?,
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

    /// Truncate toward zero to a given number of decimal places.
    #[must_use]
    pub const fn trunc_dp(&self, dp: u32) -> Self {
        if self.scale <= dp {
            return *self;
        }

        let diff = self.scale - dp;
        let Some(divisor) = Self::checked_pow10(diff) else {
            return *self;
        };

        Self {
            mantissa: self.mantissa / divisor,
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
        // Only true rounded magnitude overflow remains; overflowing addition
        // has same-sign operands.
        self.checked_add_impl(rhs)
            .unwrap_or_else(|| Self::saturating_extreme(self.is_sign_negative()))
    }

    /// Saturating subtraction.
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        // Operand ordering owns the difference's sign, including positive
        // overflow near the asymmetric signed MIN boundary.
        self.checked_sub(rhs)
            .unwrap_or_else(|| Self::saturating_extreme(self < rhs))
    }

    /// Exact remainder with the dividend's sign, at the greater operand scale.
    /// Returns `None` on division by zero; scale alignment cannot overflow.
    #[must_use]
    pub fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem_impl(rhs)
    }

    /// Checked absolute value; returns `None` for the one `i128::MIN`
    /// mantissa case that cannot be represented as positive `i128`.
    #[must_use]
    pub const fn checked_abs(&self) -> Option<Self> {
        let Some(mantissa) = self.mantissa.checked_abs() else {
            return None;
        };

        Some(Self {
            mantissa,
            scale: self.scale,
        })
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

    /// Checked integer exponentiation using the same exponentiation-by-squaring
    /// shape as `powu`, but failing instead of saturating on intermediate
    /// magnitude overflow. Each multiplication uses the same rounded
    /// fixed-representation contract as `checked_mul`.
    #[must_use]
    pub fn checked_powu(&self, exp: u64) -> Option<Self> {
        if exp == 0 {
            return Some(Self::new(1, 0));
        }

        let mut base = *self;
        let mut power = exp;
        let mut acc = Self::new(1, 0);

        while power > 0 {
            if power & 1 == 1 {
                acc = acc.checked_mul(base)?;
            }

            power >>= 1;

            if power > 0 {
                base = base.checked_mul(base)?;
            }
        }

        Some(acc)
    }

    // Admitted scale alignment fits I256 even when the i128 intermediate does
    // not. Addition, subtraction and exact remainder use this single owner.
    fn align_to_scale(mantissa: i128, current_scale: u32, target_scale: u32) -> Option<I256> {
        let factor = Self::checked_pow10(target_scale.checked_sub(current_scale)?)?;
        I256::from(mantissa).checked_mul(I256::from(factor))
    }

    // Prepare integer operands for fixed-scale decimal division.
    fn division_operands(lhs: Self, rhs: Self, target_scale: u32) -> Option<(I256, I256)> {
        let exponent = i64::from(target_scale) + i64::from(rhs.scale) - i64::from(lhs.scale);
        let factor = I256::new(10).checked_pow(u32::try_from(exponent.unsigned_abs()).ok()?)?;
        let lhs = I256::from(lhs.mantissa);
        let rhs = I256::from(rhs.mantissa);

        if exponent >= 0 {
            return Some((lhs.checked_mul(factor)?, rhs));
        }

        Some((lhs, rhs.checked_mul(factor)?))
    }

    // Divide with round-half-away-from-zero semantics.
    fn div_round_half_away_from_zero(numerator: I256, denominator: I256) -> Option<i128> {
        // Round before narrowing. An unrepresentable signed result, including
        // i128::MIN / -1, follows the caller's maintained overflow contract.
        let quotient = numerator.checked_div(denominator)?;
        let remainder = numerator.checked_rem(denominator)?;

        if remainder == 0 {
            return i128::try_from(quotient).ok();
        }

        let twice_remainder = remainder.unsigned_abs().checked_mul(2_u8.into())?;
        if twice_remainder < denominator.unsigned_abs() {
            return i128::try_from(quotient).ok();
        }

        let rounded = if (numerator < 0) == (denominator < 0) {
            quotient.checked_add(I256::new(1))?
        } else {
            quotient.checked_sub(I256::new(1))?
        };
        i128::try_from(rounded).ok()
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
        self.checked_mul_impl(rhs).unwrap_or_else(|| {
            Self::saturating_extreme(self.is_sign_negative() != rhs.is_sign_negative())
        })
    }
}

impl MulAssign for Decimal {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

impl Neg for Decimal {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            mantissa: self.mantissa.saturating_neg(),
            scale: self.scale,
        }
    }
}

impl Product for Decimal {
    fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::new_unchecked(1, 0), |acc, value| acc * value)
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
            Self::saturating_extreme(negative)
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

impl RemAssign for Decimal {
    fn rem_assign(&mut self, rhs: Self) {
        *self = *self % rhs;
    }
}

impl Sum for Decimal {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, |acc, value| acc + value)
    }
}
