//! Module: base::normalizer::num
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{
    base::helper::try_cast_decimal, prelude::*, schema::NumericValue, visitor::Normalizer,
};
use std::any::type_name;

///
/// Clamp
///

#[normalizer]
pub struct Clamp {
    min: Decimal,
    max: Decimal,
    invalid_configuration: bool,
}

impl Clamp {
    /// Configure bounds; unrepresentable bounds make normalization fail without mutation.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Generated attachments pass literal, constant and function-result bounds by value"
    )]
    pub fn new<N: NumericValue>(min: N, max: N) -> Self {
        let min = try_cast_decimal(&min);
        let max = try_cast_decimal(&max);

        // Keep conversion failure distinct from a real zero bound. The derived
        // Default remains the valid zero interval used by existing callers.
        Self {
            min: min.unwrap_or_default(),
            max: max.unwrap_or_default(),
            invalid_configuration: min.is_none() || max.is_none(),
        }
    }
}

impl<T: NumericValue> Normalizer<T> for Clamp {
    fn normalize(&self, value: &mut T) -> Result<(), String> {
        if self.invalid_configuration {
            return Err("Clamp bounds cannot be represented as Decimal".to_string());
        }
        if self.min > self.max {
            return Err(format!(
                "Clamp requires min <= max (got {}..={})",
                self.min, self.max
            ));
        }

        let v = try_cast_decimal(value).ok_or_else(|| {
            format!(
                "value of type {} cannot be represented as Decimal",
                type_name::<T>()
            )
        })?;

        let clamped = if v < self.min {
            self.min
        } else if v > self.max {
            self.max
        } else {
            v
        };

        *value = T::try_from_decimal(clamped).ok_or_else(|| {
            format!(
                "clamped value cannot be represented as {}",
                type_name::<T>()
            )
        })?;

        Ok(())
    }
}

///
/// RoundDecimalPlaces
///

#[normalizer]
pub struct RoundDecimalPlaces {
    scale: u32,
    invalid_configuration: bool,
}

impl RoundDecimalPlaces {
    /// Configure a scale; failed conversion makes normalization fail without mutation.
    #[must_use]
    pub fn new(scale: impl TryInto<u32>) -> Self {
        let scale = scale.try_into().ok();
        Self {
            scale: scale.unwrap_or_default(),
            invalid_configuration: scale.is_none(),
        }
    }
}

impl Normalizer<Decimal> for RoundDecimalPlaces {
    fn normalize(&self, value: &mut Decimal) -> Result<(), String> {
        if self.invalid_configuration {
            return Err("decimal rounding scale cannot be represented as u32".to_string());
        }
        *value = value.round_dp(self.scale);

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
