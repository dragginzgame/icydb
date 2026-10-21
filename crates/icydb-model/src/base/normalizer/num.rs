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
mod tests {
    use super::*;
    use core::str::FromStr;

    fn dec(v: &str) -> Decimal {
        Decimal::from_str(v).unwrap()
    }

    #[test]
    fn clamps_integers() {
        let clamp = Clamp::new(10, 20);

        let mut v = 5;
        clamp.normalize(&mut v).unwrap();
        assert_eq!(v, 10);

        let mut v = 25;
        clamp.normalize(&mut v).unwrap();
        assert_eq!(v, 20);

        let mut v = 15;
        clamp.normalize(&mut v).unwrap();
        assert_eq!(v, 15);
    }

    #[test]
    fn clamp_invalid_config() {
        let clamp = Clamp::new(20, 10);

        let mut v = 15;
        assert!(clamp.normalize(&mut v).is_err());
    }

    #[test]
    fn invalid_numeric_clamp_configuration_preserves_values() {
        let clamps = [
            Clamp::new(0_u128, u128::MAX),
            Clamp::new(u128::MAX, 0),
            Clamp::new(u128::MAX, u128::MAX),
            Clamp::new(0.0_f64, f64::NAN),
            Clamp::new(f64::INFINITY, 1.0),
            Clamp::new(0.0, f64::NEG_INFINITY),
            Clamp::new(0.0, 1e40),
            Clamp::new(-1e40, 0.0),
            Clamp::new(0.0_f32, f32::MAX),
        ];
        for clamp in clamps {
            for original in [0_u128, 7, u128::MAX] {
                let mut value = original;
                for _ in 0..2 {
                    assert!(clamp.normalize(&mut value).is_err());
                    assert_eq!(value, original);
                }
            }
        }
    }

    #[test]
    fn invalid_numeric_rounding_configuration_preserves_values() {
        for round in [
            RoundDecimalPlaces::new(-1),
            RoundDecimalPlaces::new(u64::MAX),
        ] {
            for original in [dec("1.235"), dec("-1.235"), Decimal::ZERO] {
                let mut value = original;
                for _ in 0..2 {
                    assert!(round.normalize(&mut value).is_err());
                    assert_eq!(value, original);
                }
            }
        }
    }

    #[test]
    fn numeric_normalizer_boundaries_and_defaults_remain_valid() {
        let mut value = i128::MIN;
        Clamp::new(i128::MIN, i128::MAX)
            .normalize(&mut value)
            .unwrap();
        assert_eq!(value, i128::MIN);
        let mut value = i128::MAX as u128;
        Clamp::new(0_u128, i128::MAX as u128)
            .normalize(&mut value)
            .unwrap();
        assert_eq!(value, i128::MAX as u128);
        let mut value = dec("1.235");
        Clamp::new(dec("1.2"), dec("1.3"))
            .normalize(&mut value)
            .unwrap();
        assert_eq!(value, dec("1.235"));
        let mut value = 7;
        Clamp::default().normalize(&mut value).unwrap();
        assert_eq!(value, 0);
        for (round, expected) in [
            (RoundDecimalPlaces::default(), dec("1")),
            (RoundDecimalPlaces::new(2), dec("1.24")),
            (RoundDecimalPlaces::new(u32::MAX), dec("1.235")),
        ] {
            let mut value = dec("1.235");
            round.normalize(&mut value).unwrap();
            assert_eq!(value, expected);
        }
    }

    #[test]
    fn rounds_decimal_places_midpoint_away_from_zero() {
        let round = RoundDecimalPlaces::new(2);

        let mut v = dec("1.234");
        round.normalize(&mut v).unwrap();
        assert_eq!(v, dec("1.23"));

        let mut v = dec("1.235");
        round.normalize(&mut v).unwrap();
        assert_eq!(v, dec("1.24"));

        let mut v = dec("-1.235");
        round.normalize(&mut v).unwrap();
        assert_eq!(v, dec("-1.24"));
    }
}
