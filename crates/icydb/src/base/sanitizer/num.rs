use crate::{
    base::helper::decimal_cast::try_cast_decimal,
    design::prelude::*,
    traits::{NumCast, Sanitizer},
};
use std::any::type_name;

///
/// Clamp
///

#[sanitizer]
pub struct Clamp {
    min: Decimal,
    max: Decimal,
}

impl Clamp {
    pub fn new<N: NumCast + Clone>(min: N, max: N) -> Self {
        let min = try_cast_decimal(&min).unwrap_or_default();
        let max = try_cast_decimal(&max).unwrap_or_default();

        Self { min, max }
    }
}

impl<T: NumCast + Clone> Sanitizer<T> for Clamp {
    fn sanitize(&self, value: &mut T) -> Result<(), String> {
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

        *value = <T as NumCast>::from(clamped).ok_or_else(|| {
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

#[sanitizer]
pub struct RoundDecimalPlaces {
    scale: u32,
}

impl RoundDecimalPlaces {
    #[must_use]
    pub fn new(scale: impl TryInto<u32>) -> Self {
        Self {
            scale: scale.try_into().unwrap_or_default(),
        }
    }
}

impl Sanitizer<Decimal> for RoundDecimalPlaces {
    fn sanitize(&self, value: &mut Decimal) -> Result<(), String> {
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
        clamp.sanitize(&mut v).unwrap();
        assert_eq!(v, 10);

        let mut v = 25;
        clamp.sanitize(&mut v).unwrap();
        assert_eq!(v, 20);

        let mut v = 15;
        clamp.sanitize(&mut v).unwrap();
        assert_eq!(v, 15);
    }

    #[test]
    fn clamp_invalid_config() {
        let clamp = Clamp::new(20, 10);

        let mut v = 15;
        assert!(clamp.sanitize(&mut v).is_err());
    }

    #[test]
    fn rounds_decimal_places_midpoint_away_from_zero() {
        let round = RoundDecimalPlaces::new(2);

        let mut v = dec("1.234");
        round.sanitize(&mut v).unwrap();
        assert_eq!(v, dec("1.23"));

        let mut v = dec("1.235");
        round.sanitize(&mut v).unwrap();
        assert_eq!(v, dec("1.24"));

        let mut v = dec("-1.235");
        round.sanitize(&mut v).unwrap();
        assert_eq!(v, dec("-1.24"));
    }
}
