use crate::{
    core::traits::{NumCast, Sanitizer},
    prelude::*,
};

///
/// Clamp
///

#[sanitizer]
pub struct Clamp {
    min: Decimal,
    max: Decimal,
}

impl Clamp {
    pub fn new<N: NumCast>(min: N, max: N) -> Self {
        let min = <Decimal as NumCast>::from(min).unwrap();
        let max = <Decimal as NumCast>::from(max).unwrap();
        assert!(min <= max, "clamp requires min <= max");

        Self { min, max }
    }
}

impl<T: NumCast + Clone> Sanitizer<T> for Clamp {
    fn sanitize(&self, value: T) -> T {
        let v = <Decimal as NumCast>::from(value).unwrap();

        let clamped = if v < self.min {
            self.min
        } else if v > self.max {
            self.max
        } else {
            v
        };

        // Convert clamped Decimal back into original type N
        <T as NumCast>::from(clamped).expect("clamped value must fit into target type")
    }
}

///
/// RoundDecimalPlaces
///
/// Rounds a `Decimal` to a fixed number of decimal places.
/// Defaults to midpoint-away-from-zero rounding, which is friendlier for
/// currency-style values than bankers rounding.
///

#[sanitizer]
pub struct RoundDecimalPlaces {
    scale: u32,
}

impl RoundDecimalPlaces {
    pub fn new(scale: u32) -> Self {
        Self { scale }
    }
}

impl Sanitizer<Decimal> for RoundDecimalPlaces {
    fn sanitize(&self, value: Decimal) -> Decimal {
        value.round_dp(self.scale)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn clamps_integers() {
        let clamp = Clamp::new(10, 20);

        assert_eq!(clamp.sanitize(5), 10, "below min should clamp to min");
        assert_eq!(clamp.sanitize(25), 20, "above max should clamp to max");
        assert_eq!(clamp.sanitize(15), 15, "within range should stay the same");
        assert_eq!(clamp.sanitize(10), 10, "exact min should stay the same");
        assert_eq!(clamp.sanitize(20), 20, "exact max should stay the same");
    }

    #[test]
    fn handles_edge_cases() {
        let clamp = Clamp::new(-10, -5);

        assert_eq!(clamp.sanitize(-20), -10, "below min clamps to min");
        assert_eq!(clamp.sanitize(-7), -7, "within range is untouched");
        assert_eq!(clamp.sanitize(-5), -5, "exact max stays");
        assert_eq!(clamp.sanitize(0), -5, "above max clamps to max");
    }

    #[test]
    fn rounds_decimal_places_midpoint_away_from_zero() {
        let round = RoundDecimalPlaces::new(2);

        assert_eq!(
            round.sanitize(Decimal::from_str("1.234").unwrap()),
            Decimal::from_str("1.23").unwrap(),
            "should round down when below midpoint"
        );
        assert_eq!(
            round.sanitize(Decimal::from_str("1.235").unwrap()),
            Decimal::from_str("1.24").unwrap(),
            "should round up at midpoint away from zero"
        );
        assert_eq!(
            round.sanitize(Decimal::from_str("-1.235").unwrap()),
            Decimal::from_str("-1.24").unwrap(),
            "negative midpoint should round away from zero"
        );
    }
}
