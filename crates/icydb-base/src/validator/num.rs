use crate::{
    core::traits::{NumCast, Validator},
    prelude::*,
};

fn cast_decimal<N: NumCast + Clone>(value: &N) -> Result<Decimal, String> {
    <Decimal as NumCast>::from(value.clone()).ok_or_else(|| {
        format!(
            "value of type {} cannot be represented as Decimal",
            core::any::type_name::<N>()
        )
    })
}

///
/// Lt
///

#[validator]
pub struct Lt {
    target: Decimal,
}

impl Lt {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("Lt::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for Lt {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v < self.target {
            Ok(())
        } else {
            Err(format!("{} must be < {}", v, self.target))
        }
    }
}

///
/// Gt
///

#[validator]
pub struct Gt {
    target: Decimal,
}

impl Gt {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("Gt::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for Gt {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v > self.target {
            Ok(())
        } else {
            Err(format!("{} must be > {}", v, self.target))
        }
    }
}

///
/// Lte
///

#[validator]
pub struct Lte {
    target: Decimal,
}

impl Lte {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("Lte::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for Lte {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v <= self.target {
            Ok(())
        } else {
            Err(format!("{} must be <= {}", v, self.target))
        }
    }
}

///
/// Gte
///

#[validator]
pub struct Gte {
    target: Decimal,
}

impl Gte {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("Gte::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for Gte {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v >= self.target {
            Ok(())
        } else {
            Err(format!("{} must be >= {}", v, self.target))
        }
    }
}

///
/// Equal
///

#[validator]
pub struct Equal {
    target: Decimal,
}

impl Equal {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("Equal::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for Equal {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v == self.target {
            Ok(())
        } else {
            Err(format!("{} must be == {}", v, self.target))
        }
    }
}

///
/// NotEqual
///

#[validator]
pub struct NotEqual {
    target: Decimal,
}

impl NotEqual {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("NotEqual::new failed to convert target: {e}"));

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for NotEqual {
    fn validate(&self, value: &N) -> Result<(), String> {
        let v = cast_decimal(value)?;

        if v == self.target {
            Err(format!("{} must be != {}", v, self.target))
        } else {
            Ok(())
        }
    }
}

///
/// Range
///

#[validator]
pub struct Range {
    min: Decimal,
    max: Decimal,
}

impl Range {
    pub fn new<N: NumCast + Clone>(min: N, max: N) -> Self {
        let min =
            cast_decimal(&min).unwrap_or_else(|e| panic!("Range::new failed to convert min: {e}"));
        let max =
            cast_decimal(&max).unwrap_or_else(|e| panic!("Range::new failed to convert max: {e}"));
        assert!(min <= max, "range requires min <= max");

        Self { min, max }
    }
}

impl<N: NumCast + Clone> Validator<N> for Range {
    fn validate(&self, n: &N) -> Result<(), String> {
        let v = cast_decimal(n)?;

        if v < self.min || v > self.max {
            Err(format!(
                "{} must be between {} and {}",
                v, self.min, self.max
            ))
        } else {
            Ok(())
        }
    }
}

///
/// MultipleOf
///

#[validator]
pub struct MultipleOf {
    target: Decimal,
}

impl MultipleOf {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal(&target)
            .unwrap_or_else(|e| panic!("MultipleOf::new failed to convert target: {e}"));
        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for MultipleOf {
    fn validate(&self, n: &N) -> Result<(), String> {
        let v = cast_decimal(n)?;

        if (*v % *self.target).is_zero() {
            Ok(())
        } else {
            Err(format!("{v} is not a multiple of {}", self.target))
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(v: &str) -> Decimal {
        Decimal::from_str(v).unwrap()
    }

    // ---------------------
    // Lt
    // ---------------------

    #[test]
    fn lt_success() {
        assert!(Lt::new(10).validate(&5).is_ok());
        assert!(Lt::new(5.1).validate(&5.0).is_ok());
        assert!(Lt::new(dec("10.0")).validate(&dec("9.999")).is_ok());
    }

    #[test]
    fn lt_failure() {
        assert!(Lt::new(5).validate(&5).is_err());
        assert!(Lt::new(5).validate(&6).is_err());
    }

    // ---------------------
    // Gt
    // ---------------------

    #[test]
    fn gt_success() {
        assert!(Gt::new(5).validate(&10).is_ok());
        assert!(Gt::new(dec("1.0")).validate(&dec("1.0001")).is_ok());
    }

    #[test]
    fn gt_failure() {
        assert!(Gt::new(10).validate(&10).is_err());
        assert!(Gt::new(10).validate(&5).is_err());
    }

    // ---------------------
    // Lte
    // ---------------------

    #[test]
    fn lte_success() {
        assert!(Lte::new(5).validate(&5).is_ok());
        assert!(Lte::new(5).validate(&4).is_ok());
        assert!(Lte::new(dec("1.0")).validate(&dec("1.0")).is_ok());
    }

    #[test]
    fn lte_failure() {
        assert!(Lte::new(5).validate(&6).is_err());
    }

    // ---------------------
    // Gte
    // ---------------------

    #[test]
    fn gte_success() {
        assert!(Gte::new(5).validate(&5).is_ok());
        assert!(Gte::new(5).validate(&6).is_ok());
    }

    #[test]
    fn gte_failure() {
        assert!(Gte::new(5).validate(&4).is_err());
    }

    // ---------------------
    // Equal
    // ---------------------

    #[test]
    fn equal_success() {
        assert!(Equal::new(5).validate(&5).is_ok());
        assert!(Equal::new(dec("1.23")).validate(&dec("1.23")).is_ok());
    }

    #[test]
    fn equal_failure() {
        assert!(Equal::new(5).validate(&6).is_err());
        assert!(Equal::new(dec("1.23")).validate(&dec("1.2300001")).is_err());
    }

    // ---------------------
    // NotEqual
    // ---------------------

    #[test]
    fn not_equal_success() {
        assert!(NotEqual::new(5).validate(&6).is_ok());
        assert!(NotEqual::new(dec("1.23")).validate(&dec("1.2301")).is_ok());
    }

    #[test]
    fn not_equal_failure() {
        assert!(NotEqual::new(5).validate(&5).is_err());
        assert!(NotEqual::new(dec("1.23")).validate(&dec("1.23")).is_err());
    }

    // ---------------------
    // Range
    // ---------------------

    #[test]
    fn range_success() {
        let r = Range::new(0, 10);
        assert!(r.validate(&0).is_ok());
        assert!(r.validate(&5).is_ok());
        assert!(r.validate(&10).is_ok());

        let r2 = Range::new(dec("1.23"), dec("2.34"));
        assert!(r2.validate(&dec("1.23")).is_ok());
        assert!(r2.validate(&dec("2.34")).is_ok());
        assert!(r2.validate(&dec("1.5")).is_ok());
    }

    #[test]
    fn range_failure() {
        let r = Range::new(0, 10);
        assert!(r.validate(&-1).is_err());
        assert!(r.validate(&11).is_err());
        assert!(r.validate(&dec("-0.0001")).is_err());
    }

    #[test]
    fn range_min_equals_max() {
        let r = Range::new(5, 5);
        assert!(r.validate(&5).is_ok());
        assert!(r.validate(&4).is_err());
        assert!(r.validate(&6).is_err());
    }

    #[test]
    #[should_panic(expected = "range requires min <= max")]
    fn range_invalid_constructor() {
        Range::new(10, 5);
    }

    // ---------------------
    // MultipleOf
    // ---------------------

    #[test]
    fn multiple_of_int_success() {
        assert!(MultipleOf::new(5).validate(&10).is_ok());
        assert!(MultipleOf::new(5).validate(&0).is_ok());
    }

    #[test]
    fn multiple_of_int_failure() {
        assert!(MultipleOf::new(5).validate(&11).is_err());
        assert!(MultipleOf::new(3).validate(&10).is_err());
    }

    #[test]
    fn multiple_of_decimal_success() {
        assert!(MultipleOf::new(dec("0.5")).validate(&dec("2.5")).is_ok());
        assert!(MultipleOf::new(dec("0.1")).validate(&dec("1.2")).is_ok());
        assert!(MultipleOf::new(dec("1.25")).validate(&dec("6.25")).is_ok());
    }

    #[test]
    fn multiple_of_decimal_failure() {
        assert!(MultipleOf::new(dec("0.5")).validate(&dec("2.6")).is_err());
        assert!(MultipleOf::new(dec("0.1")).validate(&dec("1.23")).is_err());
    }

    #[test]
    fn multiple_of_zero_edge_case() {
        // Depending on your intended semantics, this may be allowed or not.
        // If target = 0 is illegal, this should panic during new().
        assert!(MultipleOf::new(1).validate(&0).is_ok());
    }

    #[test]
    #[should_panic]
    fn multiple_of_zero_target_panics() {
        let validator = MultipleOf::new(0);
        let _ = validator.validate(&1);
    }
}
