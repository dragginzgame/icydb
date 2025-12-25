use crate::{
    core::{
        traits::{NumCast, Validator},
        visitor::ValidateIssue,
    },
    prelude::*,
};

use core::any::type_name;

// ============================================================================
// Helpers
// ============================================================================

/// Convert a numeric value into Decimal during *configuration* time.
///
/// This is infallible: failures are returned as `ValidateIssue` so they can
/// be surfaced during validation.
fn cast_decimal_cfg<N: NumCast + Clone>(value: &N) -> Result<Decimal, ValidateIssue> {
    <Decimal as NumCast>::from(value.clone()).ok_or_else(|| {
        ValidateIssue::invalid_config(format!(
            "{} cannot be represented as Decimal",
            type_name::<N>()
        ))
    })
}

/// Convert a numeric value into Decimal during *validation* time.
fn cast_decimal_val<N: NumCast + Clone>(value: &N) -> Result<Decimal, ValidateIssue> {
    <Decimal as NumCast>::from(value.clone()).ok_or_else(|| {
        ValidateIssue::validation(format!(
            "value of type {} cannot be represented as Decimal",
            type_name::<N>()
        ))
    })
}

// ============================================================================
// Comparison validators
// ============================================================================

macro_rules! cmp_validator {
    ($name:ident, $op:tt, $msg:expr) => {
        #[validator]
        pub struct $name {
            target: Decimal,
            #[serde(skip)]
            error: Option<ValidateIssue>,
        }

        impl $name {
            pub fn new<N: NumCast + Clone>(target: N) -> Self {
                match cast_decimal_cfg(&target) {
                    Ok(target) => Self {
                        target,
                        error: None,
                    },
                    Err(e) => Self {
                        target: Decimal::ZERO,
                        error: Some(e),
                    },
                }
            }
        }

        impl<N: NumCast + Clone> Validator<N> for $name {
            fn validate(&self, value: &N) -> Result<(), ValidateIssue> {
                if let Some(err) = &self.error {
                    return Err(err.clone());
                }

                let v = cast_decimal_val(value)?;
                if v $op self.target {
                    Ok(())
                } else {
                    Err(ValidateIssue::validation(format!(
                        $msg, v, self.target
                    )))
                }
            }
        }
    };
}

cmp_validator!(Lt, <,  "{} must be < {}");
cmp_validator!(Gt, >,  "{} must be > {}");
cmp_validator!(Lte, <=, "{} must be <= {}");
cmp_validator!(Gte, >=, "{} must be >= {}");
cmp_validator!(Equal, ==, "{} must be == {}");
cmp_validator!(NotEqual, !=, "{} must be != {}");

// ============================================================================
// Range
// ============================================================================

#[validator]
pub struct Range {
    min: Decimal,
    max: Decimal,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl Range {
    pub fn new<N: NumCast + Clone>(min: N, max: N) -> Self {
        let min = cast_decimal_cfg(&min);
        let max = cast_decimal_cfg(&max);

        match (min, max) {
            (Ok(min), Ok(max)) if min <= max => Self {
                min,
                max,
                error: None,
            },
            (Ok(min), Ok(max)) => Self {
                min,
                max,
                error: Some(ValidateIssue::invalid_config("range requires min <= max")),
            },
            (Err(e), _) | (_, Err(e)) => Self {
                min: Decimal::ZERO,
                max: Decimal::ZERO,
                error: Some(e),
            },
        }
    }
}

impl<N: NumCast + Clone> Validator<N> for Range {
    fn validate(&self, value: &N) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let v = cast_decimal_val(value)?;
        if v < self.min || v > self.max {
            Err(ValidateIssue::validation(format!(
                "{v} must be between {} and {}",
                self.min, self.max
            )))
        } else {
            Ok(())
        }
    }
}

// ============================================================================
// MultipleOf
// ============================================================================

#[validator]
pub struct MultipleOf {
    target: Decimal,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl MultipleOf {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        match cast_decimal_cfg(&target) {
            Ok(t) if !t.is_zero() => Self {
                target: t,
                error: None,
            },
            Ok(_) => Self {
                target: Decimal::ZERO,
                error: Some(ValidateIssue::invalid_config(
                    "MultipleOf target must be non-zero",
                )),
            },
            Err(e) => Self {
                target: Decimal::ZERO,
                error: Some(e),
            },
        }
    }
}

impl<N: NumCast + Clone> Validator<N> for MultipleOf {
    fn validate(&self, value: &N) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let v = cast_decimal_val(value)?;
        if (*v % *self.target).is_zero() {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "{v} is not a multiple of {}",
                self.target
            )))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use core::str::FromStr;

    fn dec(v: &str) -> Decimal {
        Decimal::from_str(v).unwrap()
    }

    #[test]
    fn lt() {
        assert!(Lt::new(10).validate(&5).is_ok());
        assert!(Lt::new(5).validate(&5).is_err());
    }

    #[test]
    fn gte() {
        assert!(Gte::new(5).validate(&5).is_ok());
        assert!(Gte::new(5).validate(&4).is_err());
    }

    #[test]
    fn range_valid() {
        let r = Range::new(1, 3);
        assert!(r.validate(&1).is_ok());
        assert!(r.validate(&2).is_ok());
        assert!(r.validate(&3).is_ok());
        assert!(r.validate(&0).is_err());
        assert!(r.validate(&4).is_err());
    }

    #[test]
    fn range_invalid_config() {
        let r = Range::new(5, 3);
        let err = r.validate(&5).unwrap_err();
        matches!(err, ValidateIssue::InvalidConfig(_));
    }

    #[test]
    fn multiple_of_int() {
        let m = MultipleOf::new(5);
        assert!(m.validate(&10).is_ok());
        assert!(m.validate(&11).is_err());
    }

    #[test]
    fn multiple_of_decimal() {
        let m = MultipleOf::new(dec("0.25"));
        assert!(m.validate(&dec("1.0")).is_ok());
        assert!(m.validate(&dec("1.1")).is_err());
    }

    #[test]
    fn multiple_of_invalid_config() {
        let m = MultipleOf::new(0);
        let err = m.validate(&10).unwrap_err();
        matches!(err, ValidateIssue::InvalidConfig(_));
    }
}
