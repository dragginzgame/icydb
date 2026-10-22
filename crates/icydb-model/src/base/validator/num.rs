//! Module: base::validator::num
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{base::helper::try_cast_decimal, prelude::*, schema::NumericValue, visitor::Validator};
use std::any::type_name;

/// Convert a numeric value into Decimal during *validation* time.
fn cast_decimal_val<N: NumericValue>(value: &N, ctx: &mut dyn VisitorContext) -> Option<Decimal> {
    try_cast_decimal(value).or_else(|| {
        ctx.issue(format!(
            "value of type {} cannot be represented as Decimal",
            type_name::<N>()
        ));
        None
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
            invalid_configuration: bool,
        }

        impl $name {
            /// Configure a bound; unrepresentable bounds report an issue on validation.
            pub fn new<N: NumericValue>(target: N) -> Self {
                let target = try_cast_decimal(&target);

                Self {
                    target: target.unwrap_or_default(),
                    invalid_configuration: target.is_none(),
                }
            }
        }

        impl<N: NumericValue> Validator<N> for $name {
            fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
                if self.invalid_configuration {
                    ctx.issue("numeric validator bound cannot be represented as Decimal".to_string());
                    return;
                }
                let Some(v) = cast_decimal_val(value, ctx) else { return };

                if !(v $op self.target) {
                    ctx.issue(format!($msg, v, self.target));
                }
            }
        }
    };
}

cmp_validator!(Lt, <, "{} must be < {}");
cmp_validator!(Gt, >, "{} must be > {}");
cmp_validator!(Lte, <=, "{} must be <= {}");
cmp_validator!(Gte, >=, "{} must be >= {}");
cmp_validator!(Equal, ==, "{} must be == {}");
cmp_validator!(NotEqual, !=, "{} must be != {}");

///
/// Range
///

#[validator]
pub struct Range {
    min: Decimal,
    max: Decimal,
    invalid_configuration: bool,
}

impl Range {
    /// Configure bounds; unrepresentable bounds report an issue on validation.
    pub fn new<N: NumericValue>(min: N, max: N) -> Self {
        let min = try_cast_decimal(&min);
        let max = try_cast_decimal(&max);

        Self {
            min: min.unwrap_or_default(),
            max: max.unwrap_or_default(),
            invalid_configuration: min.is_none() || max.is_none(),
        }
    }
}

impl<N: NumericValue> Validator<N> for Range {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
        if self.invalid_configuration {
            ctx.issue("numeric validator bounds cannot be represented as Decimal".to_string());
            return;
        }
        let Some(v) = cast_decimal_val(value, ctx) else {
            return;
        };

        if v < self.min || v > self.max {
            ctx.issue(format!("{v} must be between {} and {}", self.min, self.max));
        }
    }
}

///
/// MultipleOf
///

#[validator]
pub struct MultipleOf {
    target: Decimal,
    invalid_configuration: bool,
}

impl MultipleOf {
    /// Configure a divisor; unrepresentable divisors report an issue on validation.
    pub fn new<N: NumericValue>(target: N) -> Self {
        let target = try_cast_decimal(&target);

        Self {
            target: target.unwrap_or_default(),
            invalid_configuration: target.is_none(),
        }
    }
}

impl<N: NumericValue> Validator<N> for MultipleOf {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
        if self.invalid_configuration {
            ctx.issue("multipleOf target cannot be represented as Decimal".to_string());
            return;
        }
        if self.target.is_zero() {
            ctx.issue("multipleOf target must be non-zero".to_string());
            return;
        }

        let Some(v) = cast_decimal_val(value, ctx) else {
            return;
        };

        if !(v % self.target).is_zero() {
            ctx.issue(format!("{v} is not a multiple of {}", self.target));
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::validator::decimal::MaxDecimalPlaces;

    struct TestCtx {
        issues: crate::visitor::VisitorIssues,
    }

    impl TestCtx {
        fn new() -> Self {
            Self {
                issues: crate::visitor::VisitorIssues::new(),
            }
        }
    }

    impl crate::visitor::VisitorContext for TestCtx {
        fn add_issue(&mut self, issue: crate::visitor::Issue) {
            self.issues.push(String::new(), issue);
        }

        fn add_issue_at(&mut self, _: crate::visitor::PathSegment, issue: crate::visitor::Issue) {
            self.add_issue(issue);
        }
    }

    fn assert_invalid_numeric_bound<N: NumericValue + Copy>(bound: N) {
        let validators: &[&dyn Validator<u128>] = &[
            &Lt::new(bound),
            &Gt::new(bound),
            &Lte::new(bound),
            &Gte::new(bound),
            &Equal::new(bound),
            &NotEqual::new(bound),
            &Range::new(bound, bound),
            &MultipleOf::new(bound),
        ];
        for validator in validators {
            for value in [0_u128, 1, 42] {
                for _ in 0..2 {
                    let mut ctx = TestCtx::new();
                    validator.validate(&value, &mut ctx);
                    assert_eq!(ctx.issues.get("").map(<[_]>::len), Some(1));
                }
            }
        }
    }

    #[test]
    fn invalid_numeric_validator_configuration_is_never_accepted() {
        assert_invalid_numeric_bound(u128::MAX);
        for bound in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1e40, -1e40] {
            assert_invalid_numeric_bound(bound);
        }
        for bound in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, f32::MAX] {
            assert_invalid_numeric_bound(bound);
        }
        for validator in [Range::new(0_u128, u128::MAX), Range::new(u128::MAX, 0)] {
            let mut ctx = TestCtx::new();
            validator.validate(&0_u128, &mut ctx);
            assert_eq!(ctx.issues.get("").map(<[_]>::len), Some(1));
        }
    }

    #[test]
    fn invalid_numeric_decimal_places_configuration_is_never_accepted() {
        for validator in [MaxDecimalPlaces::new(-1), MaxDecimalPlaces::new(u64::MAX)] {
            for value in [Decimal::ZERO, Decimal::from_i128_with_scale(1235, 3)] {
                let mut ctx = TestCtx::new();
                validator.validate(&value, &mut ctx);
                assert_eq!(ctx.issues.get("").map(<[_]>::len), Some(1));
            }
        }
    }

    #[test]
    fn numeric_validator_boundaries_and_defaults_remain_valid() {
        let maximum = Decimal::from_i128_with_scale(i128::MAX, 0);
        for validator in [
            &Gte::new(i128::MIN) as &dyn Validator<Decimal>,
            &Lte::new(i128::MAX),
            &Equal::new(i128::MAX as u128),
            &Range::new(i128::MIN, i128::MAX),
            &MultipleOf::new(i128::MAX),
        ] {
            let mut ctx = TestCtx::new();
            validator.validate(&maximum, &mut ctx);
            assert!(ctx.issues.is_empty());
        }
        let defaults: &[(&dyn Validator<i32>, &dyn Validator<i32>)] = &[
            (&Lt::default(), &Lt::new(0)),
            (&Gt::default(), &Gt::new(0)),
            (&Lte::default(), &Lte::new(0)),
            (&Gte::default(), &Gte::new(0)),
            (&Equal::default(), &Equal::new(0)),
            (&NotEqual::default(), &NotEqual::new(0)),
            (&Range::default(), &Range::new(0, 0)),
            (&MultipleOf::default(), &MultipleOf::new(0)),
        ];
        for (default, explicit) in defaults {
            for value in [-1, 0, 1] {
                let mut default_ctx = TestCtx::new();
                let mut explicit_ctx = TestCtx::new();
                default.validate(&value, &mut default_ctx);
                explicit.validate(&value, &mut explicit_ctx);
                assert_eq!(
                    default_ctx.issues.is_empty(),
                    explicit_ctx.issues.is_empty()
                );
            }
        }
        for (validator, value, valid) in [
            (MaxDecimalPlaces::default(), Decimal::ZERO, true),
            (
                MaxDecimalPlaces::new(0),
                Decimal::from_i128_with_scale(1, 1),
                false,
            ),
            (MaxDecimalPlaces::new(u32::MAX), maximum, true),
        ] {
            let mut ctx = TestCtx::new();
            validator.validate(&value, &mut ctx);
            assert_eq!(ctx.issues.is_empty(), valid);
        }
    }

    #[test]
    fn lt() {
        let v = Lt::new(10);
        let mut ctx = TestCtx::new();

        v.validate(&5, &mut ctx);
        assert!(ctx.issues.is_empty());

        v.validate(&10, &mut ctx);
        assert!(!ctx.issues.is_empty());
    }

    #[test]
    fn gte() {
        let v = Gte::new(5);
        let mut ctx = TestCtx::new();

        v.validate(&5, &mut ctx);
        assert!(ctx.issues.is_empty());

        v.validate(&4, &mut ctx);
        assert!(!ctx.issues.is_empty());
    }

    #[test]
    fn range() {
        let r = Range::new(1, 3);
        let mut ctx = TestCtx::new();

        r.validate(&2, &mut ctx);
        assert!(ctx.issues.is_empty());

        r.validate(&0, &mut ctx);
        assert!(!ctx.issues.is_empty());
    }

    #[test]
    fn multiple_of() {
        let m = MultipleOf::new(5);
        let mut ctx = TestCtx::new();

        m.validate(&10, &mut ctx);
        assert!(ctx.issues.is_empty());

        m.validate(&11, &mut ctx);
        assert!(!ctx.issues.is_empty());
    }

    #[test]
    fn multiple_of_decimal_scale_alignment_preserves_validation() {
        for scale in 1..=28 {
            for sign in [-1_i128, 1] {
                let value =
                    Decimal::from_i128_with_scale(sign * (5 * 10_i128.pow(scale) + 1), scale);
                for divisor in [-10_000_000_000_000_i64, 10_000_000_000_000] {
                    let mut ctx = TestCtx::new();
                    MultipleOf::new(divisor).validate(&value, &mut ctx);
                    assert!(
                        !ctx.issues.is_empty(),
                        "{value} is not a multiple of {divisor}"
                    );

                    let mut ctx = TestCtx::new();
                    MultipleOf::new(Decimal::from_i128_with_scale(sign, 28))
                        .validate(&value, &mut ctx);
                    assert!(
                        ctx.issues.is_empty(),
                        "every admitted decimal is a multiple of 1e-28"
                    );
                }
            }
        }
        let mut ctx = TestCtx::new();
        MultipleOf::new(-1).validate(&Decimal::from_i128_with_scale(i128::MIN, 0), &mut ctx);
        assert!(ctx.issues.is_empty());
        let mut ctx = TestCtx::new();
        MultipleOf::new(0).validate(&Decimal::ZERO, &mut ctx);
        assert!(!ctx.issues.is_empty());
    }
}
