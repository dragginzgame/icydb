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
        #[doc = concat!("Require the numeric value to satisfy `value ", stringify!($op), " target`.")]
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
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Generated attachments pass literal, constant and function-result bounds by value"
    )]
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
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Generated validator attachments pass literal, constant and function-result arguments by value"
    )]
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
mod tests;
