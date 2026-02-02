use crate::{
    design::prelude::*,
    traits::{NumCast, Validator},
};
use std::any::type_name;

// ============================================================================
// Helpers
// ============================================================================

/// Convert a numeric value into Decimal during *configuration* time.
fn cast_decimal_cfg<N: NumCast + Clone>(value: &N) -> Decimal {
    <Decimal as NumCast>::from(value.clone()).unwrap_or_default()
}

/// Convert a numeric value into Decimal during *validation* time.
fn cast_decimal_val<N: NumCast + Clone>(
    value: &N,
    ctx: &mut dyn VisitorContext,
) -> Option<Decimal> {
    <Decimal as NumCast>::from(value.clone()).or_else(|| {
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
        }

        impl $name {
            pub fn new<N: NumCast + Clone>(target: N) -> Self {
                let target = cast_decimal_cfg(&target);

                Self { target }
            }
        }

        impl<N: NumCast + Clone> Validator<N> for $name {
            fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
                let Some(v) = cast_decimal_val(value, ctx) else { return };

                if !(v $op self.target) {
                    ctx.issue(format!($msg, v, self.target));
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
        let min = cast_decimal_cfg(&min);
        let max = cast_decimal_cfg(&max);

        Self { min, max }
    }
}

impl<N: NumCast + Clone> Validator<N> for Range {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
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
}

impl MultipleOf {
    pub fn new<N: NumCast + Clone>(target: N) -> Self {
        let target = cast_decimal_cfg(&target);

        Self { target }
    }
}

impl<N: NumCast + Clone> Validator<N> for MultipleOf {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
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
            self.issues
                .entry(String::new())
                .or_default()
                .push(issue.message);
        }

        fn add_issue_at(&mut self, _: crate::visitor::PathSegment, issue: crate::visitor::Issue) {
            self.add_issue(issue);
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
}
