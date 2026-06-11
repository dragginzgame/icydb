use crate::{
    base::helper::try_cast_decimal,
    design::prelude::*,
    traits::{NumericValue, Validator},
};

/// Convert a numeric value into Decimal during *configuration* time.
fn cast_decimal_cfg<N: NumericValue>(value: &N) -> Decimal {
    try_cast_decimal(value).unwrap_or_default()
}

/// Convert a numeric value into Decimal during *validation* time.
fn cast_decimal_val<N: NumericValue>(value: &N, ctx: &mut dyn VisitorContext) -> Option<Decimal> {
    try_cast_decimal(value).or_else(|| {
        ctx.issue(Issue::NumericNotRepresentableAsDecimal);
        None
    })
}

// ============================================================================
// Comparison validators
// ============================================================================

macro_rules! cmp_validator {
    ($name:ident, $op:tt, $issue_op:expr) => {
        #[validator]
        pub struct $name {
            target: Decimal,
        }

        impl $name {
            pub fn new<N: NumericValue>(target: N) -> Self {
                let target = cast_decimal_cfg(&target);

                Self { target }
            }
        }

        impl<N: NumericValue> Validator<N> for $name {
            fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
                let Some(v) = cast_decimal_val(value, ctx) else { return };

                if !(v $op self.target) {
                    ctx.issue(Issue::NumericComparison {
                        actual: v,
                        op: $issue_op,
                        expected: self.target,
                    });
                }
            }
        }
    };
}

cmp_validator!(Lt, <, IssueComparisonOp::Lt);
cmp_validator!(Gt, >, IssueComparisonOp::Gt);
cmp_validator!(Lte, <=, IssueComparisonOp::Lte);
cmp_validator!(Gte, >=, IssueComparisonOp::Gte);
cmp_validator!(Equal, ==, IssueComparisonOp::Eq);
cmp_validator!(NotEqual, !=, IssueComparisonOp::Ne);

///
/// Range
///

#[validator]
pub struct Range {
    min: Decimal,
    max: Decimal,
}

impl Range {
    pub fn new<N: NumericValue>(min: N, max: N) -> Self {
        let min = cast_decimal_cfg(&min);
        let max = cast_decimal_cfg(&max);

        Self { min, max }
    }
}

impl<N: NumericValue> Validator<N> for Range {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
        let Some(v) = cast_decimal_val(value, ctx) else {
            return;
        };

        if v < self.min || v > self.max {
            ctx.issue(Issue::NumericRange {
                actual: v,
                min: self.min,
                max: self.max,
            });
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
    pub fn new<N: NumericValue>(target: N) -> Self {
        let target = cast_decimal_cfg(&target);

        Self { target }
    }
}

impl<N: NumericValue> Validator<N> for MultipleOf {
    fn validate(&self, value: &N, ctx: &mut dyn VisitorContext) {
        if self.target.is_zero() {
            ctx.issue(Issue::NumericMultipleOfZero);
            return;
        }

        let Some(v) = cast_decimal_val(value, ctx) else {
            return;
        };

        if !(v % self.target).is_zero() {
            ctx.issue(Issue::NumericMultipleOf {
                actual: v,
                target: self.target,
            });
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
            self.issues.push(String::new(), issue);
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
