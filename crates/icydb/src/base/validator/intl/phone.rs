//! Module: base::validator::intl::phone
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{design::prelude::*, visitor::Validator};

///
/// E164PhoneNumber
/// Ensures phone number has the canonical E.164 envelope:
/// `+` followed by 7 to 15 ASCII digits, with a non-zero first digit.
///

#[validator]
pub struct E164PhoneNumber;

impl Validator<str> for E164PhoneNumber {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        let Some(digits) = s.strip_prefix('+') else {
            ctx.issue("phone number must start with +");
            return;
        };

        if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
            ctx.issue("phone number must contain only digits after +");
            return;
        }

        if digits.starts_with('0') {
            ctx.issue("phone number country code must not start with 0");
            return;
        }

        let digit_count = digits.len();

        if !(7..=15).contains(&digit_count) {
            ctx.issue(format!(
                "phone number has {digit_count} digits; expected 7 to 15"
            ));
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

        fn has_issues(&self) -> bool {
            !self.issues.is_empty()
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
    fn e164_phone_validator_accepts_canonical_phone_number() {
        let validator = E164PhoneNumber;
        let mut ctx = TestCtx::new();

        validator.validate("+15551234567", &mut ctx);

        assert!(!ctx.has_issues());
    }

    #[test]
    fn e164_phone_validator_rejects_interleaved_non_digits() {
        let validator = E164PhoneNumber;
        let mut ctx = TestCtx::new();

        validator.validate("+1 (555) 123-4567", &mut ctx);

        assert!(ctx.has_issues());
    }

    #[test]
    fn e164_phone_validator_rejects_zero_country_code_and_bad_lengths() {
        for value in ["+05551234567", "+123456", "+1234567890123456"] {
            let validator = E164PhoneNumber;
            let mut ctx = TestCtx::new();

            validator.validate(value, &mut ctx);

            assert!(ctx.has_issues(), "{value} should be rejected");
        }
    }
}
