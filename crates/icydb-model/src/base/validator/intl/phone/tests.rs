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
