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
fn equal_reports_mismatch() {
    let v = Equal::new(3);
    let mut ctx = TestCtx::new();

    v.validate("abcd", &mut ctx);

    assert_eq!(
        ctx.issues.get("").expect("root issue should exist")[0].message(),
        "length (4) is not equal to 3"
    );
}

#[test]
fn range_accepts_in_bounds() {
    let v = Range::new(2, 4);
    let mut ctx = TestCtx::new();

    v.validate("abc", &mut ctx);

    assert!(ctx.issues.is_empty());
}

#[test]
fn text_length_counts_unicode_scalar_values_not_utf8_bytes() {
    let one_scalar_four_bytes = "\u{1F525}";

    let equal_one = Equal::new(1);
    let min_two = Min::new(2);
    let max_one = Max::new(1);
    let mut equal_ctx = TestCtx::new();
    let mut min_ctx = TestCtx::new();
    let mut max_ctx = TestCtx::new();

    equal_one.validate(one_scalar_four_bytes, &mut equal_ctx);
    min_two.validate(one_scalar_four_bytes, &mut min_ctx);
    max_one.validate(one_scalar_four_bytes, &mut max_ctx);

    assert!(equal_ctx.issues.is_empty());
    assert_eq!(
        min_ctx.issues.get("").expect("root issue should exist")[0].message(),
        "length (1) is lower than minimum of 2"
    );
    assert!(max_ctx.issues.is_empty());
}

#[test]
fn range_reports_out_of_bounds() {
    let v = Range::new(2, 4);
    let mut ctx = TestCtx::new();

    v.validate("a", &mut ctx);

    assert_eq!(
        ctx.issues.get("").expect("root issue should exist")[0].message(),
        "length (1) must be between 2 and 4 (inclusive)"
    );
}
