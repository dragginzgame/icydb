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
fn mime_type_rejects_dot_only_tokens() {
    let validator = MimeType;
    let mut ctx = TestCtx::new();

    validator.validate("./.", &mut ctx);

    assert!(ctx.has_issues());
}

#[test]
fn mime_type_accepts_common_structured_suffix() {
    let validator = MimeType;
    let mut ctx = TestCtx::new();

    validator.validate("application/vnd.api+json", &mut ctx);

    assert!(!ctx.has_issues());
}

#[test]
fn url_rejects_unsupported_or_malformed_scheme_inputs() {
    for url in [
        "javascript:alert(1)",
        "https://javascript:alert(1)",
        "ftp://example.com",
        "https://",
        "https://example.com:abc",
        "https://exa mple.com",
    ] {
        let validator = Url;
        let mut ctx = TestCtx::new();

        validator.validate(url, &mut ctx);

        assert!(ctx.has_issues(), "{url} should be rejected");
    }
}

#[test]
fn url_accepts_http_hosts_and_numeric_ports() {
    for url in [
        "https://example.com",
        "http://localhost:8080/path?q=1",
        "https://127.0.0.1:4943/",
        "https://[::1]:4943/",
    ] {
        let validator = Url;
        let mut ctx = TestCtx::new();

        validator.validate(url, &mut ctx);

        assert!(!ctx.has_issues(), "{url} should be accepted");
    }
}
