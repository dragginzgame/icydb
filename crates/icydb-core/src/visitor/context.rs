//! Module: visitor::context
//! Responsibility: visitor issue-reporting context and path scoping helpers.
//! Does not own: concrete sanitize/validate traversal behavior.
//! Boundary: shared diagnostics context passed through visitor entrypoints.

use crate::sanitize::SanitizeWriteContext;
use serde::Deserialize;
use std::fmt;

///
/// VisitorContext
///
/// Narrow interface exposed to visitors for reporting non-fatal issues.
/// Implemented by adapters via a short-lived context object.
///

pub trait VisitorContext {
    fn add_issue(&mut self, issue: Issue);
    fn add_issue_at(&mut self, seg: PathSegment, issue: Issue);

    fn sanitize_write_context(&self) -> Option<SanitizeWriteContext> {
        None
    }
}

impl dyn VisitorContext + '_ {
    pub fn issue(&mut self, issue: impl Into<Issue>) {
        self.add_issue(issue.into());
    }

    pub fn issue_at(&mut self, seg: PathSegment, issue: impl Into<Issue>) {
        self.add_issue_at(seg, issue.into());
    }
}

/// VisitorContext that pins all issues to a single path segment.
pub struct ScopedContext<'a> {
    ctx: &'a mut dyn VisitorContext,
    seg: PathSegment,
}

impl<'a> ScopedContext<'a> {
    #[must_use]
    pub fn new(ctx: &'a mut dyn VisitorContext, seg: PathSegment) -> Self {
        Self { ctx, seg }
    }
}

impl VisitorContext for ScopedContext<'_> {
    fn add_issue(&mut self, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }

    fn add_issue_at(&mut self, _seg: PathSegment, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }

    fn sanitize_write_context(&self) -> Option<SanitizeWriteContext> {
        self.ctx.sanitize_write_context()
    }
}

///
/// Issue
///

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct Issue {
    message: String,
}

impl Issue {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }
}

impl From<String> for Issue {
    fn from(message: String) -> Self {
        Self { message }
    }
}

impl From<&str> for Issue {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

///
/// PathSegment
///

#[derive(Clone, Debug)]
pub enum PathSegment {
    Empty,
    Field(&'static str),
    Index(usize),
}

impl From<&'static str> for PathSegment {
    fn from(s: &'static str) -> Self {
        Self::Field(s)
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Index(i)
    }
}

impl From<Option<&'static str>> for PathSegment {
    fn from(opt: Option<&'static str>) -> Self {
        match opt {
            Some(s) if !s.is_empty() => Self::Field(s),
            _ => Self::Empty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Issue;

    #[test]
    fn custom_issue_preserves_message() {
        let issue = Issue::from("pet name is reserved");

        assert_eq!(issue.message(), "pet name is reserved");
        assert_eq!(issue.to_string(), "pet name is reserved");
    }
}
