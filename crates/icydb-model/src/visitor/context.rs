//! Module: visitor::context
//! Responsibility: visitor issue-reporting context and path scoping helpers.
//! Does not own: concrete normalize/validate traversal behavior.
//! Boundary: shared diagnostics context passed through visitor entrypoints.

use std::fmt;

/// Application operation that produced visitor diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationOperation {
    /// Explicit mutable normalization traversal.
    Normalize,
    /// Explicit read-only validation traversal.
    Validate,
}

impl fmt::Display for ApplicationOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normalize => f.write_str("normalization"),
            Self::Validate => f.write_str("validation"),
        }
    }
}

/// Exact generated or application callback class that reported an issue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackKind {
    /// Generated node-local normalizer traversal.
    NormalizeAuto,
    /// Application-authored node-local normalization hook.
    NormalizeCustom,
    /// One declared normalizer attachment.
    Normalizer,
    /// Generated node-local validator traversal.
    ValidateAuto,
    /// Application-authored node-local validation hook.
    ValidateCustom,
    /// One declared validator attachment.
    Validator,
}

/// Typed identity of the callback that reported an application issue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallbackIdentity {
    kind: CallbackKind,
    type_path: &'static str,
}

impl CallbackIdentity {
    /// Construct an identity from its callback class and concrete Rust type.
    #[must_use]
    pub const fn new(kind: CallbackKind, type_path: &'static str) -> Self {
        Self { kind, type_path }
    }

    /// Return the callback class.
    #[must_use]
    pub const fn kind(&self) -> CallbackKind {
        self.kind
    }

    /// Return the concrete Rust callback or application-value type path.
    #[must_use]
    pub const fn type_path(&self) -> &str {
        self.type_path
    }
}

impl fmt::Display for CallbackIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {}", self.kind, self.type_path)
    }
}

///
/// VisitorContext
///
/// Narrow interface exposed to visitors for reporting non-fatal issues.
/// Implemented by adapters via a short-lived context object.
///

pub trait VisitorContext {
    fn add_issue(&mut self, issue: Issue);
    fn add_issue_at(&mut self, seg: PathSegment, issue: Issue);
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

/// Visitor context that binds unowned issues to one typed callback identity.
///
/// Nested callback contexts preserve the innermost identity, allowing a
/// declared normalizer or validator to override its generated traversal hook.
pub struct CallbackContext<'a> {
    ctx: &'a mut dyn VisitorContext,
    callback: CallbackIdentity,
}

impl<'a> CallbackContext<'a> {
    /// Bind subsequent issues to `callback` unless already more specifically
    /// identified by a nested callback context.
    #[must_use]
    pub fn new(ctx: &'a mut dyn VisitorContext, callback: CallbackIdentity) -> Self {
        Self { ctx, callback }
    }
}

impl VisitorContext for CallbackContext<'_> {
    fn add_issue(&mut self, mut issue: Issue) {
        issue.bind_callback_if_unset(&self.callback);
        self.ctx.add_issue(issue);
    }

    fn add_issue_at(&mut self, seg: PathSegment, mut issue: Issue) {
        issue.bind_callback_if_unset(&self.callback);
        self.ctx.add_issue_at(seg, issue);
    }
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
}

///
/// Issue
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Issue {
    callback: Option<CallbackIdentity>,
    message: String,
}

impl Issue {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            callback: None,
            message: message.into(),
        }
    }

    /// Return the typed callback identity attached during traversal.
    #[must_use]
    pub const fn callback(&self) -> Option<&CallbackIdentity> {
        self.callback.as_ref()
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }

    const fn bind_callback_if_unset(&mut self, callback: &CallbackIdentity) {
        if self.callback.is_none() {
            self.callback = Some(*callback);
        }
    }
}

impl From<String> for Issue {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for Issue {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(callback) = &self.callback {
            write!(f, "{callback}: {}", self.message)
        } else {
            f.write_str(&self.message)
        }
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
    use super::{CallbackIdentity, CallbackKind, Issue};

    #[test]
    fn custom_issue_preserves_message() {
        let issue = Issue::from("pet name is reserved");

        assert_eq!(issue.message(), "pet name is reserved");
        assert_eq!(issue.callback(), None);
        assert_eq!(issue.to_string(), "pet name is reserved");
    }

    #[test]
    fn typed_issue_displays_callback_identity_without_changing_its_message() {
        let mut issue = Issue::from("pet name is reserved");
        issue.bind_callback_if_unset(&CallbackIdentity::new(
            CallbackKind::Validator,
            "schema::PetName",
        ));

        let callback = issue.callback().expect("callback identity should bind");
        assert_eq!(callback.kind(), CallbackKind::Validator);
        assert_eq!(callback.type_path(), "schema::PetName");
        assert_eq!(issue.message(), "pet name is reserved");
        assert_eq!(
            issue.to_string(),
            "Validator schema::PetName: pet name is reserved"
        );
    }
}
