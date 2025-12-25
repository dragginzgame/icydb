use crate::{
    Error, ThisError,
    traits::Visitable,
    visitor::{
        PathSegment, VisitorContext, VisitorError, VisitorMut, VisitorMutAdapter, perform_visit_mut,
    },
};

///
/// sanitize
/// Run the sanitizer visitor over a mutable visitable tree.
///
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), SanitizeError> {
    let visitor = SanitizeVisitor::new();
    let mut adapter = VisitorMutAdapter::new(visitor);

    perform_visit_mut(&mut adapter, node, PathSegment::Empty);

    // Fatal sanitization error only
    adapter.finish()
}

///
/// SanitizeIssue
/// Fatal sanitization failure (non-recoverable).
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SanitizeIssue {
    InvalidConfig(String),
}

impl SanitizeIssue {
    pub fn invalid_config(msg: impl Into<String>) -> Self {
        Self::InvalidConfig(msg.into())
    }
}

///
/// SanitizeError
/// Public-facing sanitization error
///

#[derive(Debug, ThisError)]
pub enum SanitizeError {
    #[error("invalid sanitizer configuration: {0}")]
    InvalidConfig(String),
}

impl From<SanitizeIssue> for SanitizeError {
    fn from(issue: SanitizeIssue) -> Self {
        match issue {
            SanitizeIssue::InvalidConfig(msg) => Self::InvalidConfig(msg),
        }
    }
}

impl From<SanitizeError> for Error {
    fn from(err: SanitizeError) -> Self {
        VisitorError::from(err).into()
    }
}

///
/// SanitizeVisitor
/// Walks a tree and applies sanitization at each node
///

#[derive(Debug, Default)]
pub struct SanitizeVisitor;

impl SanitizeVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut<SanitizeError> for SanitizeVisitor {
    fn enter_mut(
        &mut self,
        node: &mut dyn Visitable,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), SanitizeError> {
        node.sanitize_self(ctx)?;
        node.sanitize_custom(ctx)?;

        Ok(())
    }

    fn exit_mut(&mut self, _: &mut dyn Visitable) -> Result<(), SanitizeError> {
        Ok(())
    }
}
