//! Module: sanitize
//!
//! Responsibility: top-level sanitize entrypoint over visitable trees.
//! Does not own: visitor diagnostics or per-type sanitize implementations.
//! Boundary: convenient crate-level sanitize surface that delegates to visitor traversal.

use crate::{
    traits::Visitable,
    types::Timestamp,
    visitor::{
        PathSegment, VisitorError, VisitorMutAdapter, perform_visit_mut, sanitize::SanitizeVisitor,
    },
};

///
/// SanitizeWriteMode
///
/// Explicit write-mode contract exposed to sanitizer-driven write preflight.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SanitizeWriteMode {
    Insert,
    Replace,
    Update,
}

///
/// SanitizeWriteContext
///
/// Shared write preflight context passed through sanitizer traversal.
/// This keeps lifecycle-managed fields on one deterministic mutation contract.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SanitizeWriteContext {
    mode: SanitizeWriteMode,
    now: Timestamp,
}

impl SanitizeWriteContext {
    /// Build one explicit write preflight context.
    #[must_use]
    pub const fn new(mode: SanitizeWriteMode, now: Timestamp) -> Self {
        Self { mode, now }
    }

    /// Return the write-mode contract active for this preflight pass.
    #[must_use]
    pub const fn mode(self) -> SanitizeWriteMode {
        self.mode
    }

    /// Return the stable timestamp captured for this preflight pass.
    #[must_use]
    pub const fn now(self) -> Timestamp {
        self.now
    }
}

///
/// sanitize
///
/// Run the sanitizer visitor over a mutable visitable tree.
///
/// Sanitization is total and non-failing. Any issues discovered during
/// sanitization are reported via the returned `VisitorError`.
///
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), VisitorError> {
    sanitize_with_context(node, None)
}

///
/// sanitize_with_context
///
/// Run the sanitizer visitor over a mutable visitable tree with one explicit
/// optional write context.
///
/// Sanitization is total and non-failing. Any issues discovered during
/// sanitization are reported via the returned `VisitorError`.
///
pub fn sanitize_with_context(
    node: &mut dyn Visitable,
    write_context: Option<SanitizeWriteContext>,
) -> Result<(), VisitorError> {
    let visitor = SanitizeVisitor::new();
    let mut adapter = VisitorMutAdapter::with_sanitize_write_context(visitor, write_context);

    perform_visit_mut(&mut adapter, node, PathSegment::Empty);

    adapter.result().map_err(VisitorError::from)
}
