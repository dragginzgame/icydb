//! Module: sanitize
//!
//! Responsibility: top-level sanitize entrypoint over visitable trees.
//! Does not own: visitor diagnostics or per-type sanitize implementations.
//! Boundary: convenient crate-level sanitize surface that delegates to visitor traversal.

use crate::{
    traits::Visitable,
    visitor::{
        PathSegment, VisitorError, VisitorMutAdapter, perform_visit_mut, sanitize::SanitizeVisitor,
    },
};

///
/// sanitize
///
/// Run the sanitizer visitor over a mutable visitable tree.
///
/// Sanitization is total and non-failing. Any issues discovered during
/// sanitization are reported via the returned `VisitorError`.
///
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), VisitorError> {
    let visitor = SanitizeVisitor::new();
    let mut adapter = VisitorMutAdapter::new(visitor);

    perform_visit_mut(&mut adapter, node, PathSegment::Empty);

    adapter.result().map_err(VisitorError::from)
}
