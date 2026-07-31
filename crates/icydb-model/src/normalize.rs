//! Module: normalize
//!
//! Responsibility: top-level normalize entrypoint over visitable trees.
//! Does not own: visitor diagnostics or per-type normalize implementations.
//! Boundary: convenient crate-level normalize surface that delegates to visitor traversal.

use crate::visitor::{
    ApplicationOperation, PathSegment, Visitable, VisitorError, VisitorMutAdapter,
    normalize::NormalizeVisitor, perform_visit_mut,
};

///
/// normalize
///
/// Run the normalizer visitor over a mutable visitable tree.
///
/// Traversal visits the complete value tree and aggregates any reported issues
/// into the returned `VisitorError`.
///
/// # Errors
///
/// Returns `VisitorError` when one or more normalizers report an issue.
///
pub fn normalize(node: &mut dyn Visitable) -> Result<(), VisitorError> {
    let visitor = NormalizeVisitor::new();
    let mut adapter = VisitorMutAdapter::new(visitor);

    perform_visit_mut(&mut adapter, node, PathSegment::Empty);

    adapter
        .result()
        .map_err(|issues| VisitorError::new(ApplicationOperation::Normalize, issues))
}
