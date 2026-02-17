use crate::{
    traits::Visitable,
    visitor::{
        PathSegment, VisitorError, VisitorMutAdapter, perform_visit_mut, sanitize::SanitizeVisitor,
    },
};

///
/// sanitize
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
