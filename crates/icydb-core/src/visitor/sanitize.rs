use crate::{
    traits::Visitable,
    visitor::{
        PathSegment, VisitorContext, VisitorIssues, VisitorMut, VisitorMutAdapter,
        perform_visit_mut,
    },
};

///
/// sanitize
/// Run the sanitizer visitor over a mutable visitable tree.
///
/// Sanitization is total and non-failing. Any issues discovered during
/// sanitization are reported via the returned `VisitorIssues`.
///
pub(crate) fn sanitize(node: &mut dyn Visitable) -> Result<(), VisitorIssues> {
    let visitor = SanitizeVisitor::new();
    let mut adapter = VisitorMutAdapter::new(visitor);

    perform_visit_mut(&mut adapter, node, PathSegment::Empty);

    adapter.result()
}

///
/// SanitizeVisitor
/// Walks a tree and applies sanitization at each node.
///

#[derive(Debug, Default)]
pub struct SanitizeVisitor;

impl SanitizeVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut for SanitizeVisitor {
    fn enter_mut(&mut self, node: &mut dyn Visitable, ctx: &mut dyn VisitorContext) {
        node.sanitize_self(ctx);
        node.sanitize_custom(ctx);
    }

    fn exit_mut(&mut self, _: &mut dyn Visitable, _: &mut dyn VisitorContext) {}
}
