use crate::{
    traits::Visitable,
    visitor::{PathSegment, Visitor, VisitorAdapter, VisitorContext, VisitorIssues, perform_visit},
};

///
/// validate
/// Validate a visitable tree, collecting issues by path.
///
/// Validation is non-failing at the traversal level. All validation
/// issues are collected and returned to the caller, which may choose
/// how to interpret them.
///
pub(crate) fn validate(node: &dyn Visitable) -> Result<(), VisitorIssues> {
    let visitor = ValidateVisitor::new();
    let mut adapter = VisitorAdapter::new(visitor);

    perform_visit(&mut adapter, node, PathSegment::Empty);

    adapter.result()
}

///
/// ValidateVisitor
/// Walks a tree and applies validation at each node.
///
#[derive(Debug, Default)]
pub struct ValidateVisitor;

impl ValidateVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Visitor for ValidateVisitor {
    fn enter(&mut self, node: &dyn Visitable, ctx: &mut dyn VisitorContext) {
        node.validate_self(ctx);
        node.validate_custom(ctx);
    }

    fn exit(&mut self, _: &dyn Visitable, _: &mut dyn VisitorContext) {}
}
