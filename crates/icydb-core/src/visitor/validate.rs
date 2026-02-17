use crate::{
    traits::Visitable,
    visitor::{Visitor, VisitorContext},
};

///
/// ValidateVisitor
/// Walks a tree and applies validation at each node.
///

#[derive(Debug, Default)]
pub(crate) struct ValidateVisitor;

impl ValidateVisitor {
    #[must_use]
    pub(crate) const fn new() -> Self {
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
