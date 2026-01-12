use crate::{
    traits::Visitable,
    visitor::{VisitorContext, VisitorMut},
};

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
