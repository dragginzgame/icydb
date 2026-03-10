//! Module: visitor::validate
//! Responsibility: module-local ownership and contracts for visitor::validate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
