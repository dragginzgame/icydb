//! Module: visitor::normalize
//! Responsibility: normalize visitor implementation over visitable trees.
//! Does not own: top-level normalize entrypoints or issue aggregation policy.
//! Boundary: mutating visitor used by the crate-level normalize surface.

use crate::visitor::{Visitable, VisitorContext, VisitorMut};

///
/// NormalizeVisitor
///
/// Walks a tree and applies normalization at each node.
///

#[derive(Debug, Default)]
pub(crate) struct NormalizeVisitor;

impl NormalizeVisitor {
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self
    }
}

impl VisitorMut for NormalizeVisitor {
    fn enter_mut(&mut self, node: &mut dyn Visitable, ctx: &mut dyn VisitorContext) {
        node.normalize_self(ctx);
        node.normalize_custom(ctx);
    }

    fn exit_mut(&mut self, _: &mut dyn Visitable, _: &mut dyn VisitorContext) {}
}
