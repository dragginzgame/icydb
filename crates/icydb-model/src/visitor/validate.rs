//! Module: visitor::validate
//! Responsibility: validation visitor implementation over visitable trees.
//! Does not own: top-level validation entrypoints or issue aggregation policy.
//! Boundary: read-only visitor used by the crate-level validate surface.

use crate::visitor::{
    CallbackContext, CallbackIdentity, CallbackKind, Visitable, Visitor, VisitorContext,
};

///
/// ValidateVisitor
///
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
        let type_identity = node.type_identity();
        let mut auto_context = CallbackContext::new(
            ctx,
            CallbackIdentity::new(CallbackKind::ValidateAuto, type_identity),
        );
        node.validate_self(&mut auto_context);

        let mut custom_context = CallbackContext::new(
            ctx,
            CallbackIdentity::new(CallbackKind::ValidateCustom, type_identity),
        );
        node.validate_custom(&mut custom_context);
    }

    fn exit(&mut self, _: &dyn Visitable, _: &mut dyn VisitorContext) {}
}
