//! Module: visitor::normalize
//! Responsibility: normalize visitor implementation over visitable trees.
//! Does not own: top-level normalize entrypoints or issue aggregation policy.
//! Boundary: mutating visitor used by the crate-level normalize surface.

use crate::visitor::{
    CallbackContext, CallbackIdentity, CallbackKind, Visitable, VisitorContext, VisitorMut,
};

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
        let type_identity = node.type_identity();
        let mut auto_context = CallbackContext::new(
            ctx,
            CallbackIdentity::new(CallbackKind::NormalizeAuto, type_identity),
        );
        node.normalize_self(&mut auto_context);

        let mut custom_context = CallbackContext::new(
            ctx,
            CallbackIdentity::new(CallbackKind::NormalizeCustom, type_identity),
        );
        node.normalize_custom(&mut custom_context);
    }

    fn exit_mut(&mut self, _: &mut dyn Visitable, _: &mut dyn VisitorContext) {}
}
