use crate::{
    traits::Visitable,
    visitor::{Event, VisitorMut},
};

///
/// SanitizeVisitor
/// Walks a tree and applies sanitize() on every node
///

#[derive(Debug, Default)]
pub struct SanitizeVisitor;

impl SanitizeVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

///
/// perform_visit_mut
/// like `perform_visit` but takes &mut for in-place mutation
///

impl VisitorMut for SanitizeVisitor {
    fn visit(&mut self, node: &mut dyn Visitable, event: Event) {
        if matches!(event, Event::Enter) {
            node.sanitize_self();
            node.sanitize_children();
            node.sanitize_custom();
        }
    }
}
