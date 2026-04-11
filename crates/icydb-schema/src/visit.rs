use crate::{node::VisitableNode, prelude::*};

///
/// Event
///
/// Enter/exit marker passed to schema visitors during recursive traversal.
///

#[derive(Debug)]
pub enum Event {
    Enter,
    Exit,
}

///
/// Visitor
///
/// Minimal visitor trait over schema nodes.
/// Implementors may track path state, collect errors, or gather derived
/// metadata while nodes drive traversal.
///

pub trait Visitor {
    // Observe one node at one traversal phase.
    fn visit<V: VisitableNode + ?Sized>(&mut self, _: &V, _: Event) {}

    // Maintain the current visitor path as traversal enters and exits nodes.
    fn push(&mut self, _: &str) {}
    fn pop(&mut self) {}
}

///
/// ValidateVisitor
///
/// Visitor that runs `ValidateNode::validate()` across the schema tree and
/// merges errors under their computed traversal route.
///

#[derive(Debug, Default)]
pub struct ValidateVisitor {
    errors: ErrorTree,
    path: Vec<String>,
    node_count: usize,
}

impl ValidateVisitor {
    /// Build one empty validating visitor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            errors: ErrorTree::new(),
            ..Default::default()
        }
    }

    #[must_use]
    pub const fn node_count(&self) -> usize {
        self.node_count
    }

    #[must_use]
    pub const fn errors(&self) -> &ErrorTree {
        &self.errors
    }

    #[must_use]
    pub fn into_errors(self) -> ErrorTree {
        self.errors
    }
}

impl ValidateVisitor {
    // Build one dotted route string from the active visitor path stack.
    fn current_route(&self) -> String {
        let mut route = String::new();

        for segment in self.path.iter().filter(|segment| !segment.is_empty()) {
            if !route.is_empty() {
                route.push('.');
            }
            route.push_str(segment);
        }

        route
    }
}

impl Visitor for ValidateVisitor {
    fn visit<T: VisitableNode + ?Sized>(&mut self, node: &T, event: Event) {
        match event {
            Event::Enter => {
                self.node_count += 1;

                match node.validate() {
                    Ok(()) => {}
                    Err(errs) => {
                        if !errs.is_empty() {
                            let route = self.current_route();

                            if route.is_empty() {
                                // At the current level, merge directly.
                                self.errors.merge(errs);
                            } else {
                                // Add to a child entry under the computed route.
                                self.errors.merge_for(route, errs);
                            }
                        }
                    }
                }
            }
            Event::Exit => {}
        }
    }

    fn push(&mut self, s: &str) {
        self.path.push(s.to_string());
    }

    fn pop(&mut self) {
        self.path.pop();
    }
}
