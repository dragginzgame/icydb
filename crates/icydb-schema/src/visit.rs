use crate::{node::VisitableNode, prelude::*};

///
/// Event
/// fine to redeclare this every time we use the visitor pattern
/// makes no sense to rely on such a simple dependency
///

#[derive(Debug)]
pub enum Event {
    Enter,
    Exit,
}

///
/// Visitor
///

pub trait Visitor {
    // visit
    fn visit<V: VisitableNode + ?Sized>(&mut self, _: &V, _: Event) {}

    // key
    fn push(&mut self, _: &str) {}
    fn pop(&mut self) {}
}

///
/// ValidateVisitor
///

#[derive(Debug, Default)]
pub struct ValidateVisitor {
    pub errors: ErrorTree,
    pub path: Vec<String>,
    pub node_count: usize,
}

impl ValidateVisitor {
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
}

impl ValidateVisitor {
    fn current_route(&self) -> String {
        self.path
            .iter()
            .filter(|s| !s.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join(".")
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
                                self.errors.children.entry(route).or_default().merge(errs);
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
