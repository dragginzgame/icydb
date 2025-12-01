pub mod sanitize;
pub mod validate;

pub use sanitize::*;
pub use validate::*;

use crate::{ThisError, traits::Visitable, visitor::SanitizeVisitor};

///
/// VisitorError
///

#[derive(Debug, ThisError)]
pub enum VisitorError {
    #[error(transparent)]
    ValidateError(#[from] validate::ValidateError),
}

///
/// MAIN FUNCTIONS
///

// sanitize
pub fn sanitize(node: &mut dyn Visitable) {
    let mut visitor = SanitizeVisitor::new();
    perform_visit_mut(&mut visitor, node, PathSegment::Empty);
}

///
/// Event
///

#[derive(Clone, Copy, Debug)]
pub enum Event {
    Enter,
    Exit,
}

///
/// PathSegment
///

#[derive(Debug)]
pub enum PathSegment {
    Empty,
    Field(&'static str),
    Index(usize),
}

impl From<&'static str> for PathSegment {
    fn from(s: &'static str) -> Self {
        Self::Field(s)
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Index(i)
    }
}

impl From<Option<&'static str>> for PathSegment {
    fn from(opt: Option<&'static str>) -> Self {
        match opt {
            Some(s) if !s.is_empty() => Self::Field(s),
            _ => Self::Empty,
        }
    }
}

///
/// Visitor
/// plus helper functions that allow navigation of the tree in an object-safe way
///

pub trait Visitor {
    // nodes
    fn visit(&mut self, node: &dyn Visitable, event: Event);

    // path
    fn push(&mut self, _: PathSegment) {}
    fn pop(&mut self) {}
}

// perform_visit
#[inline]
pub fn perform_visit<S: Into<PathSegment>>(
    visitor: &mut dyn Visitor,
    node: &dyn Visitable,
    seg: S,
) {
    let seg = seg.into();
    let should_push = !matches!(seg, PathSegment::Empty);
    if should_push {
        visitor.push(seg);
    }
    visitor.visit(node, Event::Enter);
    node.drive(visitor);
    visitor.visit(node, Event::Exit);
    if should_push {
        visitor.pop();
    }
}

#[inline]
pub fn perform_visit_mut<S: Into<PathSegment>>(
    visitor: &mut dyn VisitorMut,
    node: &mut dyn Visitable,
    seg: S,
) {
    let seg = seg.into();
    let should_push = !matches!(seg, PathSegment::Empty);
    if should_push {
        visitor.push(seg);
    }
    visitor.visit(node, Event::Enter);
    node.drive_mut(visitor);
    visitor.visit(node, Event::Exit);
    if should_push {
        visitor.pop();
    }
}

///
/// VisitorMut
/// (adapted for mutable sanitization)
///

pub trait VisitorMut {
    fn visit(&mut self, node: &mut dyn Visitable, event: Event);

    fn push(&mut self, _: PathSegment) {}
    fn pop(&mut self) {}
}
