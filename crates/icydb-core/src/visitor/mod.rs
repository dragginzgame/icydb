pub mod sanitize;
pub mod validate;

pub use sanitize::*;
pub use validate::*;

use crate::{ThisError, traits::Visitable};
use std::collections::BTreeMap;

// ============================================================================
// Public integration error
// ============================================================================

#[derive(Debug, ThisError)]
pub enum VisitorError {
    #[error(transparent)]
    ValidateError(#[from] validate::ValidateError),

    #[error(transparent)]
    SanitizeError(#[from] sanitize::SanitizeError),
}

// ============================================================================
// Path
// ============================================================================

#[derive(Clone, Debug)]
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

// ============================================================================
// VisitorContext
// ============================================================================

/// Narrow interface exposed to visitors for reporting non-fatal issues.
/// Implemented by adapters via a short-lived context object.
pub trait VisitorContext {
    fn add_issue(&mut self, message: String);
    fn add_issue_at(&mut self, seg: PathSegment, message: String);
}

// ============================================================================
// Visitor (immutable)
// ============================================================================

pub trait Visitor<E> {
    fn enter(&mut self, node: &dyn Visitable, ctx: &mut dyn VisitorContext) -> Result<(), E>;

    fn exit(&mut self, node: &dyn Visitable) -> Result<(), E>;
}

// ============================================================================
// VisitorCore (object-safe traversal)
// ============================================================================

pub trait VisitorCore {
    fn enter(&mut self, node: &dyn Visitable);
    fn exit(&mut self, node: &dyn Visitable);

    fn push(&mut self, _: PathSegment) {}
    fn pop(&mut self) {}
}

// ============================================================================
// Internal adapter context (fixes borrow checker)
// ============================================================================

struct AdapterContext<'a> {
    path: &'a [PathSegment],
    issues: &'a mut BTreeMap<String, Vec<String>>,
}

impl VisitorContext for AdapterContext<'_> {
    fn add_issue(&mut self, message: String) {
        let key = render_path(self.path, None);
        self.issues.entry(key).or_default().push(message);
    }

    fn add_issue_at(&mut self, seg: PathSegment, message: String) {
        let key = render_path(self.path, Some(seg));
        self.issues.entry(key).or_default().push(message);
    }
}

fn render_path(path: &[PathSegment], extra: Option<PathSegment>) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let mut first = true;

    let iter = path.iter().cloned().chain(extra);

    for seg in iter {
        match seg {
            PathSegment::Field(s) => {
                if !first {
                    out.push('.');
                }
                out.push_str(s);
            }
            PathSegment::Index(i) => {
                let _ = write!(out, "[{i}]");
            }
            PathSegment::Empty => {}
        }
        first = false;
    }

    out
}

// ============================================================================
// VisitorAdapter (immutable)
// ============================================================================

pub struct VisitorAdapter<V, E> {
    visitor: V,
    fatal: Option<E>,
    path: Vec<PathSegment>,
    issues: BTreeMap<String, Vec<String>>,
}

impl<V, E> VisitorAdapter<V, E>
where
    V: Visitor<E>,
{
    pub const fn new(visitor: V) -> Self {
        Self {
            visitor,
            fatal: None,
            path: Vec::new(),
            issues: BTreeMap::new(),
        }
    }

    pub fn finish(self) -> Result<(), E> {
        match self.fatal {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub const fn issues(&self) -> &BTreeMap<String, Vec<String>> {
        &self.issues
    }
}

impl<V, E> VisitorCore for VisitorAdapter<V, E>
where
    V: Visitor<E>,
{
    fn push(&mut self, seg: PathSegment) {
        if !matches!(seg, PathSegment::Empty) {
            self.path.push(seg);
        }
    }

    fn pop(&mut self) {
        self.path.pop();
    }

    fn enter(&mut self, node: &dyn Visitable) {
        if self.fatal.is_none() {
            let mut ctx = AdapterContext {
                path: &self.path,
                issues: &mut self.issues,
            };

            if let Err(e) = self.visitor.enter(node, &mut ctx) {
                self.fatal = Some(e);
            }
        }
    }

    fn exit(&mut self, node: &dyn Visitable) {
        if self.fatal.is_none()
            && let Err(e) = self.visitor.exit(node)
        {
            self.fatal = Some(e);
        }
    }
}

// ============================================================================
// Traversal (immutable)
// ============================================================================

pub fn perform_visit<S: Into<PathSegment>>(
    visitor: &mut dyn VisitorCore,
    node: &dyn Visitable,
    seg: S,
) {
    let seg = seg.into();
    let should_push = !matches!(seg, PathSegment::Empty);

    if should_push {
        visitor.push(seg);
    }

    visitor.enter(node);
    node.drive(visitor);
    visitor.exit(node);

    if should_push {
        visitor.pop();
    }
}

// ============================================================================
// VisitorMut (mutable)
// ============================================================================

pub trait VisitorMut<E> {
    fn enter_mut(
        &mut self,
        node: &mut dyn Visitable,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), E>;

    fn exit_mut(&mut self, node: &mut dyn Visitable) -> Result<(), E>;
}

// ============================================================================
// VisitorMutCore
// ============================================================================

pub trait VisitorMutCore {
    fn enter_mut(&mut self, node: &mut dyn Visitable);
    fn exit_mut(&mut self, node: &mut dyn Visitable);

    fn push(&mut self, _: PathSegment) {}
    fn pop(&mut self) {}
}

// ============================================================================
// VisitorMutAdapter
// ============================================================================

pub struct VisitorMutAdapter<V, E> {
    visitor: V,
    fatal: Option<E>,
    path: Vec<PathSegment>,
    issues: BTreeMap<String, Vec<String>>,
}

impl<V, E> VisitorMutAdapter<V, E>
where
    V: VisitorMut<E>,
{
    pub const fn new(visitor: V) -> Self {
        Self {
            visitor,
            fatal: None,
            path: Vec::new(),
            issues: BTreeMap::new(),
        }
    }

    pub fn finish(self) -> Result<(), E> {
        match self.fatal {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub const fn issues(&self) -> &BTreeMap<String, Vec<String>> {
        &self.issues
    }
}

impl<V, E> VisitorMutCore for VisitorMutAdapter<V, E>
where
    V: VisitorMut<E>,
{
    fn push(&mut self, seg: PathSegment) {
        if !matches!(seg, PathSegment::Empty) {
            self.path.push(seg);
        }
    }

    fn pop(&mut self) {
        self.path.pop();
    }

    fn enter_mut(&mut self, node: &mut dyn Visitable) {
        if self.fatal.is_none() {
            let mut ctx = AdapterContext {
                path: &self.path,
                issues: &mut self.issues,
            };

            if let Err(e) = self.visitor.enter_mut(node, &mut ctx) {
                self.fatal = Some(e);
            }
        }
    }

    fn exit_mut(&mut self, node: &mut dyn Visitable) {
        if self.fatal.is_none()
            && let Err(e) = self.visitor.exit_mut(node)
        {
            self.fatal = Some(e);
        }
    }
}

// ============================================================================
// Traversal (mutable)
// ============================================================================

pub fn perform_visit_mut<S: Into<PathSegment>>(
    visitor: &mut dyn VisitorMutCore,
    node: &mut dyn Visitable,
    seg: S,
) {
    let seg = seg.into();
    let should_push = !matches!(seg, PathSegment::Empty);

    if should_push {
        visitor.push(seg);
    }

    visitor.enter_mut(node);
    node.drive_mut(visitor);
    visitor.exit_mut(node);

    if should_push {
        visitor.pop();
    }
}
