pub(crate) mod context;
pub(crate) mod sanitize;
pub(crate) mod validate;

use crate::{error::InternalError, traits::Visitable};
use candid::CandidType;
use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};
use thiserror::Error as ThisError;

// re-exports
pub use context::{Issue, PathSegment, ScopedContext, VisitorContext};

///
/// VisitorError
/// Structured error type for visitor-based sanitization and validation.
///

#[derive(Debug, ThisError)]
#[error("{issues}")]
pub struct VisitorError {
    issues: VisitorIssues,
}

impl VisitorError {
    #[must_use]
    pub const fn issues(&self) -> &VisitorIssues {
        &self.issues
    }
}

impl From<VisitorIssues> for VisitorError {
    fn from(issues: VisitorIssues) -> Self {
        Self { issues }
    }
}

impl From<VisitorError> for VisitorIssues {
    fn from(err: VisitorError) -> Self {
        err.issues
    }
}

impl From<VisitorError> for InternalError {
    fn from(err: VisitorError) -> Self {
        Self::executor_unsupported(err.to_string())
    }
}

///
/// VisitorIssues
/// Aggregated visitor diagnostics.
///
/// NOTE: This is not an error type. It does not represent failure.
/// It is converted into a `VisitorError` at the runtime boundary and
/// may be lifted into an `InternalError` as needed.
///

#[derive(
    Clone, Debug, Default, Deserialize, Deref, DerefMut, Serialize, CandidType, Eq, PartialEq,
)]
pub struct VisitorIssues(BTreeMap<String, Vec<String>>);

impl VisitorIssues {
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }
}

impl From<BTreeMap<String, Vec<String>>> for VisitorIssues {
    fn from(map: BTreeMap<String, Vec<String>>) -> Self {
        Self(map)
    }
}

impl fmt::Display for VisitorIssues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote = false;

        for (path, messages) in &self.0 {
            for message in messages {
                if wrote {
                    writeln!(f)?;
                }

                if path.is_empty() {
                    write!(f, "{message}")?;
                } else {
                    write!(f, "{path}: {message}")?;
                }

                wrote = true;
            }
        }

        if !wrote {
            write!(f, "no visitor issues")?;
        }

        Ok(())
    }
}

impl std::error::Error for VisitorIssues {}

///
/// Visitor
/// (immutable)
///

pub(crate) trait Visitor {
    fn enter(&mut self, node: &dyn Visitable, ctx: &mut dyn VisitorContext);
    fn exit(&mut self, node: &dyn Visitable, ctx: &mut dyn VisitorContext);
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
    issues: &'a mut VisitorIssues,
}

impl VisitorContext for AdapterContext<'_> {
    fn add_issue(&mut self, issue: Issue) {
        let key = render_path(self.path, None);
        self.issues.entry(key).or_default().push(issue.message);
    }

    fn add_issue_at(&mut self, seg: PathSegment, issue: Issue) {
        let key = render_path(self.path, Some(seg));
        self.issues.entry(key).or_default().push(issue.message);
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
                first = false;
            }
            PathSegment::Index(i) => {
                let _ = write!(out, "[{i}]");
                first = false;
            }
            PathSegment::Empty => {}
        }
    }

    out
}

// ============================================================================
// VisitorAdapter (immutable)
// ============================================================================

pub(crate) struct VisitorAdapter<V> {
    visitor: V,
    path: Vec<PathSegment>,
    issues: VisitorIssues,
}

impl<V> VisitorAdapter<V>
where
    V: Visitor,
{
    pub(crate) const fn new(visitor: V) -> Self {
        Self {
            visitor,
            path: Vec::new(),
            issues: VisitorIssues::new(),
        }
    }

    pub(crate) fn result(self) -> Result<(), VisitorIssues> {
        if self.issues.is_empty() {
            Ok(())
        } else {
            Err(self.issues)
        }
    }
}

impl<V> VisitorCore for VisitorAdapter<V>
where
    V: Visitor,
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
        let mut ctx = AdapterContext {
            path: &self.path,
            issues: &mut self.issues,
        };
        self.visitor.enter(node, &mut ctx);
    }

    fn exit(&mut self, node: &dyn Visitable) {
        let mut ctx = AdapterContext {
            path: &self.path,
            issues: &mut self.issues,
        };
        self.visitor.exit(node, &mut ctx);
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

pub(crate) trait VisitorMut {
    fn enter_mut(&mut self, node: &mut dyn Visitable, ctx: &mut dyn VisitorContext);
    fn exit_mut(&mut self, node: &mut dyn Visitable, ctx: &mut dyn VisitorContext);
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

pub(crate) struct VisitorMutAdapter<V> {
    visitor: V,
    path: Vec<PathSegment>,
    issues: VisitorIssues,
}

impl<V> VisitorMutAdapter<V>
where
    V: VisitorMut,
{
    pub(crate) const fn new(visitor: V) -> Self {
        Self {
            visitor,
            path: Vec::new(),
            issues: VisitorIssues::new(),
        }
    }

    pub(crate) fn result(self) -> Result<(), VisitorIssues> {
        if self.issues.is_empty() {
            Ok(())
        } else {
            Err(self.issues)
        }
    }
}

impl<V> VisitorMutCore for VisitorMutAdapter<V>
where
    V: VisitorMut,
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
        let mut ctx = AdapterContext {
            path: &self.path,
            issues: &mut self.issues,
        };
        self.visitor.enter_mut(node, &mut ctx);
    }

    fn exit_mut(&mut self, node: &mut dyn Visitable) {
        let mut ctx = AdapterContext {
            path: &self.path,
            issues: &mut self.issues,
        };
        self.visitor.exit_mut(node, &mut ctx);
    }
}

// ============================================================================
// Traversal (mutable)
// ============================================================================

/// Perform a mutable visitor traversal starting at a trait-object node.
///
/// This is the *core* traversal entrypoint. It operates on `&mut dyn Visitable`
/// because visitor callbacks (`enter_mut` / `exit_mut`) require a trait object.
///
/// Path segments are pushed/popped around the traversal unless the segment is
/// `PathSegment::Empty`.
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
