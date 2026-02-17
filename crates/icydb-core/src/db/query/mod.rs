//! Query Builder modules.
//!
//! Predicate semantics are defined in `docs/QUERY_PRACTICE.md` and are the
//! canonical contract for evaluation, coercion, and normalization.

pub(crate) mod builder;
pub(crate) mod expr;
pub(crate) mod intent;
pub(crate) mod plan;
pub(crate) mod policy;
pub(crate) mod predicate;
pub(crate) mod save;
pub(crate) mod session;

///
/// ReadConsistency
/// Missing-row handling policy for query execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Missing rows are ignored (no error).
    MissingOk,

    /// Missing rows are treated as corruption.
    Strict,
}
