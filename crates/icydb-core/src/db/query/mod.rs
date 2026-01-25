//! Query Builder modules.
//!
//! Predicate semantics are defined in `docs/QUERY_BUILDER.md` and are the
//! canonical contract for evaluation, coercion, and normalization.

pub mod builder;
pub mod diagnostics;
pub mod intent;
pub mod plan;
pub mod predicate;
mod save;

pub use builder::*;
pub use diagnostics::{
    QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
    QueryTraceExecutorKind,
};
pub use intent::{DeleteLimit, IntentError, Page, Query, QueryError, QueryMode};
pub use save::*;

/// Missing-row handling policy for query execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Missing rows are ignored (no error).
    MissingOk,
    /// Missing rows are treated as corruption.
    Strict,
}

// create
#[must_use]
/// Build an insert `SaveQuery`.
pub fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

// update
#[must_use]
/// Build an update `SaveQuery`.
pub fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

// replace
#[must_use]
/// Build a replace `SaveQuery`.
pub fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
