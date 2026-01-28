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
pub mod session;

pub use builder::*;
pub use diagnostics::{
    QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
    QueryTraceExecutorKind, QueryTracePhase,
};
pub use intent::{DeleteSpec, IntentError, LoadSpec, Query, QueryError, QueryMode};
pub use save::*;
pub use session::{SessionDeleteQuery, SessionLoadQuery};

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

/// Build an insert `SaveQuery`.
#[must_use]
pub fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

/// Build an update `SaveQuery`.
#[must_use]
pub fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

/// Build a replace `SaveQuery`.
#[must_use]
pub fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
