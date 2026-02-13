//! Query Builder modules.
//!
//! Predicate semantics are defined in `docs/QUERY_PRACTICE.md` and are the
//! canonical contract for evaluation, coercion, and normalization.

pub mod builder;
pub mod diagnostics;
pub mod expr;
pub mod intent;
pub mod plan;
pub(crate) mod policy;
pub mod predicate;
mod save;
pub mod session;

pub use builder::*;
pub use diagnostics::{
    QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
    QueryTraceExecutorKind, QueryTracePhase,
};
pub use expr::{FilterExpr, SortExpr, SortLowerError};
pub use intent::{DeleteSpec, IntentError, LoadSpec, Query, QueryError, QueryMode};
pub(crate) use save::SaveMode;
pub use session::{
    delete::SessionDeleteQuery,
    load::{PagedLoadQuery, SessionLoadQuery},
};

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
