mod coerce;
mod context;
mod delete;
mod filter;
mod load;
mod save;

pub(crate) use context::*;
pub use delete::DeleteExecutor;
use filter::*;
pub use load::LoadExecutor;
pub use save::SaveExecutor;

use crate::{
    Error,
    db::{
        DbError,
        primitives::FilterExpr,
        query::{QueryPlan, QueryPlanner},
        store::DataKey,
    },
    obs::metrics::Span,
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// ExecutorError
///

#[derive(Debug, ThisError)]
pub enum ExecutorError {
    #[error("data key exists: {0}")]
    KeyExists(DataKey),

    #[error("data key not found: {0}")]
    KeyNotFound(DataKey),

    #[error("index constraint violation: {0} ({1})")]
    IndexViolation(String, String),
}

impl ExecutorError {
    #[must_use]
    /// Build an index-violation error with a formatted path/field list.
    pub(crate) fn index_violation(path: &str, index_fields: &[&str]) -> Self {
        Self::IndexViolation(path.to_string(), index_fields.join(", "))
    }
}

impl From<ExecutorError> for Error {
    fn from(err: ExecutorError) -> Self {
        DbError::from(err).into()
    }
}

/// Plan a query for an entity given an optional filter.
#[must_use]
pub(crate) fn plan_for<E: EntityKind>(filter: Option<&FilterExpr>) -> QueryPlan {
    QueryPlanner::new(filter).plan::<E>()
}

/// Convenience: set span rows from a usize length.
pub(crate) const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}
