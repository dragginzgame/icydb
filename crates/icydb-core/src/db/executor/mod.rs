mod commit_ops;
mod context;
mod delete;
mod load;
mod plan;
mod save;
#[cfg(test)]
mod tests;
pub mod trace;

pub use context::*;
pub use delete::DeleteExecutor;
pub use load::LoadExecutor;
pub use save::SaveExecutor;

// Design notes:
// - SchemaInfo is the planner-visible schema (relational attributes). Executors may see
//   additional tuple payload not represented in SchemaInfo.
// - Unsupported or opaque values are treated as incomparable; executor validation may
//   skip type checks for these values.
// - ORDER BY is stable; incomparable values preserve input order.
// - Corruption indicates invalid persisted bytes or store mismatches; invariant violations
//   indicate executor/planner contract breaches.

use crate::{
    db::store::DataKey,
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use thiserror::Error as ThisError;

///
/// ExecutorError
///

#[derive(Debug, ThisError)]
pub enum ExecutorError {
    #[error("corruption detected ({origin}): {message}")]
    Corruption {
        origin: ErrorOrigin,
        message: String,
    },

    #[error("index constraint violation: {0} ({1})")]
    IndexViolation(String, String),

    #[error("index not found: {0} ({1})")]
    IndexNotFound(String, String),

    #[error("index not unique: {0} ({1})")]
    IndexNotUnique(String, String),

    #[error("index key missing: {0} ({1})")]
    IndexKeyMissing(String, String),

    #[error("data key exists: {0}")]
    KeyExists(DataKey),

    #[error("primary key type mismatch: expected {0}, got {1}")]
    KeyTypeMismatch(String, String),

    #[error("primary key out of range for {0}: {1}")]
    KeyOutOfRange(String, String),
}

impl ExecutorError {
    #[must_use]
    /// Build an index-violation error with a formatted path/field list.
    pub(crate) fn index_violation(path: &str, index_fields: &[&str]) -> Self {
        Self::IndexViolation(path.to_string(), index_fields.join(", "))
    }

    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists(_) | Self::IndexViolation(_, _) => ErrorClass::Conflict,
            Self::IndexNotFound(_, _)
            | Self::IndexNotUnique(_, _)
            | Self::IndexKeyMissing(_, _)
            | Self::KeyTypeMismatch(_, _)
            | Self::KeyOutOfRange(_, _) => ErrorClass::Unsupported,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(crate) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists(_) => ErrorOrigin::Store,
            Self::IndexViolation(_, _)
            | Self::IndexNotFound(_, _)
            | Self::IndexNotUnique(_, _)
            | Self::IndexKeyMissing(_, _) => ErrorOrigin::Index,
            Self::Corruption { origin, .. } => *origin,
            Self::KeyTypeMismatch(_, _) | Self::KeyOutOfRange(_, _) => ErrorOrigin::Executor,
        }
    }

    pub(crate) fn corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self::Corruption {
            origin,
            message: message.into(),
        }
    }
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::new(err.class(), err.origin(), err.to_string())
    }
}
