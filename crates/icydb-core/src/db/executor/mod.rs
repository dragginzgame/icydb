mod context;
mod delete;
mod load;
mod mutation;
mod ordered_key_stream;
mod plan;
mod save;
#[cfg(test)]
mod tests;

pub(super) use context::*;
pub(super) use delete::DeleteExecutor;
pub(super) use load::LoadExecutor;
pub use load::{
    ExecutionAccessPathVariant, ExecutionFastPath, ExecutionPushdownType, ExecutionTrace,
};
pub(super) use ordered_key_stream::{
    MergeOrderedKeyStream, OrderedKeyStream, OrderedKeyStreamBox, VecOrderedKeyStream,
};
pub(super) use save::SaveExecutor;

// Design notes:
// - SchemaInfo is the planner-visible schema (relational attributes). Executors may see
//   additional tuple payload not represented in SchemaInfo.
// - Unsupported or opaque values are treated as incomparable; executor validation may
//   skip type checks for these values.
// - ORDER BY is stable; incomparable values preserve input order.
// - Corruption indicates invalid persisted bytes or store mismatches; invariant violations
//   indicate executor/planner contract breaches.

use crate::{
    db::data::DataKey,
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use thiserror::Error as ThisError;

///
/// ExecutorError
///

#[derive(Debug, ThisError)]
pub(crate) enum ExecutorError {
    #[error("corruption detected ({origin}): {message}")]
    Corruption {
        origin: ErrorOrigin,
        message: String,
    },

    #[error("data key exists: {0}")]
    KeyExists(DataKey),
}

impl ExecutorError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists(_) => ErrorClass::Conflict,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(crate) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists(_) => ErrorOrigin::Store,
            Self::Corruption { origin, .. } => *origin,
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
