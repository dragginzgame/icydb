//! Module: executor::aggregate::contracts::error
//! Responsibility: aggregate runtime error taxonomy.
//! Does not own: planner-owned logical aggregate validation semantics.
//! Boundary: typed runtime failures shared by aggregate execution contracts.

use crate::error::InternalError;
use thiserror::Error as ThisError;

///
/// GroupError
///
/// GroupError is the typed grouped-execution error surface.
/// This taxonomy keeps grouped memory-limit failures explicit and prevents
/// grouped resource guardrails from degrading into generic internal errors.
///

#[derive(Debug, ThisError)]
pub(in crate::db::executor) enum GroupError {
    #[error(
        "grouped execution memory limit exceeded ({resource}): attempted={attempted}, limit={limit}"
    )]
    MemoryLimitExceeded {
        resource: &'static str,
        attempted: u64,
        limit: u64,
    },

    #[error("grouped DISTINCT budget exceeded ({resource}): attempted={attempted}, limit={limit}")]
    DistinctBudgetExceeded {
        resource: &'static str,
        attempted: u64,
        limit: u64,
    },

    #[error("{0}")]
    Internal(#[from] InternalError),
}

impl GroupError {
    /// Construct one grouped runtime invariant for missing numeric ingest
    /// payloads in grouped global DISTINCT SUM/AVG reduction.
    #[must_use]
    pub(in crate::db::executor) fn numeric_ingest_payload_required() -> Self {
        Self::from(InternalError::query_executor_invariant(
            "grouped global DISTINCT SUM/AVG reducer requires numeric ingest payload",
        ))
    }

    /// Convert grouped execution failures into executor-owned internal errors.
    #[must_use]
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::MemoryLimitExceeded { .. } | Self::DistinctBudgetExceeded { .. } => {
                InternalError::executor_internal(self.to_string())
            }
            Self::Internal(inner) => inner,
        }
    }
}
