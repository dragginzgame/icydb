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
