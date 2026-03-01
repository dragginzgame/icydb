//! Module: executor::aggregate::contracts::error
//! Responsibility: aggregate contract error taxonomy.
//! Does not own: executor orchestration or planner routing behavior.
//! Boundary: typed errors shared by aggregate contract helpers.

use crate::{db::query::plan::AggregateKind, error::InternalError};
use thiserror::Error as ThisError;

///
/// AggregateSpecSupportError
///
/// Canonical unsupported taxonomy for aggregate spec shape validation.
/// Keeps field-target capability errors explicit before runtime execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum AggregateSpecSupportError {
    #[error(
        "field-target aggregates are only supported for min/max terminals: {kind:?}({target_field})"
    )]
    FieldTargetRequiresExtrema {
        kind: AggregateKind,
        target_field: String,
    },
}

///
/// GroupAggregateSpecSupportError
///
/// Canonical unsupported taxonomy for grouped aggregate contract validation.
/// Keeps GROUP BY contract shape failures explicit before execution is enabled.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum GroupAggregateSpecSupportError {
    #[error("group aggregate spec requires at least one aggregate terminal")]
    MissingAggregateSpecs,

    #[error("group aggregate spec has duplicate group key: {field}")]
    DuplicateGroupKey { field: String },

    #[error("group aggregate spec contains unsupported terminal at index={index}: {source}")]
    AggregateSpecUnsupported {
        index: usize,
        #[source]
        source: AggregateSpecSupportError,
    },
}

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

    #[error("{0}")]
    Internal(#[from] InternalError),
}
