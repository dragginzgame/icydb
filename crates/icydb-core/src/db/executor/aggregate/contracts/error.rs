//! Module: executor::aggregate::contracts::error
//! Responsibility: aggregate runtime error taxonomy.
//! Does not own: planner-owned logical aggregate validation semantics.
//! Boundary: typed runtime failures shared by aggregate execution contracts.

use crate::error::InternalError;

///
/// GroupBudgetResourceCode
///
/// Compact grouped-resource bucket for budget-limit diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupBudgetResourceCode {
    DistinctValuesPerGroup,
    DistinctValuesTotal,
    EstimatedBytes,
    Groups,
}

///
/// GroupError
///
/// GroupError is the typed grouped-execution error surface.
/// This taxonomy keeps grouped memory-limit failures explicit and prevents
/// grouped resource guardrails from degrading into generic internal errors.
///

#[derive(Debug)]
pub(in crate::db::executor) enum GroupError {
    MemoryLimitExceeded {
        resource: GroupBudgetResourceCode,
        attempted: u64,
        limit: u64,
    },

    DistinctBudgetExceeded {
        resource: GroupBudgetResourceCode,
        attempted: u64,
        limit: u64,
    },

    Internal(InternalError),
}

impl GroupError {
    /// Construct one grouped execution memory-limit failure.
    #[must_use]
    pub(in crate::db::executor) const fn memory_limit_exceeded(
        resource: GroupBudgetResourceCode,
        attempted: u64,
        limit: u64,
    ) -> Self {
        Self::MemoryLimitExceeded {
            resource,
            attempted,
            limit,
        }
    }

    /// Construct one grouped DISTINCT budget failure.
    #[must_use]
    pub(in crate::db::executor) const fn distinct_budget_exceeded(
        resource: GroupBudgetResourceCode,
        attempted: u64,
        limit: u64,
    ) -> Self {
        Self::DistinctBudgetExceeded {
            resource,
            attempted,
            limit,
        }
    }

    /// Construct one grouped runtime invariant for missing numeric ingest
    /// payloads in grouped global DISTINCT SUM/AVG reduction.
    #[must_use]
    pub(in crate::db::executor) fn numeric_ingest_payload_required() -> Self {
        Self::from(InternalError::query_executor_invariant())
    }

    /// Convert grouped execution failures into executor-owned internal errors.
    #[must_use]
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::MemoryLimitExceeded {
                resource,
                attempted,
                limit,
            }
            | Self::DistinctBudgetExceeded {
                resource,
                attempted,
                limit,
            } => {
                let _ = (resource, attempted, limit);
                InternalError::executor_internal()
            }
            Self::Internal(inner) => inner,
        }
    }
}

impl From<InternalError> for GroupError {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}
