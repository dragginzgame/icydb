//! Module: executor::aggregate::contracts::error
//! Responsibility: aggregate runtime error taxonomy.
//! Does not own: planner-owned logical aggregate validation semantics.
//! Boundary: typed runtime failures shared by aggregate execution contracts.

use crate::{db::executor::budget::current_execution_budget_exceeded, error::InternalError};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

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

impl GroupBudgetResourceCode {
    const fn execution_resource(self) -> DiagnosticExecutionBudgetResource {
        match self {
            Self::DistinctValuesPerGroup | Self::DistinctValuesTotal | Self::Groups => {
                DiagnosticExecutionBudgetResource::GroupDistinctEntries
            }
            Self::EstimatedBytes => DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
        }
    }
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
            } => current_execution_budget_exceeded(resource.execution_resource(), limit, attempted),
            Self::Internal(inner) => inner,
        }
    }
}

impl From<InternalError> for GroupError {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupBudgetResourceCode, GroupError};
    use crate::db::{
        QueryError,
        executor::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
        DiagnosticExecutionLane, DiagnosticFactTag, RuntimeBoundaryCode,
    };

    #[test]
    fn grouped_local_limit_exhaustion_preserves_typed_execution_context() {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        );
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0x6772_6f75_702d_6361,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            Err::<(), _>(QueryError::execute(
                GroupError::memory_limit_exceeded(
                    GroupBudgetResourceCode::EstimatedBytes,
                    2_048,
                    1_024,
                )
                .into_internal_error(),
            ))
        })
        .expect_err("grouped local state exhaustion should remain typed");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (
                    DiagnosticFactTag::BudgetResource,
                    DiagnosticExecutionBudgetResource::GroupDistinctStateBytes.raw(),
                ),
                (DiagnosticFactTag::Limit, 1_024),
                (DiagnosticFactTag::Actual, 2_048),
                (DiagnosticFactTag::ExecutionBudgetScope, 1),
                (DiagnosticFactTag::ExecutionLane, 2),
                (
                    DiagnosticFactTag::QueryShapeFingerprintPrefix,
                    0x6772_6f75_702d_6361,
                ),
            ],
        );
    }
}
