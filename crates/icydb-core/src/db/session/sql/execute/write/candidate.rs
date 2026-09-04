//! Module: db::session::sql::execute::write::candidate
//! Responsibility: SQL write candidate row accounting, bounds, and staged-row
//! buffers.
//! Does not own: SQL write execution, key decoding, or returning projection
//! shaping.
//! Boundary: keeps candidate resource policy separate from INSERT/UPDATE/
//! DELETE execution.

use crate::db::{
    QueryError,
    data::AcceptedMutationIntentPatch,
    session::sql::{SqlExactUpdatePolicy, write_policy::SqlWriteExecutionBounds},
};
use icydb_diagnostic_code::{DiagnosticFactTag, SqlWriteBoundaryCode};

const SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SqlWriteCandidateBounds {
    max_rows: Option<u32>,
    overflow_boundary: SqlWriteBoundaryCode,
}

impl SqlWriteCandidateBounds {
    pub(super) const fn from_max_rows(max_rows: Option<u32>) -> Self {
        Self {
            max_rows,
            overflow_boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
        }
    }

    pub(super) const fn exact_update(policy: SqlExactUpdatePolicy) -> Self {
        Self {
            max_rows: Some(policy.require_affected_at_most()),
            overflow_boundary: SqlWriteBoundaryCode::ExactUpdateAffectedRowsExceeded,
        }
    }

    #[cfg(test)]
    pub(super) const fn max_rows(self) -> Option<u32> {
        self.max_rows
    }

    pub(super) fn validate_len(self, candidate_rows: usize) -> Result<(), QueryError> {
        let Some(max_rows) = self.max_rows else {
            return Ok(());
        };
        if candidate_rows <= usize::try_from(max_rows).unwrap_or(usize::MAX) {
            return Ok(());
        }

        Err(QueryError::sql_write_boundary_with_facts(
            self.overflow_boundary,
            vec![
                (DiagnosticFactTag::ActualCount, candidate_rows as u64),
                (DiagnosticFactTag::Limit, u64::from(max_rows)),
            ],
        ))
    }
}

pub(super) const fn sql_exact_update_candidate_bounds(
    policy: SqlExactUpdatePolicy,
) -> SqlWriteCandidateBounds {
    SqlWriteCandidateBounds::exact_update(policy)
}

pub(super) const fn sql_write_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
) -> SqlWriteCandidateBounds {
    let Some(execution_bounds) = execution_bounds else {
        return SqlWriteCandidateBounds::from_max_rows(None);
    };

    SqlWriteCandidateBounds::from_max_rows(execution_bounds.max_candidate_rows())
}

pub(super) struct SqlWriteMutationBatch<K> {
    rows: Vec<(K, AcceptedMutationIntentPatch)>,
}

impl<K> SqlWriteMutationBatch<K> {
    pub(super) const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    pub(super) fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    pub(super) fn push(&mut self, key: K, patch: AcceptedMutationIntentPatch) {
        self.rows.push((key, patch));
    }

    pub(super) fn validate_bounds(
        &self,
        bounds: SqlWriteCandidateBounds,
    ) -> Result<(), QueryError> {
        bounds.validate_len(self.rows.len())
    }

    pub(super) fn into_rows(self) -> Vec<(K, AcceptedMutationIntentPatch)> {
        self.rows
    }
}

pub(super) const fn sql_write_mutation_batch_capacity(projected_rows: usize) -> usize {
    if projected_rows < SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS {
        projected_rows
    } else {
        SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS
    }
}

#[cfg(test)]
mod tests {
    use super::{SqlWriteCandidateBounds, SqlWriteMutationBatch, sql_write_candidate_bounds};
    use crate::db::{
        data::AcceptedMutationIntentPatch,
        session::sql::{SqlWriteExecutionBounds, SqlWriteReturningBounds},
    };
    use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, SqlWriteBoundaryCode};

    #[test]
    fn sql_write_candidate_row_bound_accepts_unbounded_and_within_limit() {
        SqlWriteCandidateBounds::from_max_rows(None)
            .validate_len(2)
            .expect("unbounded candidate rows should be accepted");
        SqlWriteCandidateBounds::from_max_rows(Some(2))
            .validate_len(2)
            .expect("candidate rows equal to the bound should be accepted");
    }

    #[test]
    fn sql_write_candidate_row_bound_rejects_over_limit() {
        let err = SqlWriteCandidateBounds::from_max_rows(Some(1))
            .validate_len(2)
            .expect_err("candidate rows over the bound should reject");

        assert_eq!(
            err.diagnostic().detail(),
            Some(&DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
            }),
        );
        assert_eq!(
            err.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualCount, 2),
                (DiagnosticFactTag::Limit, 1),
            ],
        );
    }

    #[test]
    fn sql_write_candidate_bounds_use_tighter_staged_or_returning_cap() {
        let returning_is_tighter = SqlWriteExecutionBounds {
            max_staged_rows: Some(5),
            returning: SqlWriteReturningBounds {
                max_rows: Some(3),
                max_response_bytes: None,
            },
        };
        assert_eq!(
            sql_write_candidate_bounds(Some(returning_is_tighter)).max_rows(),
            Some(3),
        );

        let staged_is_tighter = SqlWriteExecutionBounds {
            max_staged_rows: Some(2),
            returning: SqlWriteReturningBounds {
                max_rows: Some(4),
                max_response_bytes: None,
            },
        };
        assert_eq!(
            sql_write_candidate_bounds(Some(staged_is_tighter)).max_rows(),
            Some(2),
        );
        assert_eq!(sql_write_candidate_bounds(None).max_rows(), None);
    }

    #[test]
    fn sql_write_mutation_batch_validates_its_staged_rows() {
        let mut rows = SqlWriteMutationBatch::<u64>::new();
        rows.push(1, AcceptedMutationIntentPatch::new());
        rows.push(2, AcceptedMutationIntentPatch::new());

        rows.validate_bounds(SqlWriteCandidateBounds::from_max_rows(Some(2)))
            .expect("batch staged rows at the bound should be accepted");

        assert!(
            rows.validate_bounds(SqlWriteCandidateBounds::from_max_rows(Some(1)))
                .is_err(),
            "batch staged rows over the bound should reject",
        );
    }
}
