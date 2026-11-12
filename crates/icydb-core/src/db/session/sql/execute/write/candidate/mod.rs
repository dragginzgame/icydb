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
mod tests;
