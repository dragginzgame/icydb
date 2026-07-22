//! Module: db::session::sql::execute::write::candidate
//! Responsibility: SQL write candidate row accounting, bounds, and staged-row
//! buffers.
//! Does not own: SQL write execution, key decoding, or returning projection
//! shaping.
//! Boundary: keeps candidate resource policy separate from INSERT/UPDATE/
//! DELETE execution.

use crate::{
    db::{
        QueryError,
        data::AcceptedMutationIntentPatch,
        session::sql::{
            SqlExactUpdatePolicy, combined_optional_row_bound,
            write_policy::SqlWriteExecutionBounds,
        },
        sql::parser::SqlReturningProjection,
    },
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

const SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS: usize = 64;

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy)]
pub(super) struct SqlWriteCandidateAccounting {
    semantic_candidates: SqlWriteCandidateRows,
    matched_candidates: SqlWriteCandidateRows,
    mutated_rows: usize,
    returning_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SqlWriteCandidateRows(usize);

impl SqlWriteCandidateRows {
    pub(super) const fn from_len(len: usize) -> Self {
        Self(len)
    }

    pub(super) const fn len(self) -> usize {
        self.0
    }

    pub(super) fn from_delete_count(row_count: u32) -> Self {
        Self(usize::try_from(row_count).unwrap_or(usize::MAX))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SqlWriteProjectedSourceRows(usize);

impl SqlWriteProjectedSourceRows {
    pub(super) const fn from_len(len: usize) -> Self {
        Self(len)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SqlWriteCandidateBoundCheck {
    InsertValuesSource,
    MutationBatchHandoff,
    SelectorSourceBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SqlWriteCandidateDiagnostics {
    pub(super) projected_source_rows: Option<SqlWriteProjectedSourceRows>,
    pub(super) semantic_candidates: SqlWriteCandidateRows,
    over_limit_at: Option<SqlWriteCandidateBoundCheck>,
}

impl SqlWriteCandidateDiagnostics {
    const fn within_limit(semantic_candidates: SqlWriteCandidateRows) -> Self {
        Self {
            projected_source_rows: None,
            semantic_candidates,
            over_limit_at: None,
        }
    }

    const fn over_limit(
        semantic_candidates: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> Self {
        Self {
            projected_source_rows: None,
            semantic_candidates,
            over_limit_at: Some(at),
        }
    }

    pub(super) const fn over_limit_at(self) -> Option<SqlWriteCandidateBoundCheck> {
        self.over_limit_at
    }

    pub(super) const fn projected_source_rows(self) -> Option<SqlWriteProjectedSourceRows> {
        self.projected_source_rows
    }

    const fn with_projected_source_rows(
        self,
        projected_source_rows: Option<SqlWriteProjectedSourceRows>,
    ) -> Self {
        Self {
            projected_source_rows,
            ..self
        }
    }
}

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

    pub(super) fn diagnostics_at(
        self,
        candidate_rows: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> SqlWriteCandidateDiagnostics {
        let Some(max_rows) = self.max_rows else {
            return SqlWriteCandidateDiagnostics::within_limit(candidate_rows);
        };
        let max_rows = usize::try_from(max_rows).unwrap_or(usize::MAX);
        if candidate_rows.len() <= max_rows {
            return SqlWriteCandidateDiagnostics::within_limit(candidate_rows);
        }

        SqlWriteCandidateDiagnostics::over_limit(candidate_rows, at)
    }

    pub(super) fn validate_at(
        self,
        candidate_rows: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> Result<SqlWriteCandidateDiagnostics, QueryError> {
        let diagnostics = self.diagnostics_at(candidate_rows, at);
        if diagnostics.over_limit_at().is_none() {
            return Ok(diagnostics);
        }

        Err(QueryError::sql_write_boundary(self.overflow_boundary))
    }
}

pub(super) fn sql_update_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
) -> SqlWriteCandidateBounds {
    SqlWriteCandidateBounds::from_max_rows(
        execution_bounds.and_then(|bounds| bounds.max_staged_rows),
    )
}

pub(super) const fn sql_exact_update_candidate_bounds(
    policy: SqlExactUpdatePolicy,
) -> SqlWriteCandidateBounds {
    SqlWriteCandidateBounds::exact_update(policy)
}

pub(super) const fn sql_insert_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
    returning: bool,
) -> SqlWriteCandidateBounds {
    let Some(execution_bounds) = execution_bounds else {
        return SqlWriteCandidateBounds::from_max_rows(None);
    };

    if !returning {
        return SqlWriteCandidateBounds::from_max_rows(execution_bounds.max_staged_rows);
    }

    SqlWriteCandidateBounds::from_max_rows(combined_optional_row_bound(
        execution_bounds.max_staged_rows,
        execution_bounds.returning.max_rows,
    ))
}

impl SqlWriteCandidateAccounting {
    pub(super) const fn mutation_batch(
        staged_rows: SqlWriteCandidateRows,
        mutated_rows: usize,
        returning: Option<&SqlReturningProjection>,
    ) -> Self {
        Self {
            semantic_candidates: staged_rows,
            matched_candidates: staged_rows,
            mutated_rows,
            returning_rows: if returning.is_some() { mutated_rows } else { 0 },
        }
    }

    pub(super) const fn delete_count(
        candidate_rows: SqlWriteCandidateRows,
        returning: bool,
    ) -> Self {
        Self {
            semantic_candidates: candidate_rows,
            matched_candidates: candidate_rows,
            mutated_rows: candidate_rows.len(),
            returning_rows: if returning { candidate_rows.len() } else { 0 },
        }
    }

    pub(super) fn staged_metric(self) -> u64 {
        usize_to_u64_saturating(self.semantic_candidates.len())
    }

    pub(super) fn matched_metric(self) -> u64 {
        usize_to_u64_saturating(self.matched_candidates.len())
    }

    pub(super) fn mutated_metric(self) -> u64 {
        usize_to_u64_saturating(self.mutated_rows)
    }

    pub(super) fn returning_metric(self) -> u64 {
        usize_to_u64_saturating(self.returning_rows)
    }
}

pub(super) struct SqlWriteMutationBatch<K> {
    rows: Vec<(K, AcceptedMutationIntentPatch)>,
}

impl<K> SqlWriteMutationBatch<K> {
    const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    pub(super) fn push(&mut self, key: K, patch: AcceptedMutationIntentPatch) {
        self.rows.push((key, patch));
    }

    const fn staged_rows(&self) -> SqlWriteCandidateRows {
        SqlWriteCandidateRows(self.rows.len())
    }

    pub(super) fn into_rows(self) -> Vec<(K, AcceptedMutationIntentPatch)> {
        self.rows
    }
}

pub(super) struct SqlWriteCandidateCollection<K> {
    diagnostics: SqlWriteCandidateDiagnostics,
    rows: SqlWriteMutationBatch<K>,
}

impl<K> SqlWriteCandidateCollection<K> {
    pub(super) const fn new() -> Self {
        Self {
            diagnostics: SqlWriteCandidateDiagnostics::within_limit(SqlWriteCandidateRows(0)),
            rows: SqlWriteMutationBatch::new(),
        }
    }

    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            diagnostics: SqlWriteCandidateDiagnostics::within_limit(SqlWriteCandidateRows(0)),
            rows: SqlWriteMutationBatch::with_capacity(capacity),
        }
    }

    pub(super) fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    pub(super) fn push(&mut self, key: K, patch: AcceptedMutationIntentPatch) {
        self.rows.push(key, patch);
        self.diagnostics.semantic_candidates = self.staged_rows();
    }

    const fn staged_rows(&self) -> SqlWriteCandidateRows {
        self.rows.staged_rows()
    }

    pub(super) const fn record_projected_source_rows(
        &mut self,
        source_rows: SqlWriteProjectedSourceRows,
    ) {
        self.diagnostics.projected_source_rows = Some(source_rows);
    }

    #[cfg(test)]
    const fn diagnostics(&self) -> SqlWriteCandidateDiagnostics {
        self.diagnostics
    }

    pub(super) fn validate_staged_rows_at(
        &mut self,
        bounds: SqlWriteCandidateBounds,
        at: SqlWriteCandidateBoundCheck,
    ) -> Result<SqlWriteCandidateRows, QueryError> {
        let staged_rows = self.staged_rows();
        self.diagnostics = bounds
            .validate_at(staged_rows, at)?
            .with_projected_source_rows(self.diagnostics.projected_source_rows());

        Ok(staged_rows)
    }

    pub(super) fn into_batch(self) -> SqlWriteMutationBatch<K> {
        self.rows
    }
}

pub(super) fn sql_write_candidate_collection_capacity(projected_rows: &[Vec<Value>]) -> usize {
    projected_rows
        .len()
        .min(SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS)
}

#[cfg(test)]
mod tests {
    use super::{
        SqlWriteCandidateAccounting, SqlWriteCandidateBoundCheck, SqlWriteCandidateBounds,
        SqlWriteCandidateCollection, SqlWriteCandidateRows, SqlWriteProjectedSourceRows,
    };
    use crate::db::data::AcceptedMutationIntentPatch;
    use icydb_diagnostic_code::{DiagnosticDetail, SqlWriteBoundaryCode};

    #[test]
    fn sql_write_candidate_row_bound_accepts_unbounded_and_within_limit() {
        SqlWriteCandidateBounds::from_max_rows(None)
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("unbounded candidate rows should be accepted");
        SqlWriteCandidateBounds::from_max_rows(Some(2))
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("candidate rows equal to the bound should be accepted");
    }

    #[test]
    fn sql_write_candidate_row_bound_rejects_over_limit() {
        let err = SqlWriteCandidateBounds::from_max_rows(Some(1))
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect_err("candidate rows over the bound should reject");

        assert_eq!(
            err.diagnostic().detail(),
            Some(&DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
            }),
        );
    }

    #[test]
    fn sql_write_candidate_bounds_report_over_limit_stage() {
        let diagnostics = SqlWriteCandidateBounds::from_max_rows(Some(1)).diagnostics_at(
            SqlWriteCandidateRows(2),
            SqlWriteCandidateBoundCheck::SelectorSourceBatch,
        );

        assert_eq!(diagnostics.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(
            diagnostics.over_limit_at(),
            Some(SqlWriteCandidateBoundCheck::SelectorSourceBatch),
        );

        let within_limit = SqlWriteCandidateBounds::from_max_rows(Some(2)).diagnostics_at(
            SqlWriteCandidateRows(2),
            SqlWriteCandidateBoundCheck::InsertValuesSource,
        );

        assert_eq!(within_limit.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(within_limit.over_limit_at(), None);
    }

    #[test]
    fn sql_write_candidate_collection_validates_staged_rows_from_buffer() {
        let mut rows = SqlWriteCandidateCollection::<u64>::new();
        rows.push(1, AcceptedMutationIntentPatch::new());
        rows.push(2, AcceptedMutationIntentPatch::new());

        let staged_rows = rows
            .validate_staged_rows_at(
                SqlWriteCandidateBounds::from_max_rows(Some(2)),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("batch staged rows at the bound should be accepted");

        assert_eq!(staged_rows.len(), 2);
        assert!(
            rows.validate_staged_rows_at(
                SqlWriteCandidateBounds::from_max_rows(Some(1)),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .is_err(),
            "batch staged rows over the bound should reject",
        );
    }

    #[test]
    fn sql_write_candidate_collection_tracks_projected_source_rows() {
        let mut rows = SqlWriteCandidateCollection::<u64>::with_capacity(3);
        rows.record_projected_source_rows(SqlWriteProjectedSourceRows::from_len(3));
        rows.push(1, AcceptedMutationIntentPatch::new());
        rows.push(2, AcceptedMutationIntentPatch::new());

        rows.validate_staged_rows_at(
            SqlWriteCandidateBounds::from_max_rows(Some(2)),
            SqlWriteCandidateBoundCheck::SelectorSourceBatch,
        )
        .expect("selector source rows at the semantic candidate bound should be accepted");

        let diagnostics = rows.diagnostics();
        assert_eq!(diagnostics.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(
            diagnostics.projected_source_rows(),
            Some(SqlWriteProjectedSourceRows::from_len(3)),
        );
        assert_eq!(diagnostics.over_limit_at(), None);
    }

    #[test]
    fn sql_write_candidate_accounting_counts_delete_rows_and_returning() {
        let count = SqlWriteCandidateAccounting::delete_count(SqlWriteCandidateRows(3), false);
        assert_eq!(count.staged_metric(), 3);
        assert_eq!(count.matched_metric(), 3);
        assert_eq!(count.mutated_metric(), 3);
        assert_eq!(count.returning_metric(), 0);

        let returning = SqlWriteCandidateAccounting::delete_count(SqlWriteCandidateRows(3), true);
        assert_eq!(returning.staged_metric(), 3);
        assert_eq!(returning.matched_metric(), 3);
        assert_eq!(returning.mutated_metric(), 3);
        assert_eq!(returning.returning_metric(), 3);
    }
}
