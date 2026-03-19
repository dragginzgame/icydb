//! Module: db::executor::pipeline::contracts::outcomes
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::outcomes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{ExecutionOptimization, pipeline::contracts::StructuralCursorPage};

///
/// MaterializedExecutionAttempt
///
/// Canonical materialization attempt output for load execution.
/// Preserves one shared boundary for retry accounting and page output.
///

pub(in crate::db::executor) struct MaterializedExecutionAttempt {
    pub(in crate::db::executor) page: StructuralCursorPage,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped: u64,
}

///
/// ExecutionOutcomeMetrics
///
/// Finalization-time observability metrics for one materialized load execution
/// attempt. Keeps path-outcome reporting fields grouped as one boundary payload.
///

pub(in crate::db::executor) struct ExecutionOutcomeMetrics {
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped: u64,
}

impl MaterializedExecutionAttempt {
    // Split one materialized execution attempt into response page + observability metrics.
    pub(in crate::db::executor) fn into_page_and_metrics(
        self,
    ) -> (StructuralCursorPage, ExecutionOutcomeMetrics) {
        let metrics = ExecutionOutcomeMetrics {
            optimization: self.optimization,
            rows_scanned: self.rows_scanned,
            post_access_rows: self.post_access_rows,
            index_predicate_applied: self.index_predicate_applied,
            index_predicate_keys_rejected: self.index_predicate_keys_rejected,
            distinct_keys_deduped: self.distinct_keys_deduped,
        };

        (self.page, metrics)
    }
}
