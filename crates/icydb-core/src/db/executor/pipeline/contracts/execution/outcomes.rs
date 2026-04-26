//! Module: db::executor::pipeline::contracts::outcomes
//! Defines execution outcome contracts reported by scalar pipeline entrypoints.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    ExecutionOptimization, pipeline::contracts::StructuralCursorPage, terminal::KernelRow,
};

/// Shared materialization payload for one scalar execution attempt.
pub(in crate::db::executor) type MaterializedExecutionPayload = StructuralCursorPage;

///
/// MaterializedExecutionAttempt
///
/// Canonical materialization attempt output for load execution.
/// Preserves one shared boundary for retry accounting and page output.
///

pub(in crate::db::executor) struct MaterializedExecutionAttempt {
    pub(in crate::db::executor) payload: MaterializedExecutionPayload,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped: u64,
}

///
/// KernelRowsExecutionAttempt
///
/// KernelRowsExecutionAttempt is the scalar-runtime output used by executor
/// consumers that need post-access/windowed rows but do not need a structural
/// page payload. Scalar aggregate terminals use it to reduce rows before
/// retained-slot page materialization would otherwise run.
///

pub(in crate::db::executor) struct KernelRowsExecutionAttempt {
    pub(in crate::db::executor) rows: Vec<KernelRow>,
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
    // Split one materialized execution attempt into payload + observability metrics.
    pub(in crate::db::executor) fn into_payload_and_metrics(
        self,
    ) -> (MaterializedExecutionPayload, ExecutionOutcomeMetrics) {
        let metrics = ExecutionOutcomeMetrics {
            optimization: self.optimization,
            rows_scanned: self.rows_scanned,
            post_access_rows: self.post_access_rows,
            index_predicate_applied: self.index_predicate_applied,
            index_predicate_keys_rejected: self.index_predicate_keys_rejected,
            distinct_keys_deduped: self.distinct_keys_deduped,
        };

        (self.payload, metrics)
    }
}
