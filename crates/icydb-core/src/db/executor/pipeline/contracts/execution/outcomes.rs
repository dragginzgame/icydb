//! Module: db::executor::pipeline::contracts::outcomes
//! Defines execution outcome contracts reported by scalar pipeline entrypoints.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::pipeline::contracts::StructuralCursorPage;
use crate::db::executor::terminal::KernelRow;

///
/// ScalarPageMaterialization
///
/// Route-independent scalar page materialization result.
/// Keeps terminal scan accounting named until route-owned metrics complete the
/// full execution attempt.
///

pub(in crate::db::executor) struct ScalarPageMaterialization {
    pub(in crate::db::executor) payload: StructuralCursorPage,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
}

///
/// MaterializedExecutionAttempt
///
/// Canonical materialization attempt output for load execution.
/// Preserves one shared boundary for retry accounting and page output.
///

pub(in crate::db::executor) struct MaterializedExecutionAttempt {
    pub(in crate::db::executor) payload: StructuralCursorPage,
    pub(in crate::db::executor) metrics: ExecutionOutcomeMetrics,
}

impl MaterializedExecutionAttempt {
    // Split one materialized execution attempt into payload + observability metrics.
    pub(in crate::db::executor) fn into_payload_and_metrics(
        self,
    ) -> (StructuralCursorPage, ExecutionOutcomeMetrics) {
        (self.payload, self.metrics)
    }
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
    pub(in crate::db::executor) metrics: ExecutionOutcomeMetrics,
}

///
/// ExecutionOutcomeMetrics
///
/// Shared scan accounting for scalar execution attempts.
///

pub(in crate::db::executor) struct ExecutionOutcomeMetrics {
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
}

impl ExecutionOutcomeMetrics {
    // Accumulate residual-retry work counters while retaining terminal-state
    // metrics from the latest attempt.
    pub(in crate::db::executor) const fn merge_residual_retry_attempt(self, latest: Self) -> Self {
        Self {
            rows_scanned: self.rows_scanned.saturating_add(latest.rows_scanned),
            post_access_rows: latest.post_access_rows,
        }
    }
}
