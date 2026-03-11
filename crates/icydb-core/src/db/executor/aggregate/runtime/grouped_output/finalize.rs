//! Module: db::executor::aggregate::runtime::grouped_output::finalize
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_output::finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::{
            ExecutionOutcomeMetrics, GroupedCursorPage, GroupedFoldStage,
            GroupedRouteStageProjection, LoadExecutor,
        },
        plan_metrics::{
            record_rows_aggregated, record_rows_emitted, record_rows_filtered, record_rows_scanned,
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Finalize grouped output payloads and observability after grouped fold execution.
    pub(in crate::db::executor) fn finalize_grouped_output<R>(
        mut route: R,
        folded: GroupedFoldStage,
        execution_time_micros: u64,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>)
    where
        R: GroupedRouteStageProjection<E>,
    {
        let rows_returned = folded.rows_returned();
        let rows_aggregated = folded.filtered_rows();
        record_rows_aggregated::<E>(rows_aggregated);

        let metrics = ExecutionOutcomeMetrics {
            optimization: folded.optimization(),
            rows_scanned: folded.rows_scanned(),
            post_access_rows: rows_returned,
            index_predicate_applied: folded.index_predicate_applied(),
            index_predicate_keys_rejected: folded.index_predicate_keys_rejected(),
            distinct_keys_deduped: folded.distinct_keys_deduped(),
        };
        Self::finalize_path_outcome(
            route.execution_trace_mut(),
            metrics,
            false,
            execution_time_micros,
        );

        let mut span = crate::metrics::sink::Span::<E>::new(crate::metrics::sink::ExecKind::Load);
        span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
        if folded.should_check_filtered_rows_upper_bound() {
            debug_assert!(
                folded.filtered_rows() >= rows_returned,
                "grouped pagination must return at most filtered row cardinality",
            );
        }

        (folded.into_page(), route.into_execution_trace())
    }

    // Record shared observability outcome for any execution path.
    pub(in crate::db::executor) fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        metrics: ExecutionOutcomeMetrics,
        index_only: bool,
        execution_time_micros: u64,
    ) {
        let ExecutionOutcomeMetrics {
            optimization,
            rows_scanned,
            post_access_rows,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        } = metrics;
        record_rows_scanned::<E>(rows_scanned);
        let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
        record_rows_filtered::<E>(rows_filtered);
        record_rows_emitted::<E>(post_access_rows);

        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(
                optimization,
                rows_scanned,
                rows_scanned,
                post_access_rows,
                execution_time_micros,
                index_only,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
            );
        }
    }
}
