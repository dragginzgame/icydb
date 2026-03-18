//! Module: db::executor::aggregate::runtime::grouped_output::finalize
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_output::finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::{
            ExecutionOutcomeMetrics, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
            LoadExecutor,
        },
    },
    metrics::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

///
/// GroupedOutputRuntimeObserver
///
/// GroupedOutputRuntimeObserver keeps entity-typed grouped output metrics and
/// span recording behind one narrow observer boundary.
/// Shared grouped output finalization stays monomorphic and delegates only the
/// entity-bound observability updates.
///

pub(in crate::db::executor) trait GroupedOutputRuntimeObserver {
    /// Record grouped output metrics and execution-trace outcome for one completed page.
    fn finalize_grouped_observability(
        &self,
        execution_trace: &mut Option<ExecutionTrace>,
        metrics: ExecutionOutcomeMetrics,
        rows_aggregated: usize,
        rows_returned: usize,
        execution_time_micros: u64,
    );
}

// Finalize grouped output payloads and observability after grouped fold
// execution using a non-generic grouped page/fold contract.
pub(in crate::db::executor) fn finalize_grouped_output_with_observer(
    observer: &dyn GroupedOutputRuntimeObserver,
    mut route: GroupedRouteStage,
    folded: GroupedFoldStage,
    execution_time_micros: u64,
) -> (GroupedCursorPage, Option<ExecutionTrace>) {
    let rows_returned = folded.rows_returned();
    let rows_aggregated = folded.filtered_rows();
    let metrics = ExecutionOutcomeMetrics {
        optimization: folded.optimization(),
        rows_scanned: folded.rows_scanned(),
        post_access_rows: rows_returned,
        index_predicate_applied: folded.index_predicate_applied(),
        index_predicate_keys_rejected: folded.index_predicate_keys_rejected(),
        distinct_keys_deduped: folded.distinct_keys_deduped(),
    };
    observer.finalize_grouped_observability(
        route.execution_trace_mut(),
        metrics,
        rows_aggregated,
        rows_returned,
        execution_time_micros,
    );

    if folded.should_check_filtered_rows_upper_bound() {
        debug_assert!(
            folded.filtered_rows() >= rows_returned,
            "grouped pagination must return at most filtered row cardinality",
        );
    }

    (folded.into_page(), route.into_execution_trace())
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Finalize grouped output payloads and observability after grouped fold execution.
    pub(in crate::db::executor) fn finalize_grouped_output(
        &self,
        route: GroupedRouteStage,
        folded: GroupedFoldStage,
        execution_time_micros: u64,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        finalize_grouped_output_with_observer(self, route, folded, execution_time_micros)
    }

    // Record shared observability outcome for scalar/grouped execution paths.
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
        crate::db::executor::plan_metrics::record_rows_scanned::<E>(rows_scanned);
        let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
        crate::db::executor::plan_metrics::record_rows_filtered::<E>(rows_filtered);
        crate::db::executor::plan_metrics::record_rows_emitted::<E>(post_access_rows);

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

impl<E> GroupedOutputRuntimeObserver for LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn finalize_grouped_observability(
        &self,
        execution_trace: &mut Option<ExecutionTrace>,
        metrics: ExecutionOutcomeMetrics,
        rows_aggregated: usize,
        rows_returned: usize,
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
        crate::db::executor::plan_metrics::record_rows_aggregated::<E>(rows_aggregated);
        crate::db::executor::plan_metrics::record_rows_scanned::<E>(rows_scanned);
        let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
        crate::db::executor::plan_metrics::record_rows_filtered::<E>(rows_filtered);
        crate::db::executor::plan_metrics::record_rows_emitted::<E>(post_access_rows);

        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(
                optimization,
                rows_scanned,
                rows_scanned,
                post_access_rows,
                execution_time_micros,
                false,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
            );
        }

        let mut span = Span::<E>::new(ExecKind::Load);
        span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
    }
}
