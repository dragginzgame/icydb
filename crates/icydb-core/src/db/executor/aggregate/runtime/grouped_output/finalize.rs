//! Module: db::executor::aggregate::runtime::grouped_output::finalize
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_output::finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::{
            ExecutionOutcomeMetrics, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
        },
        plan_metrics::{
            record_rows_aggregated_for_path, record_rows_emitted_for_path,
            record_rows_filtered_for_path, record_rows_scanned_for_path,
        },
    },
    metrics::sink::{ExecKind, PathSpan},
};

///
/// GroupedOutputRuntimeObserverBindings
///
/// GroupedOutputRuntimeObserverBindings keeps entity-typed grouped output
/// observability behind one narrow function-table boundary.
/// Shared grouped output finalization stays monomorphic and delegates only the
/// entity-bound metrics/span leaf.
///

pub(in crate::db::executor) struct GroupedOutputRuntimeObserverBindings {
    entity_path: &'static str,
}

impl GroupedOutputRuntimeObserverBindings {
    /// Build one grouped output observer bundle from one typed executor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn new<E>() -> Self
    where
        E: crate::traits::EntityKind + crate::traits::EntityValue,
    {
        Self {
            entity_path: E::PATH,
        }
    }

    /// Record grouped output metrics and execution-trace outcome for one completed page.
    fn finalize_grouped_observability(
        &self,
        execution_trace: &mut Option<ExecutionTrace>,
        metrics: ExecutionOutcomeMetrics,
        rows_aggregated: usize,
        rows_returned: usize,
        execution_time_micros: u64,
    ) {
        finalize_grouped_observability_for_path(
            self.entity_path,
            execution_trace,
            metrics,
            rows_aggregated,
            rows_returned,
            execution_time_micros,
        );
    }
}

// Finalize grouped output payloads and observability after grouped fold
// execution using a non-generic grouped page/fold contract.
pub(in crate::db::executor) fn finalize_grouped_output_with_observer(
    observer: &GroupedOutputRuntimeObserverBindings,
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

// Record shared observability outcome for scalar/grouped execution paths.
pub(in crate::db::executor) fn finalize_path_outcome_for_path(
    entity_path: &'static str,
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
    record_rows_scanned_for_path(entity_path, rows_scanned);
    let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
    record_rows_filtered_for_path(entity_path, rows_filtered);
    record_rows_emitted_for_path(entity_path, post_access_rows);

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

fn finalize_grouped_observability_for_path(
    entity_path: &'static str,
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
    record_rows_aggregated_for_path(entity_path, rows_aggregated);
    record_rows_scanned_for_path(entity_path, rows_scanned);
    let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
    record_rows_filtered_for_path(entity_path, rows_filtered);
    record_rows_emitted_for_path(entity_path, post_access_rows);

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

    let mut span = PathSpan::new(ExecKind::Load, entity_path);
    span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
}
