//! Module: db::session::query::diagnostics::model
//! Responsibility: diagnostics attribution DTOs and executor counter projection.
//! Does not own: measured execution or query dispatch.
//! Boundary: shapes diagnostics wire payloads from already-captured phase counters.

use crate::db::executor::{
    DirectDataRowPhaseAttribution, GroupedCountAttribution as ExecutorGroupedCountAttribution,
    GroupedRuntimeAttribution as ExecutorGroupedRuntimeAttribution, KernelRowPhaseAttribution,
    ScalarAggregateTerminalAttribution,
};
use candid::CandidType;
use serde::Deserialize;

// DirectDataRowAttribution
//
// Candid diagnostics payload for direct scalar row execution counters.
// The short field names are scoped by the `direct_data_row` parent field on
// `QueryExecutionAttribution`.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct DirectDataRowAttribution {
    pub scan_local_instructions: u64,
    pub key_stream_local_instructions: u64,
    pub row_read_local_instructions: u64,
    pub key_encode_local_instructions: u64,
    pub store_get_local_instructions: u64,
    pub order_window_local_instructions: u64,
    pub page_window_local_instructions: u64,
}

impl DirectDataRowAttribution {
    #[cfg(any(test, feature = "sql"))]
    const fn from_direct_phase(phase: DirectDataRowPhaseAttribution) -> Option<Self> {
        if phase.has_work() {
            Some(Self::from_phase_unchecked(phase))
        } else {
            None
        }
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn from_captured_phase(
        phase: DirectDataRowPhaseAttribution,
    ) -> Option<Self> {
        Self::from_direct_phase(phase)
    }

    const fn from_phase_unchecked(phase: DirectDataRowPhaseAttribution) -> Self {
        Self {
            scan_local_instructions: phase.scan_local_instructions,
            key_stream_local_instructions: phase.key_stream_local_instructions,
            row_read_local_instructions: phase.row_read_local_instructions,
            key_encode_local_instructions: phase.key_encode_local_instructions,
            store_get_local_instructions: phase.store_get_local_instructions,
            order_window_local_instructions: phase.order_window_local_instructions,
            page_window_local_instructions: phase.page_window_local_instructions,
        }
    }
}

// KernelRowAttribution
//
// Candid diagnostics payload for retained/data kernel-row execution counters.
// The short field names are scoped by the `kernel_row` parent field on
// `QueryExecutionAttribution`.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct KernelRowAttribution {
    pub scan_local_instructions: u64,
    pub key_stream_local_instructions: u64,
    pub row_read_local_instructions: u64,
    pub order_window_local_instructions: u64,
    pub page_window_local_instructions: u64,
    pub retained_layout_hits: u64,
    pub retained_slot_values: u64,
    pub retained_octet_length_values: u64,
    /// Maximum kernel-row candidates retained by a successful scan collection.
    pub peak_retained_candidates: u64,
}

impl KernelRowAttribution {
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn from_captured_phase(
        phase: KernelRowPhaseAttribution,
    ) -> Option<Self> {
        Self::from_kernel_phase(phase)
    }

    const fn from_kernel_phase(phase: KernelRowPhaseAttribution) -> Option<Self> {
        if phase.has_work() {
            Some(Self {
                scan_local_instructions: phase.scan_local_instructions,
                key_stream_local_instructions: phase.key_stream_local_instructions,
                row_read_local_instructions: phase.row_read_local_instructions,
                order_window_local_instructions: phase.order_window_local_instructions,
                page_window_local_instructions: phase.page_window_local_instructions,
                retained_layout_hits: phase.retained_layout_hits,
                retained_slot_values: phase.retained_slot_values,
                retained_octet_length_values: phase.retained_octet_length_values,
                peak_retained_candidates: phase.peak_retained_candidates,
            })
        } else {
            None
        }
    }
}

// GroupedCountAttribution
//
// Candid diagnostics payload for grouped COUNT fold counters.
// This mirrors the executor-internal grouped-count attribution shape while
// remaining a public diagnostics wire type.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct GroupedCountAttribution {
    pub borrowed_hash_computations: u64,
    pub bucket_candidate_checks: u64,
    pub existing_group_hits: u64,
    pub new_group_inserts: u64,
    pub row_materialization_local_instructions: u64,
    pub group_lookup_local_instructions: u64,
    pub existing_group_update_local_instructions: u64,
    pub new_group_insert_local_instructions: u64,
}

impl GroupedCountAttribution {
    pub(in crate::db) const fn from_executor(count: ExecutorGroupedCountAttribution) -> Self {
        Self {
            borrowed_hash_computations: count.borrowed_hash_computations,
            bucket_candidate_checks: count.bucket_candidate_checks,
            existing_group_hits: count.existing_group_hits,
            new_group_inserts: count.new_group_inserts,
            row_materialization_local_instructions: count.row_materialization_local_instructions,
            group_lookup_local_instructions: count.group_lookup_local_instructions,
            existing_group_update_local_instructions: count
                .existing_group_update_local_instructions,
            new_group_insert_local_instructions: count.new_group_insert_local_instructions,
        }
    }
}

/// Candid diagnostics payload for grouped execution counters and physical state facts.
///
/// Stream, fold, finalize, runtime-state, and grouped-count metrics stay
/// together so grouped execution is not reconstructed at the session layer.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct GroupedExecutionAttribution {
    /// Local instructions consumed while building the grouped source stream.
    pub stream_local_instructions: u64,
    /// Local instructions consumed while folding grouped source rows.
    pub fold_local_instructions: u64,
    /// Local instructions consumed while finalizing the grouped response.
    pub finalize_local_instructions: u64,
    /// Candidate source rows read by grouped execution.
    pub rows_scanned: u64,
    /// Canonical groups observed by successful fold execution.
    pub groups_observed: u64,
    /// Canonical groups finalized by successful fold execution.
    pub groups_finalized: u64,
    /// Peak number of simultaneously live canonical groups.
    pub peak_live_groups: u64,
    /// Peak number of simultaneously live aggregate state slots.
    pub peak_live_aggregate_states: u64,
    /// Peak number of simultaneously live grouped DISTINCT values.
    pub peak_live_distinct_values: u64,
    /// Whether bounded ordered selection stopped the source scan early.
    pub early_scan_stop: bool,
    /// Dedicated grouped `COUNT(*)` hot-path attribution.
    pub count: GroupedCountAttribution,
}

impl GroupedExecutionAttribution {
    pub(in crate::db) const fn from_executor_parts(
        stream_local_instructions: u64,
        fold_local_instructions: u64,
        finalize_local_instructions: u64,
        runtime: ExecutorGroupedRuntimeAttribution,
        count: ExecutorGroupedCountAttribution,
    ) -> Self {
        Self {
            stream_local_instructions,
            fold_local_instructions,
            finalize_local_instructions,
            rows_scanned: runtime.rows_scanned,
            groups_observed: runtime.groups_observed,
            groups_finalized: runtime.groups_finalized,
            peak_live_groups: runtime.peak_live_groups,
            peak_live_aggregate_states: runtime.peak_live_aggregate_states,
            peak_live_distinct_values: runtime.peak_live_distinct_values,
            early_scan_stop: runtime.early_scan_stop,
            count: GroupedCountAttribution::from_executor(count),
        }
    }
}

///
/// ScalarAggregateAttribution
///
/// Candid diagnostics payload for scalar aggregate terminal execution.
/// This is shared by SQL and fluent terminal attribution so count/existence
/// paths do not need frontend-specific executor DTO conversion.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct ScalarAggregateAttribution {
    pub base_row_local_instructions: u64,
    pub reducer_fold_local_instructions: u64,
    pub expression_evaluations: u64,
    pub filter_evaluations: u64,
    pub rows_ingested: u64,
    pub terminal_count: u64,
    pub unique_input_expr_count: u64,
    pub unique_filter_expr_count: u64,
    pub sink_mode: Option<String>,
}

impl ScalarAggregateAttribution {
    /// Project executor scalar aggregate attribution into the shared diagnostics payload.
    ///
    /// Returns `None` when the executor reported no scalar aggregate work.
    pub(in crate::db) fn from_executor(
        terminal: ScalarAggregateTerminalAttribution,
    ) -> Option<Self> {
        if terminal.has_work() {
            Some(Self {
                base_row_local_instructions: terminal.base_row_local_instructions,
                reducer_fold_local_instructions: terminal.reducer_fold_local_instructions,
                expression_evaluations: terminal.expression_evaluations,
                filter_evaluations: terminal.filter_evaluations,
                rows_ingested: terminal.rows_ingested,
                terminal_count: terminal.terminal_count,
                unique_input_expr_count: terminal.unique_input_expr_count,
                unique_filter_expr_count: terminal.unique_filter_expr_count,
                sink_mode: terminal.sink_mode.label().map(str::to_string),
            })
        } else {
            None
        }
    }
}
