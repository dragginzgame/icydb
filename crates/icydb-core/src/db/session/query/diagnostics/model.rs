//! Module: db::session::query::diagnostics::model
//! Responsibility: diagnostics attribution DTOs and executor counter projection.
//! Does not own: measured execution or query dispatch.
//! Boundary: shapes diagnostics wire payloads from already-captured phase counters.

use crate::db::{
    diagnostics::StoreCounterSnapshot,
    executor::{
        DirectDataRowPhaseAttribution, GroupedCountAttribution as ExecutorGroupedCountAttribution,
        GroupedExecutePhaseAttribution,
        GroupedRuntimeAttribution as ExecutorGroupedRuntimeAttribution, KernelRowPhaseAttribution,
        ScalarAggregateTerminalAttribution, ScalarExecutePhaseAttribution,
    },
    query::read_intent::ReadIntentKind,
    session::query::{QueryPlanCacheAttribution, QueryPlanCompilePhaseAttribution},
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

    pub(in crate::db) const fn from_scalar_phase(phase: ScalarExecutePhaseAttribution) -> Self {
        Self::from_phase_unchecked(DirectDataRowPhaseAttribution {
            scan_local_instructions: phase.direct_data_row_scan_local_instructions,
            key_stream_local_instructions: phase.direct_data_row_key_stream_local_instructions,
            row_read_local_instructions: phase.direct_data_row_row_read_local_instructions,
            key_encode_local_instructions: phase.direct_data_row_key_encode_local_instructions,
            store_get_local_instructions: phase.direct_data_row_store_get_local_instructions,
            order_window_local_instructions: phase.direct_data_row_order_window_local_instructions,
            page_window_local_instructions: phase.direct_data_row_page_window_local_instructions,
        })
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
    pub(in crate::db) const fn from_scalar_phase(
        phase: ScalarExecutePhaseAttribution,
    ) -> Option<Self> {
        Self::from_kernel_phase(KernelRowPhaseAttribution {
            scan_local_instructions: phase.kernel_row_scan_local_instructions,
            key_stream_local_instructions: phase.kernel_row_key_stream_local_instructions,
            row_read_local_instructions: phase.kernel_row_row_read_local_instructions,
            order_window_local_instructions: phase.kernel_row_order_window_local_instructions,
            page_window_local_instructions: phase.kernel_row_page_window_local_instructions,
            retained_layout_hits: phase.kernel_row_retained_layout_hits,
            retained_slot_values: phase.kernel_row_retained_slot_values,
            retained_octet_length_values: phase.kernel_row_retained_octet_length_values,
            peak_retained_candidates: phase.kernel_row_peak_retained_candidates,
        })
    }

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
    pub(in crate::db) const fn from_executor_phase(phase: GroupedExecutePhaseAttribution) -> Self {
        Self::from_executor_parts(
            phase.stream_local_instructions,
            phase.fold_local_instructions,
            phase.finalize_local_instructions,
            phase.runtime,
            phase.grouped_count,
        )
    }

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

///
/// FluentTerminalExecutionAttribution
///
/// Diagnostics payload for one fluent scalar terminal call. Terminal calls use
/// the same prepared-plan and executor internals as page queries, but report
/// scalar aggregate work separately because terminals do not build page
/// response envelopes.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct FluentTerminalExecutionAttribution {
    pub read_intent: ReadIntentKind,
    pub compile_local_instructions: u64,
    pub compile_schema_catalog_local_instructions: u64,
    pub compile_schema_info_local_instructions: u64,
    pub compile_prepare_local_instructions: u64,
    pub compile_cache_key_local_instructions: u64,
    pub compile_cache_lookup_local_instructions: u64,
    pub compile_plan_build_local_instructions: u64,
    pub compile_cache_insert_local_instructions: u64,
    pub plan_lookup_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub store_get_calls: u64,
    pub index_store_get_calls: u64,
    pub index_store_range_scan_calls: u64,
    pub index_store_entry_reads: u64,
    pub scalar_aggregate: Option<ScalarAggregateAttribution>,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

///
/// QueryAttributionCommon
///
/// QueryAttributionCommon carries compile/cache/store counters shared by
/// paged query attribution and fluent scalar terminal attribution. It keeps the
/// two public DTO builders aligned without changing their public field shapes.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::session::query) struct QueryAttributionCommon {
    compile_phase_attribution: QueryPlanCompilePhaseAttribution,
    plan_lookup_local_instructions: u64,
    store_counters: StoreCounterSnapshot,
    cache_attribution: QueryPlanCacheAttribution,
}

impl QueryAttributionCommon {
    #[must_use]
    pub(in crate::db::session::query) const fn new(
        plan_lookup_local_instructions: u64,
        compile_phase_attribution: QueryPlanCompilePhaseAttribution,
        cache_attribution: QueryPlanCacheAttribution,
        store_counters: StoreCounterSnapshot,
    ) -> Self {
        Self {
            compile_phase_attribution,
            plan_lookup_local_instructions,
            store_counters,
            cache_attribution,
        }
    }

    const fn compile_local_instructions(self) -> u64 {
        self.plan_lookup_local_instructions
    }

    const fn total_local_instructions(self, execute_local_instructions: u64) -> u64 {
        self.compile_local_instructions()
            .saturating_add(execute_local_instructions)
    }
}

// QueryExecutionAttribution
//
// QueryExecutionAttribution records the top-level compile/execute split for
// typed/fluent query execution at the session boundary.
// Every field is an additive counter where zero means no observed work or no
// observed event for that bucket. Path-specific counters are present only for
// the execution path that produced them.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct QueryExecutionAttribution {
    pub read_intent: ReadIntentKind,
    pub compile_local_instructions: u64,
    pub compile_schema_catalog_local_instructions: u64,
    pub compile_schema_info_local_instructions: u64,
    pub compile_prepare_local_instructions: u64,
    pub compile_cache_key_local_instructions: u64,
    pub compile_cache_lookup_local_instructions: u64,
    pub compile_plan_build_local_instructions: u64,
    pub compile_cache_insert_local_instructions: u64,
    pub plan_lookup_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub load_plan_local_instructions: u64,
    pub row_layout_local_instructions: u64,
    pub continuation_signature_local_instructions: u64,
    pub scalar_runtime_handoff_local_instructions: u64,
    pub route_plan_local_instructions: u64,
    pub runtime_prepare_local_instructions: u64,
    pub runtime_local_instructions: u64,
    pub finalize_local_instructions: u64,
    pub direct_data_row: Option<DirectDataRowAttribution>,
    pub kernel_row: Option<KernelRowAttribution>,
    pub grouped: Option<GroupedExecutionAttribution>,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub store_get_calls: u64,
    pub index_store_get_calls: u64,
    pub index_store_range_scan_calls: u64,
    pub index_store_entry_reads: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

///
/// QueryExecutePhaseAttribution
///
/// QueryExecutePhaseAttribution is the private per-execution measurement
/// bundle used while the diagnostics query path builds the public attribution
/// DTO. It keeps executor phase counters grouped until the final response
/// fields are assembled.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct QueryExecutePhaseAttribution {
    executor_invocation_local_instructions: u64,
    response_finalization_local_instructions: u64,
    runtime_local_instructions: u64,
    finalize_local_instructions: u64,
    load_plan_local_instructions: u64,
    row_layout_local_instructions: u64,
    continuation_signature_local_instructions: u64,
    scalar_runtime_handoff_local_instructions: u64,
    route_plan_local_instructions: u64,
    runtime_prepare_local_instructions: u64,
    direct_data_row: Option<DirectDataRowAttribution>,
    kernel_row: Option<KernelRowAttribution>,
    grouped: Option<GroupedExecutionAttribution>,
}

impl QueryExecutePhaseAttribution {
    pub(super) const fn empty() -> Self {
        Self {
            executor_invocation_local_instructions: 0,
            response_finalization_local_instructions: 0,
            load_plan_local_instructions: 0,
            row_layout_local_instructions: 0,
            continuation_signature_local_instructions: 0,
            scalar_runtime_handoff_local_instructions: 0,
            route_plan_local_instructions: 0,
            runtime_prepare_local_instructions: 0,
            runtime_local_instructions: 0,
            finalize_local_instructions: 0,
            direct_data_row: None,
            kernel_row: None,
            grouped: None,
        }
    }

    pub(super) const fn from_delete(executor_invocation_local_instructions: u64) -> Self {
        Self {
            executor_invocation_local_instructions,
            ..Self::empty()
        }
    }

    pub(super) const fn from_scalar_phase(
        phase: ScalarExecutePhaseAttribution,
        executor_invocation_local_instructions: u64,
    ) -> Self {
        Self {
            executor_invocation_local_instructions,
            response_finalization_local_instructions: 0,
            load_plan_local_instructions: phase.load_plan_local_instructions,
            row_layout_local_instructions: phase.row_layout_local_instructions,
            continuation_signature_local_instructions: phase
                .continuation_signature_local_instructions,
            scalar_runtime_handoff_local_instructions: phase
                .scalar_runtime_handoff_local_instructions,
            route_plan_local_instructions: phase.route_plan_local_instructions,
            runtime_prepare_local_instructions: phase.runtime_prepare_local_instructions,
            runtime_local_instructions: phase.runtime_local_instructions,
            finalize_local_instructions: phase.finalize_local_instructions,
            direct_data_row: Some(DirectDataRowAttribution::from_scalar_phase(phase)),
            kernel_row: KernelRowAttribution::from_scalar_phase(phase),
            grouped: None,
        }
    }

    pub(super) const fn from_grouped_phase(
        phase: GroupedExecutePhaseAttribution,
        executor_invocation_local_instructions: u64,
        response_finalization_local_instructions: u64,
    ) -> Self {
        Self {
            executor_invocation_local_instructions,
            response_finalization_local_instructions,
            load_plan_local_instructions: 0,
            row_layout_local_instructions: 0,
            continuation_signature_local_instructions: 0,
            scalar_runtime_handoff_local_instructions: 0,
            route_plan_local_instructions: 0,
            runtime_prepare_local_instructions: 0,
            runtime_local_instructions: phase
                .stream_local_instructions
                .saturating_add(phase.fold_local_instructions),
            finalize_local_instructions: phase.finalize_local_instructions,
            direct_data_row: None,
            kernel_row: None,
            grouped: Some(GroupedExecutionAttribution::from_executor_phase(phase)),
        }
    }
}

impl FluentTerminalExecutionAttribution {
    pub(in crate::db::session::query) const fn from_common(
        common: QueryAttributionCommon,
        executor_invocation_local_instructions: u64,
        scalar_aggregate: Option<ScalarAggregateAttribution>,
    ) -> Self {
        let execute_local_instructions = executor_invocation_local_instructions;

        Self {
            read_intent: ReadIntentKind::Unspecified,
            compile_local_instructions: common.compile_local_instructions(),
            compile_schema_catalog_local_instructions: common
                .compile_phase_attribution
                .schema_catalog,
            compile_schema_info_local_instructions: common.compile_phase_attribution.schema_info,
            compile_prepare_local_instructions: common.compile_phase_attribution.prepare,
            compile_cache_key_local_instructions: common.compile_phase_attribution.cache_key,
            compile_cache_lookup_local_instructions: common.compile_phase_attribution.cache_lookup,
            compile_plan_build_local_instructions: common.compile_phase_attribution.plan_build,
            compile_cache_insert_local_instructions: common.compile_phase_attribution.cache_insert,
            plan_lookup_local_instructions: common.plan_lookup_local_instructions,
            executor_invocation_local_instructions,
            execute_local_instructions,
            total_local_instructions: common.total_local_instructions(execute_local_instructions),
            store_get_calls: common.store_counters.data_store_get_calls,
            index_store_get_calls: common.store_counters.index_store_get_calls,
            index_store_range_scan_calls: common.store_counters.index_store_range_scan_calls,
            index_store_entry_reads: common.store_counters.index_store_entry_reads,
            scalar_aggregate,
            shared_query_plan_cache_hits: common.cache_attribution.hits,
            shared_query_plan_cache_misses: common.cache_attribution.misses,
        }
    }

    pub(in crate::db) const fn with_read_intent(mut self, read_intent: ReadIntentKind) -> Self {
        self.read_intent = read_intent;
        self
    }
}

impl QueryExecutionAttribution {
    pub(super) const fn from_common(
        common: QueryAttributionCommon,
        execute_phase_attribution: &QueryExecutePhaseAttribution,
        response_decode_local_instructions: u64,
    ) -> Self {
        let execute_local_instructions = execute_phase_attribution
            .executor_invocation_local_instructions
            .saturating_add(execute_phase_attribution.response_finalization_local_instructions);

        Self {
            read_intent: ReadIntentKind::Unspecified,
            compile_local_instructions: common.compile_local_instructions(),
            compile_schema_catalog_local_instructions: common
                .compile_phase_attribution
                .schema_catalog,
            compile_schema_info_local_instructions: common.compile_phase_attribution.schema_info,
            compile_prepare_local_instructions: common.compile_phase_attribution.prepare,
            compile_cache_key_local_instructions: common.compile_phase_attribution.cache_key,
            compile_cache_lookup_local_instructions: common.compile_phase_attribution.cache_lookup,
            compile_plan_build_local_instructions: common.compile_phase_attribution.plan_build,
            compile_cache_insert_local_instructions: common.compile_phase_attribution.cache_insert,
            plan_lookup_local_instructions: common.plan_lookup_local_instructions,
            executor_invocation_local_instructions: execute_phase_attribution
                .executor_invocation_local_instructions,
            response_finalization_local_instructions: execute_phase_attribution
                .response_finalization_local_instructions,
            load_plan_local_instructions: execute_phase_attribution.load_plan_local_instructions,
            row_layout_local_instructions: execute_phase_attribution.row_layout_local_instructions,
            continuation_signature_local_instructions: execute_phase_attribution
                .continuation_signature_local_instructions,
            scalar_runtime_handoff_local_instructions: execute_phase_attribution
                .scalar_runtime_handoff_local_instructions,
            route_plan_local_instructions: execute_phase_attribution.route_plan_local_instructions,
            runtime_prepare_local_instructions: execute_phase_attribution
                .runtime_prepare_local_instructions,
            runtime_local_instructions: execute_phase_attribution.runtime_local_instructions,
            finalize_local_instructions: execute_phase_attribution.finalize_local_instructions,
            direct_data_row: execute_phase_attribution.direct_data_row,
            kernel_row: execute_phase_attribution.kernel_row,
            grouped: execute_phase_attribution.grouped,
            response_decode_local_instructions,
            execute_local_instructions,
            total_local_instructions: common.total_local_instructions(execute_local_instructions),
            store_get_calls: common.store_counters.data_store_get_calls,
            index_store_get_calls: common.store_counters.index_store_get_calls,
            index_store_range_scan_calls: common.store_counters.index_store_range_scan_calls,
            index_store_entry_reads: common.store_counters.index_store_entry_reads,
            shared_query_plan_cache_hits: common.cache_attribution.hits,
            shared_query_plan_cache_misses: common.cache_attribution.misses,
        }
    }

    pub(in crate::db) const fn with_read_intent(mut self, read_intent: ReadIntentKind) -> Self {
        self.read_intent = read_intent;
        self
    }
}
