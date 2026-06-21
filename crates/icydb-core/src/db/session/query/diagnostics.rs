//! Module: db::session::query::diagnostics
//! Responsibility: diagnostics-only query execution attribution.
//! Does not own: normal execution dispatch, cursor handling, fluent adaptation, or explain surfaces.
//! Boundary: measures the existing execution path and shapes public attribution counters.

use crate::{
    db::{
        DbSession, LoadQueryResult, PersistedRow, Query, QueryError,
        diagnostics::{
            StoreCounterSnapshot, measure_local_instruction_delta as measure_query_stage,
        },
        executor::{
            DirectDataRowPhaseAttribution,
            GroupedCountAttribution as ExecutorGroupedCountAttribution,
            GroupedExecutePhaseAttribution, KernelRowPhaseAttribution,
            ScalarAggregateTerminalAttribution, ScalarExecutePhaseAttribution,
        },
        session::finalize_structural_grouped_projection_result,
        session::query::{PreparedQueryExecutionOutcome, PreparedQueryExecutionOutput},
    },
    traits::{CanisterKind, EntityValue},
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
        let attribution = Self::from_phase_unchecked(phase);

        if attribution.has_work() {
            Some(attribution)
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

    #[cfg(any(test, feature = "sql"))]
    const fn has_work(self) -> bool {
        self.scan_local_instructions != 0
            || self.key_stream_local_instructions != 0
            || self.row_read_local_instructions != 0
            || self.key_encode_local_instructions != 0
            || self.store_get_local_instructions != 0
            || self.order_window_local_instructions != 0
            || self.page_window_local_instructions != 0
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
        })
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn from_captured_phase(
        phase: KernelRowPhaseAttribution,
    ) -> Option<Self> {
        Self::from_kernel_phase(phase)
    }

    const fn from_kernel_phase(phase: KernelRowPhaseAttribution) -> Option<Self> {
        let attribution = Self {
            scan_local_instructions: phase.scan_local_instructions,
            key_stream_local_instructions: phase.key_stream_local_instructions,
            row_read_local_instructions: phase.row_read_local_instructions,
            order_window_local_instructions: phase.order_window_local_instructions,
            page_window_local_instructions: phase.page_window_local_instructions,
            retained_layout_hits: phase.retained_layout_hits,
            retained_slot_values: phase.retained_slot_values,
            retained_octet_length_values: phase.retained_octet_length_values,
        };

        if attribution.has_work() {
            Some(attribution)
        } else {
            None
        }
    }

    const fn has_work(self) -> bool {
        self.scan_local_instructions != 0
            || self.key_stream_local_instructions != 0
            || self.row_read_local_instructions != 0
            || self.order_window_local_instructions != 0
            || self.page_window_local_instructions != 0
            || self.retained_layout_hits != 0
            || self.retained_slot_values != 0
            || self.retained_octet_length_values != 0
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

// GroupedExecutionAttribution
//
// Candid diagnostics payload for grouped execution counters.
// Stream, fold, finalize, and grouped-count metrics stay together so grouped
// execution is no longer spread across top-level query attribution fields.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct GroupedExecutionAttribution {
    pub stream_local_instructions: u64,
    pub fold_local_instructions: u64,
    pub finalize_local_instructions: u64,
    pub count: GroupedCountAttribution,
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
        let has_scalar_aggregate_work = terminal.base_row_local_instructions != 0
            || terminal.reducer_fold_local_instructions != 0
            || terminal.expression_evaluations != 0
            || terminal.filter_evaluations != 0
            || terminal.rows_ingested != 0
            || terminal.terminal_count != 0
            || terminal.unique_input_expr_count != 0
            || terminal.unique_filter_expr_count != 0
            || terminal.sink_mode.label().is_some();
        if !has_scalar_aggregate_work {
            return None;
        }

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

// QueryExecutionAttribution
//
// QueryExecutionAttribution records the top-level compile/execute split for
// typed/fluent query execution at the session boundary.
// Every field is an additive counter where zero means no observed work or no
// observed event for that bucket. Path-specific counters are present only for
// the execution path that produced them.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct QueryExecutionAttribution {
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
struct QueryExecutePhaseAttribution {
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

impl<C: CanisterKind> DbSession<C> {
    const fn empty_query_execute_phase_attribution() -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
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

    const fn scalar_query_execute_phase_attribution(
        phase: ScalarExecutePhaseAttribution,
        executor_invocation_local_instructions: u64,
    ) -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
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

    const fn grouped_query_execute_phase_attribution(
        phase: GroupedExecutePhaseAttribution,
        executor_invocation_local_instructions: u64,
        response_finalization_local_instructions: u64,
    ) -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
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
            grouped: Some(GroupedExecutionAttribution {
                stream_local_instructions: phase.stream_local_instructions,
                fold_local_instructions: phase.fold_local_instructions,
                finalize_local_instructions: phase.finalize_local_instructions,
                count: GroupedCountAttribution::from_executor(phase.grouped_count),
            }),
        }
    }

    /// Execute one typed query while reporting the compile/execute split at
    /// the shared fluent query seam.
    #[doc(hidden)]
    pub fn execute_query_result_with_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure compile work at the typed/fluent boundary,
        // including the shared lower query-plan cache lookup/build exactly
        // once. This preserves honest hit/miss attribution without
        // double-building plans on one-shot cache misses.
        let (plan_lookup_local_instructions, plan_and_cache) = measure_query_stage(|| {
            self.cached_prepared_query_plan_for_entity_with_compile_phase_attribution::<E>(query)
        });
        let (plan, cache_attribution, compile_phase_attribution) = plan_and_cache?;
        let compile_local_instructions = plan_lookup_local_instructions;

        // Phase 2: execute one prepared plan through the shared execution
        // pipeline, preserving the same outer invocation measurement boundary.
        let store_counters_before = StoreCounterSnapshot::capture();
        let (executor_invocation_local_instructions, outcome) = measure_query_stage(|| {
            self.execute_prepared(plan, true, PreparedQueryExecutionOutput::Rows)
        });
        let outcome = outcome?;
        let store_counters = store_counters_before.delta_since();
        let (result, execute_phase_attribution, response_decode_local_instructions) =
            Self::query_execution_attribution_from_outcome(
                outcome,
                executor_invocation_local_instructions,
            )?;
        let execute_local_instructions = execute_phase_attribution
            .executor_invocation_local_instructions
            .saturating_add(execute_phase_attribution.response_finalization_local_instructions);
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            result,
            QueryExecutionAttribution {
                compile_local_instructions,
                compile_schema_catalog_local_instructions: compile_phase_attribution.schema_catalog,
                compile_schema_info_local_instructions: compile_phase_attribution.schema_info,
                compile_prepare_local_instructions: compile_phase_attribution.prepare,
                compile_cache_key_local_instructions: compile_phase_attribution.cache_key,
                compile_cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                compile_plan_build_local_instructions: compile_phase_attribution.plan_build,
                compile_cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                plan_lookup_local_instructions,
                executor_invocation_local_instructions: execute_phase_attribution
                    .executor_invocation_local_instructions,
                response_finalization_local_instructions: execute_phase_attribution
                    .response_finalization_local_instructions,
                load_plan_local_instructions: execute_phase_attribution
                    .load_plan_local_instructions,
                row_layout_local_instructions: execute_phase_attribution
                    .row_layout_local_instructions,
                continuation_signature_local_instructions: execute_phase_attribution
                    .continuation_signature_local_instructions,
                scalar_runtime_handoff_local_instructions: execute_phase_attribution
                    .scalar_runtime_handoff_local_instructions,
                route_plan_local_instructions: execute_phase_attribution
                    .route_plan_local_instructions,
                runtime_prepare_local_instructions: execute_phase_attribution
                    .runtime_prepare_local_instructions,
                runtime_local_instructions: execute_phase_attribution.runtime_local_instructions,
                finalize_local_instructions: execute_phase_attribution.finalize_local_instructions,
                direct_data_row: execute_phase_attribution.direct_data_row,
                kernel_row: execute_phase_attribution.kernel_row,
                grouped: execute_phase_attribution.grouped,
                response_decode_local_instructions,
                execute_local_instructions,
                total_local_instructions,
                store_get_calls: store_counters.data_store_get_calls,
                index_store_get_calls: store_counters.index_store_get_calls,
                index_store_range_scan_calls: store_counters.index_store_range_scan_calls,
                index_store_entry_reads: store_counters.index_store_entry_reads,
                shared_query_plan_cache_hits: cache_attribution.hits,
                shared_query_plan_cache_misses: cache_attribution.misses,
            },
        ))
    }

    // Convert the shared execution outcome into the diagnostics public result
    // and phase-attribution bundle. Grouped response finalization is still
    // measured separately because that counter is part of the public contract.
    fn query_execution_attribution_from_outcome<E>(
        outcome: PreparedQueryExecutionOutcome<E>,
        executor_invocation_local_instructions: u64,
    ) -> Result<(LoadQueryResult<E>, QueryExecutePhaseAttribution, u64), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match outcome {
            PreparedQueryExecutionOutcome::Scalar {
                rows,
                phase: Some(phase_attribution),
                response_decode_local_instructions,
            } => Ok((
                LoadQueryResult::Rows(rows),
                Self::scalar_query_execute_phase_attribution(
                    phase_attribution,
                    executor_invocation_local_instructions,
                ),
                response_decode_local_instructions,
            )),
            PreparedQueryExecutionOutcome::Grouped {
                result,
                trace,
                phase: Some(phase_attribution),
            } => {
                let (response_finalization_local_instructions, grouped) =
                    measure_query_stage(|| {
                        finalize_structural_grouped_projection_result(result, trace)
                    });
                let grouped = grouped?;

                Ok((
                    LoadQueryResult::Grouped(grouped),
                    Self::grouped_query_execute_phase_attribution(
                        phase_attribution,
                        executor_invocation_local_instructions,
                        response_finalization_local_instructions,
                    ),
                    0,
                ))
            }
            PreparedQueryExecutionOutcome::Delete { rows } => Ok((
                LoadQueryResult::Rows(rows),
                QueryExecutePhaseAttribution {
                    executor_invocation_local_instructions,
                    ..Self::empty_query_execute_phase_attribution()
                },
                0,
            )),
            PreparedQueryExecutionOutcome::DeleteCount { .. } => Err(QueryError::invariant()),
            PreparedQueryExecutionOutcome::Scalar { phase: None, .. }
            | PreparedQueryExecutionOutcome::Grouped { phase: None, .. } => {
                Err(QueryError::invariant())
            }
        }
    }
}
