//! Top-level SQL query execution attribution assembly.
//! Does not own: SQL execution or individual attribution DTO definitions.

use super::{
    SqlCompileAttribution, SqlExecutePhaseAttribution, SqlExecutionAttribution,
    SqlHybridCoveringAttribution, SqlOutputBlobAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, output_blob::sql_output_blob_attribution,
};
use crate::db::{
    DirectDataRowAttribution, GroupedExecutionAttribution, KernelRowAttribution,
    ScalarAggregateAttribution,
    diagnostics::StoreCounterSnapshot,
    session::sql::{
        cache::SqlCacheAttribution, compile::SqlCompilePhaseAttribution,
        projection::SqlProjectionMaterializationMetrics, result::SqlStatementResult,
    },
};
use candid::CandidType;
use serde::Deserialize;

///
/// SqlQueryExecutionAttribution
///
/// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
/// split at the new compile/execute boundary.
/// Every field is an additive counter where zero means no observed work or no
/// observed event for that bucket. Path-specific counters are present only for
/// the execution path that produced them.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile: SqlCompileAttribution,
    pub plan_lookup_local_instructions: u64,
    pub execution: SqlExecutionAttribution,
    pub direct_data_row: Option<DirectDataRowAttribution>,
    pub kernel_row: Option<KernelRowAttribution>,
    pub grouped: Option<GroupedExecutionAttribution>,
    pub scalar_aggregate: Option<ScalarAggregateAttribution>,
    pub pure_covering: Option<SqlPureCoveringAttribution>,
    pub hybrid_covering: Option<SqlHybridCoveringAttribution>,
    pub output_blob: SqlOutputBlobAttribution,
    pub store_get_calls: u64,
    pub index_store_get_calls: u64,
    pub index_store_range_scan_calls: u64,
    pub index_store_entry_reads: u64,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub cache: SqlQueryCacheAttribution,
}

#[derive(Clone, Copy)]
pub(in crate::db::session::sql) struct SqlQueryExecutionAttributionInputs {
    pub compile_local_instructions: u64,
    pub compile_phase_attribution: SqlCompilePhaseAttribution,
    pub compile_cache_attribution: SqlCacheAttribution,
    pub execute_cache_attribution: SqlCacheAttribution,
    pub execute_phase_attribution: SqlExecutePhaseAttribution,
    pub pure_covering_decode_local_instructions: u64,
    pub pure_covering_row_assembly_local_instructions: u64,
    pub projection_materialization: SqlProjectionMaterializationMetrics,
    pub store_counters: StoreCounterSnapshot,
}

impl SqlQueryExecutionAttribution {
    pub(in crate::db::session::sql) fn from_inputs(
        result: &SqlStatementResult,
        inputs: &SqlQueryExecutionAttributionInputs,
    ) -> Self {
        let execute_phase = &inputs.execute_phase_attribution;
        let execute_local_instructions = sql_execute_local_instructions_from_phase(execute_phase);
        let total_local_instructions = inputs
            .compile_local_instructions
            .saturating_add(execute_local_instructions);
        let grouped = matches!(result, SqlStatementResult::Grouped { .. }).then_some(
            GroupedExecutionAttribution::from_executor_parts(
                execute_phase.grouped_stream_local_instructions,
                execute_phase.grouped_fold_local_instructions,
                execute_phase.grouped_finalize_local_instructions,
                execute_phase.grouped_count,
            ),
        );

        Self {
            compile_local_instructions: inputs.compile_local_instructions,
            compile: SqlCompileAttribution::from_phase(inputs.compile_phase_attribution),
            plan_lookup_local_instructions: execute_phase.planner_local_instructions,
            execution: SqlExecutionAttribution::from_phase(execute_phase),
            direct_data_row: execute_phase.direct_data_row,
            kernel_row: execute_phase.kernel_row,
            grouped,
            scalar_aggregate: ScalarAggregateAttribution::from_executor(
                execute_phase.scalar_aggregate_terminal,
            ),
            pure_covering: SqlPureCoveringAttribution::from_local_instructions(
                inputs.pure_covering_decode_local_instructions,
                inputs.pure_covering_row_assembly_local_instructions,
            ),
            hybrid_covering: SqlHybridCoveringAttribution::from_projection_metrics(
                inputs.projection_materialization,
            ),
            output_blob: sql_output_blob_attribution(result),
            store_get_calls: inputs.store_counters.data_store_get_calls,
            index_store_get_calls: inputs.store_counters.index_store_get_calls,
            index_store_range_scan_calls: inputs.store_counters.index_store_range_scan_calls,
            index_store_entry_reads: inputs.store_counters.index_store_entry_reads,
            response_decode_local_instructions: 0,
            execute_local_instructions,
            total_local_instructions,
            cache: SqlQueryCacheAttribution::from_phases(
                inputs.compile_cache_attribution,
                inputs.execute_cache_attribution,
            ),
        }
    }
}

const fn sql_execute_local_instructions_from_phase(phase: &SqlExecutePhaseAttribution) -> u64 {
    phase
        .planner_local_instructions
        .saturating_add(phase.store_local_instructions)
        .saturating_add(phase.executor_local_instructions)
        .saturating_add(phase.response_finalization_local_instructions)
}
