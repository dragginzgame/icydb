//! Module: session::sql::attribution
//! Responsibility: SQL compile/execute diagnostics and phase-attribution DTOs.
//! Does not own: SQL execution, cache lookup, or response shaping.
//! Boundary: typed attribution payloads shared by session SQL orchestration and execute helpers.

#[cfg(feature = "diagnostics")]
use crate::db::{
    DirectDataRowAttribution, GroupedExecutionAttribution, KernelRowAttribution,
    ScalarAggregateAttribution,
    executor::{
        GroupedCountAttribution as ExecutorGroupedCountAttribution,
        ScalarAggregateTerminalAttribution,
    },
    session::sql::{
        cache::SqlCacheAttribution, compile::SqlCompilePhaseAttribution,
        projection::SqlProjectionMaterializationMetrics,
    },
};
#[cfg(feature = "diagnostics")]
use candid::CandidType;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;

///
/// SqlCompileAttribution
///
/// Candid diagnostics payload for SQL front-end compile counters.
/// The short field names are scoped by the `compile` parent field on
/// `SqlQueryExecutionAttribution`.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlCompileAttribution {
    pub cache_key_local_instructions: u64,
    pub cache_lookup_local_instructions: u64,
    pub parse_local_instructions: u64,
    pub parse_tokenize_local_instructions: u64,
    pub parse_select_local_instructions: u64,
    pub parse_expr_local_instructions: u64,
    pub parse_predicate_local_instructions: u64,
    pub aggregate_lane_check_local_instructions: u64,
    pub prepare_local_instructions: u64,
    pub lower_local_instructions: u64,
    pub bind_local_instructions: u64,
    pub cache_insert_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
impl SqlCompileAttribution {
    pub(in crate::db::session::sql) const fn from_phase(phase: SqlCompilePhaseAttribution) -> Self {
        Self {
            cache_key_local_instructions: phase.cache_key,
            cache_lookup_local_instructions: phase.cache_lookup,
            parse_local_instructions: phase.parse,
            parse_tokenize_local_instructions: phase.parse_tokenize,
            parse_select_local_instructions: phase.parse_select,
            parse_expr_local_instructions: phase.parse_expr,
            parse_predicate_local_instructions: phase.parse_predicate,
            aggregate_lane_check_local_instructions: phase.aggregate_lane_check,
            prepare_local_instructions: phase.prepare,
            lower_local_instructions: phase.lower,
            bind_local_instructions: phase.bind,
            cache_insert_local_instructions: phase.cache_insert,
        }
    }
}

///
/// SqlExecutionAttribution
///
/// Candid diagnostics payload for the reduced SQL execute phase.
/// Planner, store, executor invocation, executor runtime, and response
/// finalization counters stay together under the `execution` parent field.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlExecutionAttribution {
    pub planner_local_instructions: u64,
    pub planner_schema_info_local_instructions: u64,
    pub planner_prepare_local_instructions: u64,
    pub planner_cache_key_local_instructions: u64,
    pub planner_cache_lookup_local_instructions: u64,
    pub planner_plan_build_local_instructions: u64,
    pub planner_cache_insert_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
impl SqlExecutionAttribution {
    pub(in crate::db::session::sql) const fn from_phase(
        phase: &SqlExecutePhaseAttribution,
    ) -> Self {
        Self {
            planner_local_instructions: phase.planner_local_instructions,
            planner_schema_info_local_instructions: phase.planner_schema_info_local_instructions,
            planner_prepare_local_instructions: phase.planner_prepare_local_instructions,
            planner_cache_key_local_instructions: phase.planner_cache_key_local_instructions,
            planner_cache_lookup_local_instructions: phase.planner_cache_lookup_local_instructions,
            planner_plan_build_local_instructions: phase.planner_plan_build_local_instructions,
            planner_cache_insert_local_instructions: phase.planner_cache_insert_local_instructions,
            store_local_instructions: phase.store_local_instructions,
            executor_invocation_local_instructions: phase.executor_invocation_local_instructions,
            executor_local_instructions: phase.executor_local_instructions,
            response_finalization_local_instructions: phase
                .response_finalization_local_instructions,
        }
    }
}

#[cfg(feature = "diagnostics")]
pub type SqlScalarAggregateAttribution = ScalarAggregateAttribution;

///
/// SqlPureCoveringAttribution
///
/// Candid diagnostics payload for pure covering projection counters.
/// The value is optional on the top-level SQL attribution because most query
/// shapes do not enter this projection path.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlPureCoveringAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
impl SqlPureCoveringAttribution {
    pub(in crate::db::session::sql) const fn from_local_instructions(
        decode_local_instructions: u64,
        row_assembly_local_instructions: u64,
    ) -> Option<Self> {
        if decode_local_instructions == 0 && row_assembly_local_instructions == 0 {
            return None;
        }

        Some(Self {
            decode_local_instructions,
            row_assembly_local_instructions,
        })
    }
}

///
/// SqlHybridCoveringAttribution
///
/// Candid diagnostics payload for hybrid covering projection counters.
/// Hybrid covering reads use index/primary-key values where possible and sparse
/// row reads only for uncovered projected fields.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlHybridCoveringAttribution {
    pub path_hits: u64,
    pub index_field_accesses: u64,
    pub row_field_accesses: u64,
}

#[cfg(feature = "diagnostics")]
impl SqlHybridCoveringAttribution {
    pub(in crate::db::session::sql) const fn from_projection_metrics(
        metrics: SqlProjectionMaterializationMetrics,
    ) -> Option<Self> {
        if metrics.has_hybrid_covering_work() {
            Some(Self {
                path_hits: metrics.hybrid_covering_path_hits,
                index_field_accesses: metrics.hybrid_covering_index_field_accesses,
                row_field_accesses: metrics.hybrid_covering_row_field_accesses,
            })
        } else {
            None
        }
    }
}

///
/// SqlOutputBlobAttribution
///
/// Candid diagnostics payload for SQL projection payload size. Raw bytes count
/// the blob bytes projected into SQL output values; rendered hex bytes count
/// the blob-specific `0x...` text that public SQL row rendering will emit.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlOutputBlobAttribution {
    pub projected_values: u64,
    pub projected_bytes: u64,
    pub rendered_hex_bytes: u64,
}

///
/// SqlQueryCacheAttribution
///
/// Candid diagnostics payload for SQL compiled-command and shared query-plan
/// cache counters observed during one SQL query call.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryCacheAttribution {
    pub sql_compiled_command_hits: u64,
    pub sql_compiled_command_misses: u64,
    pub shared_query_plan_hits: u64,
    pub shared_query_plan_misses: u64,
}

#[cfg(feature = "diagnostics")]
impl SqlQueryCacheAttribution {
    pub(in crate::db::session::sql) const fn from_phases(
        compile: SqlCacheAttribution,
        execute: SqlCacheAttribution,
    ) -> Self {
        let merged = compile.merge(execute);

        Self {
            sql_compiled_command_hits: merged.sql_compiled_command_cache_hits,
            sql_compiled_command_misses: merged.sql_compiled_command_cache_misses,
            shared_query_plan_hits: merged.shared_query_plan_cache_hits,
            shared_query_plan_misses: merged.shared_query_plan_cache_misses,
        }
    }
}

///
/// SqlQueryExecutionAttribution
///
/// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
/// split at the new compile/execute boundary.
/// Every field is an additive counter where zero means no observed work or no
/// observed event for that bucket. Path-specific counters are present only for
/// the execution path that produced them.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile: SqlCompileAttribution,
    pub plan_lookup_local_instructions: u64,
    pub execution: SqlExecutionAttribution,
    pub direct_data_row: Option<DirectDataRowAttribution>,
    pub kernel_row: Option<KernelRowAttribution>,
    pub grouped: Option<GroupedExecutionAttribution>,
    pub scalar_aggregate: Option<SqlScalarAggregateAttribution>,
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

///
/// SqlExecutePhaseAttribution
///
/// SqlExecutePhaseAttribution keeps the execute side split into select-plan
/// work, physical store/index access, and narrower runtime execution.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlExecutePhaseAttribution {
    pub planner_local_instructions: u64,
    pub planner_schema_info_local_instructions: u64,
    pub planner_prepare_local_instructions: u64,
    pub planner_cache_key_local_instructions: u64,
    pub planner_cache_lookup_local_instructions: u64,
    pub planner_plan_build_local_instructions: u64,
    pub planner_cache_insert_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: ExecutorGroupedCountAttribution,
    pub scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
    pub direct_data_row: Option<DirectDataRowAttribution>,
    pub kernel_row: Option<KernelRowAttribution>,
}

#[cfg(feature = "diagnostics")]
impl SqlExecutePhaseAttribution {
    /// Build execute-phase attribution from legacy execute and store totals.
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
            planner_schema_info_local_instructions: 0,
            planner_prepare_local_instructions: 0,
            planner_cache_key_local_instructions: 0,
            planner_cache_lookup_local_instructions: 0,
            planner_plan_build_local_instructions: 0,
            planner_cache_insert_local_instructions: 0,
            store_local_instructions,
            executor_invocation_local_instructions: execute_local_instructions,
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions),
            response_finalization_local_instructions: 0,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: ExecutorGroupedCountAttribution::none(),
            scalar_aggregate_terminal: ScalarAggregateTerminalAttribution::none(),
            direct_data_row: None,
            kernel_row: None,
        }
    }
}
