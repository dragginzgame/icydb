//! Module: session::sql::attribution
//! Responsibility: SQL compile/execute diagnostics and phase-attribution DTOs.
//! Does not own: SQL execution, cache lookup, or response shaping.
//! Boundary: typed attribution payloads shared by session SQL orchestration and execute helpers.

#[cfg(feature = "diagnostics")]
use crate::db::{
    DirectDataRowAttribution, GroupedExecutionAttribution, KernelRowAttribution,
    ScalarAggregateAttribution,
    diagnostics::StoreCounterSnapshot,
    executor::{
        GroupedCountAttribution as ExecutorGroupedCountAttribution, GroupedExecutePhaseAttribution,
        ScalarAggregateTerminalAttribution,
    },
    session::{
        query::QueryPlanCompilePhaseAttribution,
        sql::{
            cache::SqlCacheAttribution, compile::SqlCompilePhaseAttribution,
            projection::SqlProjectionMaterializationMetrics, result::SqlStatementResult,
        },
    },
};
#[cfg(feature = "diagnostics")]
use crate::value::OutputValue;
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

#[cfg(feature = "diagnostics")]
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

#[cfg(feature = "diagnostics")]
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
            scalar_aggregate: SqlScalarAggregateAttribution::from_executor(
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

#[cfg(feature = "diagnostics")]
const fn sql_execute_local_instructions_from_phase(phase: &SqlExecutePhaseAttribution) -> u64 {
    phase
        .planner_local_instructions
        .saturating_add(phase.store_local_instructions)
        .saturating_add(phase.executor_local_instructions)
        .saturating_add(phase.response_finalization_local_instructions)
}

#[cfg(feature = "diagnostics")]
fn sql_output_blob_attribution(result: &SqlStatementResult) -> SqlOutputBlobAttribution {
    let mut attribution = SqlOutputBlobAttribution::default();

    match result {
        SqlStatementResult::Projection { rows, .. } => {
            for row in rows {
                for value in row {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Grouped { rows, .. } => {
            for row in rows {
                for value in row.group_key().iter().chain(row.aggregate_values()) {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities { .. }
        | SqlStatementResult::ShowStores { .. }
        | SqlStatementResult::ShowMemory(_)
        | SqlStatementResult::Ddl(_) => {}
        #[cfg(feature = "sql-explain")]
        SqlStatementResult::Explain(_) => {}
    }

    attribution
}

#[cfg(feature = "diagnostics")]
fn record_output_value_blob_attribution(
    value: &OutputValue,
    attribution: &mut SqlOutputBlobAttribution,
) {
    match value {
        OutputValue::Blob(bytes) => {
            let byte_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            attribution.projected_values = attribution.projected_values.saturating_add(1);
            attribution.projected_bytes = attribution.projected_bytes.saturating_add(byte_len);
            attribution.rendered_hex_bytes = attribution
                .rendered_hex_bytes
                .saturating_add(byte_len.saturating_mul(2).saturating_add(2));
        }
        OutputValue::Enum(value) => {
            if let Some(payload) = value.payload() {
                record_output_value_blob_attribution(payload, attribution);
            }
        }
        OutputValue::List(items) => {
            for item in items {
                record_output_value_blob_attribution(item, attribution);
            }
        }
        OutputValue::Map(entries) => {
            for (key, value) in entries {
                record_output_value_blob_attribution(key, attribution);
                record_output_value_blob_attribution(value, attribution);
            }
        }
        OutputValue::Account(_)
        | OutputValue::Bool(_)
        | OutputValue::Date(_)
        | OutputValue::Decimal(_)
        | OutputValue::Duration(_)
        | OutputValue::Float32(_)
        | OutputValue::Float64(_)
        | OutputValue::Int64(_)
        | OutputValue::Int128(_)
        | OutputValue::IntBig(_)
        | OutputValue::Null
        | OutputValue::Principal(_)
        | OutputValue::Subaccount(_)
        | OutputValue::Text(_)
        | OutputValue::Timestamp(_)
        | OutputValue::Nat64(_)
        | OutputValue::Nat128(_)
        | OutputValue::NatBig(_)
        | OutputValue::Ulid(_)
        | OutputValue::Unit => {}
    }
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

    #[must_use]
    pub(in crate::db) const fn from_query_plan_execute_total_and_store_total(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self::from_execute_total_and_store_total(
            execute_local_instructions,
            store_local_instructions,
        )
        .with_query_plan_compile_attribution(planner_local_instructions, plan_compile_attribution)
    }

    #[must_use]
    pub(in crate::db) const fn from_grouped_select_phase(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        grouped_phase_attribution: GroupedExecutePhaseAttribution,
    ) -> Self {
        let execute_without_response =
            execute_local_instructions.saturating_sub(response_finalization_local_instructions);
        let mut attribution = Self::from_query_plan_execute_total_and_store_total(
            planner_local_instructions,
            plan_compile_attribution,
            execute_without_response,
            store_local_instructions,
        );
        attribution.response_finalization_local_instructions =
            response_finalization_local_instructions;
        attribution.grouped_stream_local_instructions =
            grouped_phase_attribution.stream_local_instructions;
        attribution.grouped_fold_local_instructions =
            grouped_phase_attribution.fold_local_instructions;
        attribution.grouped_finalize_local_instructions =
            grouped_phase_attribution.finalize_local_instructions;
        attribution.grouped_count = grouped_phase_attribution.grouped_count;

        attribution
    }

    #[must_use]
    pub(in crate::db) const fn from_projection_select_phase(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        direct_data_row: Option<DirectDataRowAttribution>,
        kernel_row: Option<KernelRowAttribution>,
    ) -> Self {
        let mut attribution = Self::from_query_plan_execute_total_and_store_total(
            planner_local_instructions,
            plan_compile_attribution,
            execute_local_instructions,
            store_local_instructions,
        );
        attribution.response_finalization_local_instructions =
            response_finalization_local_instructions;
        attribution.direct_data_row = direct_data_row;
        attribution.kernel_row = kernel_row;

        attribution
    }

    #[must_use]
    pub(in crate::db) const fn with_query_plan_compile_attribution(
        mut self,
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
    ) -> Self {
        self.planner_local_instructions = planner_local_instructions;
        self.planner_schema_info_local_instructions = plan_compile_attribution.schema_info;
        self.planner_prepare_local_instructions = plan_compile_attribution.prepare;
        self.planner_cache_key_local_instructions = plan_compile_attribution.cache_key;
        self.planner_cache_lookup_local_instructions = plan_compile_attribution.cache_lookup;
        self.planner_plan_build_local_instructions = plan_compile_attribution.plan_build;
        self.planner_cache_insert_local_instructions = plan_compile_attribution.cache_insert;

        self
    }

    #[must_use]
    pub(in crate::db) const fn with_scalar_aggregate_terminal(
        mut self,
        scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
    ) -> Self {
        self.scalar_aggregate_terminal = scalar_aggregate_terminal;

        self
    }
}
