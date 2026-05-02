//! Module: db::session::sql::attribution
//! Responsibility: SQL compile/execute diagnostics and phase-attribution DTOs.
//! Does not own: SQL execution, cache lookup, or response shaping.
//! Boundary: typed attribution payloads shared by session SQL orchestration and execute helpers.

#[cfg(feature = "diagnostics")]
use crate::db::{
    GroupedExecutionAttribution,
    executor::{
        GroupedCountAttribution as ExecutorGroupedCountAttribution,
        ScalarAggregateTerminalAttribution,
    },
};
#[cfg(feature = "diagnostics")]
use candid::CandidType;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;

// SqlCompileAttribution
//
// Candid diagnostics payload for SQL front-end compile counters.
// The short field names are scoped by the `compile` parent field on
// `SqlQueryExecutionAttribution`.
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

// SqlExecutionAttribution
//
// Candid diagnostics payload for the reduced SQL execute phase.
// Planner, store, executor invocation, executor runtime, and response
// finalization counters stay together under the `execution` parent field.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlExecutionAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
}

// SqlScalarAggregateAttribution
//
// Candid diagnostics payload for scalar aggregate terminal execution.
// The field names drop the old `scalar_aggregate_` prefix because the parent
// field now owns that context.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlScalarAggregateAttribution {
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

#[cfg(feature = "diagnostics")]
impl SqlScalarAggregateAttribution {
    pub(in crate::db::session::sql) fn from_executor(
        terminal: ScalarAggregateTerminalAttribution,
    ) -> Option<Self> {
        // Treat the nested payload as absent only when the executor reported
        // no scalar aggregate work at all. This keeps COUNT fast paths compact
        // while preserving any future counter that becomes nonzero.
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

// SqlPureCoveringAttribution
//
// Candid diagnostics payload for pure covering projection counters.
// The value is optional on the top-level SQL attribution because most query
// shapes do not enter this projection path.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlPureCoveringAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

// SqlQueryCacheAttribution
//
// Candid diagnostics payload for SQL compiled-command and shared query-plan
// cache counters observed during one SQL query call.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryCacheAttribution {
    pub sql_compiled_command_hits: u64,
    pub sql_compiled_command_misses: u64,
    pub shared_query_plan_hits: u64,
    pub shared_query_plan_misses: u64,
}

// SqlQueryExecutionAttribution
//
// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
// split at the new compile/execute boundary.
// Every field is an additive counter where zero means no observed work or no
// observed event for that bucket. Path-specific counters are present only for
// the execution path that produced them.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile: SqlCompileAttribution,
    pub plan_lookup_local_instructions: u64,
    pub execution: SqlExecutionAttribution,
    pub grouped: Option<GroupedExecutionAttribution>,
    pub scalar_aggregate: Option<SqlScalarAggregateAttribution>,
    pub pure_covering: Option<SqlPureCoveringAttribution>,
    pub store_get_calls: u64,
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
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: ExecutorGroupedCountAttribution,
    pub scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
}

#[cfg(feature = "diagnostics")]
impl SqlExecutePhaseAttribution {
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
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
        }
    }
}
