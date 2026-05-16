//!
//! Dedicated SQL perf-audit canister used only for instruction-sampling and
//! access-shape coverage.
//!

extern crate canic_cdk as ic_cdk;

use candid::CandidType;
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::{
    ErrorKind, ErrorOrigin, QueryErrorKind,
    db::{
        DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
        PersistedRow, QueryExecutionAttribution, SqlCompileAttribution, SqlExecutionAttribution,
        SqlPureCoveringAttribution, SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
        response::QueryResponse, sql::SqlQueryResult,
    },
    prelude::*,
};
use icydb_testing_audit_sql_perf_fixtures::{
    PerfAuditAccount, PerfAuditBlob, PerfAuditCanister, PerfAuditUser,
};

icydb::start!();

// SqlQueryPerfResult
//
// Dedicated audit envelope that preserves the SQL result payload while
// attaching one compile/execute instruction sample for the measured query call
// or one average sample across a same-call loop.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

// FluentQueryPerfOutcome
//
// Dedicated fluent audit summary keeps the canister response stable and small:
// only the response family and row count are needed for perf-baseline checks.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct FluentQueryPerfOutcome {
    result_kind: String,
    entity: String,
    row_count: u32,
}

// FluentQueryPerfResult
//
// Dedicated fluent perf envelope mirrors the SQL audit shape but carries one
// reduced fluent response summary instead of the full query payload.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct FluentQueryPerfResult {
    outcome: FluentQueryPerfOutcome,
    attribution: QueryExecutionAttribution,
}

#[cfg(feature = "sql")]
fn invalid_perf_loop_runs_error() -> icydb::Error {
    icydb::Error::new(
        ErrorKind::Query(QueryErrorKind::Validate),
        ErrorOrigin::Query,
        "sql perf loop requires runs > 0",
    )
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedCountTotals {
    borrowed_hash_computations: u64,
    bucket_candidate_checks: u64,
    existing_group_hits: u64,
    new_group_inserts: u64,
    row_materialization_local_instructions: u64,
    group_lookup_local_instructions: u64,
    existing_group_update_local_instructions: u64,
    new_group_insert_local_instructions: u64,
}

#[cfg(feature = "sql")]
impl GroupedCountTotals {
    const fn record_fluent(&mut self, attribution: &QueryExecutionAttribution) {
        let Some(grouped) = attribution.grouped else {
            return;
        };

        self.borrowed_hash_computations = self
            .borrowed_hash_computations
            .saturating_add(grouped.count.borrowed_hash_computations);
        self.bucket_candidate_checks = self
            .bucket_candidate_checks
            .saturating_add(grouped.count.bucket_candidate_checks);
        self.existing_group_hits = self
            .existing_group_hits
            .saturating_add(grouped.count.existing_group_hits);
        self.new_group_inserts = self
            .new_group_inserts
            .saturating_add(grouped.count.new_group_inserts);
        self.row_materialization_local_instructions = self
            .row_materialization_local_instructions
            .saturating_add(grouped.count.row_materialization_local_instructions);
        self.group_lookup_local_instructions = self
            .group_lookup_local_instructions
            .saturating_add(grouped.count.group_lookup_local_instructions);
        self.existing_group_update_local_instructions = self
            .existing_group_update_local_instructions
            .saturating_add(grouped.count.existing_group_update_local_instructions);
        self.new_group_insert_local_instructions = self
            .new_group_insert_local_instructions
            .saturating_add(grouped.count.new_group_insert_local_instructions);
    }

    const fn record_grouped_count(&mut self, count: GroupedCountAttribution) {
        self.borrowed_hash_computations = self
            .borrowed_hash_computations
            .saturating_add(count.borrowed_hash_computations);
        self.bucket_candidate_checks = self
            .bucket_candidate_checks
            .saturating_add(count.bucket_candidate_checks);
        self.existing_group_hits = self
            .existing_group_hits
            .saturating_add(count.existing_group_hits);
        self.new_group_inserts = self
            .new_group_inserts
            .saturating_add(count.new_group_inserts);
        self.row_materialization_local_instructions = self
            .row_materialization_local_instructions
            .saturating_add(count.row_materialization_local_instructions);
        self.group_lookup_local_instructions = self
            .group_lookup_local_instructions
            .saturating_add(count.group_lookup_local_instructions);
        self.existing_group_update_local_instructions = self
            .existing_group_update_local_instructions
            .saturating_add(count.existing_group_update_local_instructions);
        self.new_group_insert_local_instructions = self
            .new_group_insert_local_instructions
            .saturating_add(count.new_group_insert_local_instructions);
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
#[expect(
    clippy::field_reassign_with_default,
    reason = "perf attribution DTOs intentionally use default-backed assignment so future diagnostics counters do not break audit initializers"
)]
fn average_attribution(
    total_compile_local_instructions: u64,
    total_compile_cache_key_local_instructions: u64,
    total_compile_cache_lookup_local_instructions: u64,
    total_compile_parse_local_instructions: u64,
    total_compile_parse_tokenize_local_instructions: u64,
    total_compile_parse_select_local_instructions: u64,
    total_compile_parse_expr_local_instructions: u64,
    total_compile_parse_predicate_local_instructions: u64,
    total_compile_aggregate_lane_check_local_instructions: u64,
    total_compile_prepare_local_instructions: u64,
    total_compile_lower_local_instructions: u64,
    total_compile_bind_local_instructions: u64,
    total_compile_cache_insert_local_instructions: u64,
    total_plan_lookup_local_instructions: u64,
    total_planner_local_instructions: u64,
    total_store_local_instructions: u64,
    total_executor_invocation_local_instructions: u64,
    total_executor_local_instructions: u64,
    total_response_finalization_local_instructions: u64,
    total_pure_covering_decode_local_instructions: u64,
    total_pure_covering_row_assembly_local_instructions: u64,
    total_grouped_stream_local_instructions: u64,
    total_grouped_fold_local_instructions: u64,
    total_grouped_finalize_local_instructions: u64,
    total_grouped_count_borrowed_hash_computations: u64,
    total_grouped_count_bucket_candidate_checks: u64,
    total_grouped_count_existing_group_hits: u64,
    total_grouped_count_new_group_inserts: u64,
    total_grouped_count_row_materialization_local_instructions: u64,
    total_grouped_count_group_lookup_local_instructions: u64,
    total_grouped_count_existing_group_update_local_instructions: u64,
    total_grouped_count_new_group_insert_local_instructions: u64,
    total_store_get_calls: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_sql_compiled_command_cache_hits: u64,
    total_sql_compiled_command_cache_misses: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    saw_pure_covering: bool,
    saw_grouped: bool,
    runs: u32,
) -> SqlQueryExecutionAttribution {
    let divisor = u64::from(runs);

    let mut attribution = SqlQueryExecutionAttribution::default();
    attribution.compile_local_instructions = total_compile_local_instructions / divisor;
    attribution.compile = SqlCompileAttribution {
        cache_key_local_instructions: total_compile_cache_key_local_instructions / divisor,
        cache_lookup_local_instructions: total_compile_cache_lookup_local_instructions / divisor,
        parse_local_instructions: total_compile_parse_local_instructions / divisor,
        parse_tokenize_local_instructions: total_compile_parse_tokenize_local_instructions
            / divisor,
        parse_select_local_instructions: total_compile_parse_select_local_instructions / divisor,
        parse_expr_local_instructions: total_compile_parse_expr_local_instructions / divisor,
        parse_predicate_local_instructions: total_compile_parse_predicate_local_instructions
            / divisor,
        aggregate_lane_check_local_instructions:
            total_compile_aggregate_lane_check_local_instructions / divisor,
        prepare_local_instructions: total_compile_prepare_local_instructions / divisor,
        lower_local_instructions: total_compile_lower_local_instructions / divisor,
        bind_local_instructions: total_compile_bind_local_instructions / divisor,
        cache_insert_local_instructions: total_compile_cache_insert_local_instructions / divisor,
    };
    attribution.plan_lookup_local_instructions = total_plan_lookup_local_instructions / divisor;
    attribution.execution = SqlExecutionAttribution {
        planner_local_instructions: total_planner_local_instructions / divisor,
        store_local_instructions: total_store_local_instructions / divisor,
        executor_invocation_local_instructions: total_executor_invocation_local_instructions
            / divisor,
        executor_local_instructions: total_executor_local_instructions / divisor,
        response_finalization_local_instructions: total_response_finalization_local_instructions
            / divisor,
    };
    if saw_pure_covering {
        attribution.pure_covering = Some(SqlPureCoveringAttribution {
            decode_local_instructions: total_pure_covering_decode_local_instructions / divisor,
            row_assembly_local_instructions: total_pure_covering_row_assembly_local_instructions
                / divisor,
        });
    }
    if saw_grouped {
        attribution.grouped = Some(GroupedExecutionAttribution {
            stream_local_instructions: total_grouped_stream_local_instructions / divisor,
            fold_local_instructions: total_grouped_fold_local_instructions / divisor,
            finalize_local_instructions: total_grouped_finalize_local_instructions / divisor,
            count: GroupedCountAttribution {
                borrowed_hash_computations: total_grouped_count_borrowed_hash_computations
                    / divisor,
                bucket_candidate_checks: total_grouped_count_bucket_candidate_checks / divisor,
                existing_group_hits: total_grouped_count_existing_group_hits / divisor,
                new_group_inserts: total_grouped_count_new_group_inserts / divisor,
                row_materialization_local_instructions:
                    total_grouped_count_row_materialization_local_instructions / divisor,
                group_lookup_local_instructions: total_grouped_count_group_lookup_local_instructions
                    / divisor,
                existing_group_update_local_instructions:
                    total_grouped_count_existing_group_update_local_instructions / divisor,
                new_group_insert_local_instructions:
                    total_grouped_count_new_group_insert_local_instructions / divisor,
            },
        });
    }
    attribution.store_get_calls = total_store_get_calls / divisor;
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.cache = SqlQueryCacheAttribution {
        sql_compiled_command_hits: total_sql_compiled_command_cache_hits,
        sql_compiled_command_misses: total_sql_compiled_command_cache_misses,
        shared_query_plan_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_misses: total_shared_query_plan_cache_misses,
    };

    attribution
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
#[expect(
    clippy::field_reassign_with_default,
    reason = "perf attribution DTOs intentionally use default-backed assignment so future diagnostics counters do not break audit initializers"
)]
fn average_fluent_attribution(
    total_compile_local_instructions: u64,
    total_plan_lookup_local_instructions: u64,
    total_executor_invocation_local_instructions: u64,
    total_response_finalization_local_instructions: u64,
    total_runtime_local_instructions: u64,
    total_finalize_local_instructions: u64,
    total_direct_data_row_scan_local_instructions: u64,
    total_direct_data_row_key_stream_local_instructions: u64,
    total_direct_data_row_row_read_local_instructions: u64,
    total_direct_data_row_key_encode_local_instructions: u64,
    total_direct_data_row_store_get_local_instructions: u64,
    total_direct_data_row_order_window_local_instructions: u64,
    total_direct_data_row_page_window_local_instructions: u64,
    total_grouped_stream_local_instructions: u64,
    total_grouped_fold_local_instructions: u64,
    total_grouped_finalize_local_instructions: u64,
    total_grouped_count_borrowed_hash_computations: u64,
    total_grouped_count_bucket_candidate_checks: u64,
    total_grouped_count_existing_group_hits: u64,
    total_grouped_count_new_group_inserts: u64,
    total_grouped_count_row_materialization_local_instructions: u64,
    total_grouped_count_group_lookup_local_instructions: u64,
    total_grouped_count_existing_group_update_local_instructions: u64,
    total_grouped_count_new_group_insert_local_instructions: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    saw_direct_data_row: bool,
    saw_grouped: bool,
    runs: u32,
) -> QueryExecutionAttribution {
    let divisor = u64::from(runs);

    let mut attribution = QueryExecutionAttribution::default();
    attribution.compile_local_instructions = total_compile_local_instructions / divisor;
    attribution.plan_lookup_local_instructions = total_plan_lookup_local_instructions / divisor;
    attribution.executor_invocation_local_instructions =
        total_executor_invocation_local_instructions / divisor;
    attribution.response_finalization_local_instructions =
        total_response_finalization_local_instructions / divisor;
    attribution.runtime_local_instructions = total_runtime_local_instructions / divisor;
    attribution.finalize_local_instructions = total_finalize_local_instructions / divisor;
    if saw_direct_data_row {
        attribution.direct_data_row = Some(DirectDataRowAttribution {
            scan_local_instructions: total_direct_data_row_scan_local_instructions / divisor,
            key_stream_local_instructions: total_direct_data_row_key_stream_local_instructions
                / divisor,
            row_read_local_instructions: total_direct_data_row_row_read_local_instructions
                / divisor,
            key_encode_local_instructions: total_direct_data_row_key_encode_local_instructions
                / divisor,
            store_get_local_instructions: total_direct_data_row_store_get_local_instructions
                / divisor,
            order_window_local_instructions: total_direct_data_row_order_window_local_instructions
                / divisor,
            page_window_local_instructions: total_direct_data_row_page_window_local_instructions
                / divisor,
        });
    }
    if saw_grouped {
        attribution.grouped = Some(GroupedExecutionAttribution {
            stream_local_instructions: total_grouped_stream_local_instructions / divisor,
            fold_local_instructions: total_grouped_fold_local_instructions / divisor,
            finalize_local_instructions: total_grouped_finalize_local_instructions / divisor,
            count: GroupedCountAttribution {
                borrowed_hash_computations: total_grouped_count_borrowed_hash_computations
                    / divisor,
                bucket_candidate_checks: total_grouped_count_bucket_candidate_checks / divisor,
                existing_group_hits: total_grouped_count_existing_group_hits / divisor,
                new_group_inserts: total_grouped_count_new_group_inserts / divisor,
                row_materialization_local_instructions:
                    total_grouped_count_row_materialization_local_instructions / divisor,
                group_lookup_local_instructions: total_grouped_count_group_lookup_local_instructions
                    / divisor,
                existing_group_update_local_instructions:
                    total_grouped_count_existing_group_update_local_instructions / divisor,
                new_group_insert_local_instructions:
                    total_grouped_count_new_group_insert_local_instructions / divisor,
            },
        });
    }
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.shared_query_plan_cache_hits = total_shared_query_plan_cache_hits;
    attribution.shared_query_plan_cache_misses = total_shared_query_plan_cache_misses;

    attribution
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_entity_with_perf_loop<E>(sql: &str, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error>
where
    E: icydb::db::PersistedRow<Canister = PerfAuditCanister> + icydb::traits::EntityValue,
{
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = db();
    let mut first_result = None;
    let mut total_compile_local_instructions = 0_u64;
    let mut total_compile_cache_key_local_instructions = 0_u64;
    let mut total_compile_cache_lookup_local_instructions = 0_u64;
    let mut total_compile_parse_local_instructions = 0_u64;
    let mut total_compile_parse_tokenize_local_instructions = 0_u64;
    let mut total_compile_parse_select_local_instructions = 0_u64;
    let mut total_compile_parse_expr_local_instructions = 0_u64;
    let mut total_compile_parse_predicate_local_instructions = 0_u64;
    let mut total_compile_aggregate_lane_check_local_instructions = 0_u64;
    let mut total_compile_prepare_local_instructions = 0_u64;
    let mut total_compile_lower_local_instructions = 0_u64;
    let mut total_compile_bind_local_instructions = 0_u64;
    let mut total_compile_cache_insert_local_instructions = 0_u64;
    let mut total_plan_lookup_local_instructions = 0_u64;
    let mut total_planner_local_instructions = 0_u64;
    let mut total_store_local_instructions = 0_u64;
    let mut total_executor_invocation_local_instructions = 0_u64;
    let mut total_executor_local_instructions = 0_u64;
    let mut total_response_finalization_local_instructions = 0_u64;
    let mut total_pure_covering_decode_local_instructions = 0_u64;
    let mut total_pure_covering_row_assembly_local_instructions = 0_u64;
    let mut total_grouped_stream_local_instructions = 0_u64;
    let mut total_grouped_fold_local_instructions = 0_u64;
    let mut total_grouped_finalize_local_instructions = 0_u64;
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_store_get_calls = 0_u64;
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_sql_compiled_command_cache_hits = 0_u64;
    let mut total_sql_compiled_command_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut saw_pure_covering = false;
    let mut saw_grouped = false;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_sql_query_with_attribution::<E>(sql)?;
        if first_result.is_none() {
            first_result = Some(result);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_compile_cache_key_local_instructions = total_compile_cache_key_local_instructions
            .saturating_add(attribution.compile.cache_key_local_instructions);
        total_compile_cache_lookup_local_instructions =
            total_compile_cache_lookup_local_instructions
                .saturating_add(attribution.compile.cache_lookup_local_instructions);
        total_compile_parse_local_instructions = total_compile_parse_local_instructions
            .saturating_add(attribution.compile.parse_local_instructions);
        total_compile_parse_tokenize_local_instructions =
            total_compile_parse_tokenize_local_instructions
                .saturating_add(attribution.compile.parse_tokenize_local_instructions);
        total_compile_parse_select_local_instructions =
            total_compile_parse_select_local_instructions
                .saturating_add(attribution.compile.parse_select_local_instructions);
        total_compile_parse_expr_local_instructions = total_compile_parse_expr_local_instructions
            .saturating_add(attribution.compile.parse_expr_local_instructions);
        total_compile_parse_predicate_local_instructions =
            total_compile_parse_predicate_local_instructions
                .saturating_add(attribution.compile.parse_predicate_local_instructions);
        total_compile_aggregate_lane_check_local_instructions =
            total_compile_aggregate_lane_check_local_instructions
                .saturating_add(attribution.compile.aggregate_lane_check_local_instructions);
        total_compile_prepare_local_instructions = total_compile_prepare_local_instructions
            .saturating_add(attribution.compile.prepare_local_instructions);
        total_compile_lower_local_instructions = total_compile_lower_local_instructions
            .saturating_add(attribution.compile.lower_local_instructions);
        total_compile_bind_local_instructions = total_compile_bind_local_instructions
            .saturating_add(attribution.compile.bind_local_instructions);
        total_compile_cache_insert_local_instructions =
            total_compile_cache_insert_local_instructions
                .saturating_add(attribution.compile.cache_insert_local_instructions);
        total_plan_lookup_local_instructions = total_plan_lookup_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions);
        total_planner_local_instructions = total_planner_local_instructions
            .saturating_add(attribution.execution.planner_local_instructions);
        total_store_local_instructions = total_store_local_instructions
            .saturating_add(attribution.execution.store_local_instructions);
        total_executor_invocation_local_instructions = total_executor_invocation_local_instructions
            .saturating_add(attribution.execution.executor_invocation_local_instructions);
        total_executor_local_instructions = total_executor_local_instructions
            .saturating_add(attribution.execution.executor_local_instructions);
        total_response_finalization_local_instructions =
            total_response_finalization_local_instructions.saturating_add(
                attribution
                    .execution
                    .response_finalization_local_instructions,
            );
        if let Some(pure_covering) = attribution.pure_covering {
            saw_pure_covering = true;
            total_pure_covering_decode_local_instructions =
                total_pure_covering_decode_local_instructions
                    .saturating_add(pure_covering.decode_local_instructions);
            total_pure_covering_row_assembly_local_instructions =
                total_pure_covering_row_assembly_local_instructions
                    .saturating_add(pure_covering.row_assembly_local_instructions);
        }
        if let Some(grouped) = attribution.grouped {
            saw_grouped = true;
            total_grouped_stream_local_instructions = total_grouped_stream_local_instructions
                .saturating_add(grouped.stream_local_instructions);
            total_grouped_fold_local_instructions = total_grouped_fold_local_instructions
                .saturating_add(grouped.fold_local_instructions);
            total_grouped_finalize_local_instructions = total_grouped_finalize_local_instructions
                .saturating_add(grouped.finalize_local_instructions);
            grouped_count_totals.record_grouped_count(grouped.count);
        }
        total_store_get_calls = total_store_get_calls.saturating_add(attribution.store_get_calls);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_sql_compiled_command_cache_hits = total_sql_compiled_command_cache_hits
            .saturating_add(attribution.cache.sql_compiled_command_hits);
        total_sql_compiled_command_cache_misses = total_sql_compiled_command_cache_misses
            .saturating_add(attribution.cache.sql_compiled_command_misses);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.cache.shared_query_plan_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.cache.shared_query_plan_misses);
    }

    Ok(SqlQueryPerfResult {
        result: first_result.expect("perf loop with runs > 0 should record one result"),
        attribution: average_attribution(
            total_compile_local_instructions,
            total_compile_cache_key_local_instructions,
            total_compile_cache_lookup_local_instructions,
            total_compile_parse_local_instructions,
            total_compile_parse_tokenize_local_instructions,
            total_compile_parse_select_local_instructions,
            total_compile_parse_expr_local_instructions,
            total_compile_parse_predicate_local_instructions,
            total_compile_aggregate_lane_check_local_instructions,
            total_compile_prepare_local_instructions,
            total_compile_lower_local_instructions,
            total_compile_bind_local_instructions,
            total_compile_cache_insert_local_instructions,
            total_plan_lookup_local_instructions,
            total_planner_local_instructions,
            total_store_local_instructions,
            total_executor_invocation_local_instructions,
            total_executor_local_instructions,
            total_response_finalization_local_instructions,
            total_pure_covering_decode_local_instructions,
            total_pure_covering_row_assembly_local_instructions,
            total_grouped_stream_local_instructions,
            total_grouped_fold_local_instructions,
            total_grouped_finalize_local_instructions,
            grouped_count_totals.borrowed_hash_computations,
            grouped_count_totals.bucket_candidate_checks,
            grouped_count_totals.existing_group_hits,
            grouped_count_totals.new_group_inserts,
            grouped_count_totals.row_materialization_local_instructions,
            grouped_count_totals.group_lookup_local_instructions,
            grouped_count_totals.existing_group_update_local_instructions,
            grouped_count_totals.new_group_insert_local_instructions,
            total_store_get_calls,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_sql_compiled_command_cache_hits,
            total_sql_compiled_command_cache_misses,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            saw_pure_covering,
            saw_grouped,
            runs,
        ),
    })
}

#[cfg(feature = "sql")]
fn summarize_fluent_outcome<E>(result: &QueryResponse<E>) -> FluentQueryPerfOutcome
where
    E: PersistedRow<Canister = PerfAuditCanister> + icydb::traits::EntityValue,
{
    match result {
        QueryResponse::Rows(rows) => FluentQueryPerfOutcome {
            result_kind: "rows".to_string(),
            entity: E::MODEL.name().to_string(),
            row_count: rows.count(),
        },
        QueryResponse::Grouped(grouped) => FluentQueryPerfOutcome {
            result_kind: "grouped".to_string(),
            entity: E::MODEL.name().to_string(),
            row_count: u32::try_from(grouped.items().len()).unwrap_or(u32::MAX),
        },
    }
}

#[cfg(feature = "sql")]
fn run_user_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "user.id.order_only.asc.limit2" => {
            let query = session.load::<PerfAuditUser>().order_asc("id").limit(2);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("age")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit2.parity" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("age")
                .order_asc("id")
                .limit(2);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.active_true.order_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("active").eq(true))
                .order_asc("age")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_compare.age_eq_age_nat.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("age").eq_field("age_nat"))
                .order_asc("age")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_between.rank_age_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("rank").between_fields("age", "age"))
                .order_asc("age")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.rank.in_list.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("rank").in_list([17_i32, 28_i32, 30_i32]))
                .order_asc("age")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.grouped.age_count.limit10" => {
            let query = session
                .load::<PerfAuditUser>()
                .group_by("age")?
                .aggregate(count())
                .order_asc("age")
                .limit(10);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(icydb::Error::new(
            ErrorKind::Query(QueryErrorKind::Validate),
            ErrorOrigin::Query,
            format!("unknown fluent user perf scenario: {scenario}"),
        )),
    }
}

#[cfg(feature = "sql")]
fn run_account_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "account.active_true.order_handle.asc.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter(FieldRef::new("active").eq(true))
                .order_asc("handle")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.gold_active.order_handle.asc.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter(FilterExpr::and(vec![
                    FieldRef::new("active").eq(true),
                    FieldRef::new("tier").eq("gold"),
                ]))
                .order_asc("handle")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.score_gte_75.order_score.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter(FieldRef::new("score").gte(75_u64))
                .order_asc("score")
                .order_asc("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(icydb::Error::new(
            ErrorKind::Query(QueryErrorKind::Validate),
            ErrorOrigin::Query,
            format!("unknown fluent account perf scenario: {scenario}"),
        )),
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_fluent_scenario_loop(
    surface: &str,
    scenario: &str,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = db();
    let mut first_outcome = None;
    let mut total_compile_local_instructions = 0_u64;
    let mut total_plan_lookup_local_instructions = 0_u64;
    let mut total_executor_invocation_local_instructions = 0_u64;
    let mut total_response_finalization_local_instructions = 0_u64;
    let mut total_runtime_local_instructions = 0_u64;
    let mut total_finalize_local_instructions = 0_u64;
    let mut total_direct_data_row_scan_local_instructions = 0_u64;
    let mut total_direct_data_row_key_stream_local_instructions = 0_u64;
    let mut total_direct_data_row_row_read_local_instructions = 0_u64;
    let mut total_direct_data_row_key_encode_local_instructions = 0_u64;
    let mut total_direct_data_row_store_get_local_instructions = 0_u64;
    let mut total_direct_data_row_order_window_local_instructions = 0_u64;
    let mut total_direct_data_row_page_window_local_instructions = 0_u64;
    let mut total_grouped_stream_local_instructions = 0_u64;
    let mut total_grouped_fold_local_instructions = 0_u64;
    let mut total_grouped_finalize_local_instructions = 0_u64;
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut saw_direct_data_row = false;
    let mut saw_grouped = false;

    for _ in 0..runs {
        let (outcome, attribution) = match surface {
            "user" => run_user_fluent_scenario_once(&session, scenario)?,
            "account" => run_account_fluent_scenario_once(&session, scenario)?,
            _ => {
                return Err(icydb::Error::new(
                    ErrorKind::Query(QueryErrorKind::Validate),
                    ErrorOrigin::Query,
                    format!("unknown fluent perf surface: {surface}"),
                ));
            }
        };

        if first_outcome.is_none() {
            first_outcome = Some(outcome);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_plan_lookup_local_instructions = total_plan_lookup_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions);
        total_executor_invocation_local_instructions = total_executor_invocation_local_instructions
            .saturating_add(attribution.executor_invocation_local_instructions);
        total_response_finalization_local_instructions =
            total_response_finalization_local_instructions
                .saturating_add(attribution.response_finalization_local_instructions);
        total_runtime_local_instructions =
            total_runtime_local_instructions.saturating_add(attribution.runtime_local_instructions);
        total_finalize_local_instructions = total_finalize_local_instructions
            .saturating_add(attribution.finalize_local_instructions);
        if let Some(direct_data_row) = attribution.direct_data_row {
            saw_direct_data_row = true;
            total_direct_data_row_scan_local_instructions =
                total_direct_data_row_scan_local_instructions
                    .saturating_add(direct_data_row.scan_local_instructions);
            total_direct_data_row_key_stream_local_instructions =
                total_direct_data_row_key_stream_local_instructions
                    .saturating_add(direct_data_row.key_stream_local_instructions);
            total_direct_data_row_row_read_local_instructions =
                total_direct_data_row_row_read_local_instructions
                    .saturating_add(direct_data_row.row_read_local_instructions);
            total_direct_data_row_key_encode_local_instructions =
                total_direct_data_row_key_encode_local_instructions
                    .saturating_add(direct_data_row.key_encode_local_instructions);
            total_direct_data_row_store_get_local_instructions =
                total_direct_data_row_store_get_local_instructions
                    .saturating_add(direct_data_row.store_get_local_instructions);
            total_direct_data_row_order_window_local_instructions =
                total_direct_data_row_order_window_local_instructions
                    .saturating_add(direct_data_row.order_window_local_instructions);
            total_direct_data_row_page_window_local_instructions =
                total_direct_data_row_page_window_local_instructions
                    .saturating_add(direct_data_row.page_window_local_instructions);
        }
        if let Some(grouped) = attribution.grouped {
            saw_grouped = true;
            total_grouped_stream_local_instructions = total_grouped_stream_local_instructions
                .saturating_add(grouped.stream_local_instructions);
            total_grouped_fold_local_instructions = total_grouped_fold_local_instructions
                .saturating_add(grouped.fold_local_instructions);
            total_grouped_finalize_local_instructions = total_grouped_finalize_local_instructions
                .saturating_add(grouped.finalize_local_instructions);
        }
        grouped_count_totals.record_fluent(&attribution);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.shared_query_plan_cache_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.shared_query_plan_cache_misses);
    }

    Ok(FluentQueryPerfResult {
        outcome: first_outcome.expect("perf loop with runs > 0 should record one fluent outcome"),
        attribution: average_fluent_attribution(
            total_compile_local_instructions,
            total_plan_lookup_local_instructions,
            total_executor_invocation_local_instructions,
            total_response_finalization_local_instructions,
            total_runtime_local_instructions,
            total_finalize_local_instructions,
            total_direct_data_row_scan_local_instructions,
            total_direct_data_row_key_stream_local_instructions,
            total_direct_data_row_row_read_local_instructions,
            total_direct_data_row_key_encode_local_instructions,
            total_direct_data_row_store_get_local_instructions,
            total_direct_data_row_order_window_local_instructions,
            total_direct_data_row_page_window_local_instructions,
            total_grouped_stream_local_instructions,
            total_grouped_fold_local_instructions,
            total_grouped_finalize_local_instructions,
            grouped_count_totals.borrowed_hash_computations,
            grouped_count_totals.bucket_candidate_checks,
            grouped_count_totals.existing_group_hits,
            grouped_count_totals.new_group_inserts,
            grouped_count_totals.row_materialization_local_instructions,
            grouped_count_totals.group_lookup_local_instructions,
            grouped_count_totals.existing_group_update_local_instructions,
            grouped_count_totals.new_group_insert_local_instructions,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            saw_direct_data_row,
            saw_grouped,
            runs,
        ),
    })
}

/// Clear all dedicated perf fixture rows from this canister.
#[update]
fn __icydb_fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<PerfAuditAccount>().execute()?;
    db().delete::<PerfAuditBlob>().execute()?;
    db().delete::<PerfAuditUser>().execute()?;

    Ok(())
}

/// Load one deterministic fixture batch tuned for SQL perf audit queries.
#[update]
fn __icydb_fixtures_load() -> Result<(), icydb::Error> {
    __icydb_fixtures_reset()?;
    db().insert_many_atomic(perf_audit_users())?;
    db().insert_many_atomic(perf_audit_blobs())?;
    db().insert_many_atomic(perf_audit_accounts())?;

    Ok(())
}

/// Execute one PerfAuditUser-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_user(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<PerfAuditUser>(sql.as_str())
}

/// Execute one PerfAuditUser-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditUser-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditUser-only SQL query repeatedly inside one canister
/// query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditUser>(sql.as_str(), runs)
}

/// Execute one PerfAuditAccount-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_account(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<PerfAuditAccount>(sql.as_str())
}

/// Execute one PerfAuditAccount-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditAccount>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditAccount-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditAccount>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditAccount-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditAccount>(sql.as_str(), runs)
}

/// Execute one PerfAuditBlob-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_blob(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<PerfAuditBlob>(sql.as_str())
}

/// Execute one PerfAuditBlob-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditBlob>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditBlob-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_blob_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<PerfAuditBlob>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditBlob-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditBlob>(sql.as_str(), runs)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario and attach one
/// local instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario through the update
/// surface so the shared lower query cache can persist for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario repeatedly inside
/// one canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_fluent_loop_with_perf(
    scenario: String,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), runs)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario and attach one
/// local instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario through the
/// update surface so the shared lower query cache can persist for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario repeatedly
/// inside one canister query call and report the per-run average instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_fluent_loop_with_perf(
    scenario: String,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), runs)
}

/// Build the deterministic user fixture batch used by the perf audit.
fn perf_audit_users() -> Vec<PerfAuditUser> {
    vec![
        PerfAuditUser {
            id: 1,
            name: "Alice".to_string(),
            age: 31,
            age_nat: 31,
            rank: 28,
            active: true,
            ..Default::default()
        },
        PerfAuditUser {
            id: 2,
            name: "bob".to_string(),
            age: 24,
            age_nat: 24,
            rank: 25,
            active: true,
            ..Default::default()
        },
        PerfAuditUser {
            id: 3,
            name: "Charlie".to_string(),
            age: 43,
            age_nat: 43,
            rank: 43,
            active: false,
            ..Default::default()
        },
        PerfAuditUser {
            id: 4,
            name: "amber".to_string(),
            age: 27,
            age_nat: 26,
            rank: 29,
            active: true,
            ..Default::default()
        },
        PerfAuditUser {
            id: 5,
            name: "Andrew".to_string(),
            age: 31,
            age_nat: 30,
            rank: 30,
            active: true,
            ..Default::default()
        },
        PerfAuditUser {
            id: 6,
            name: "Zelda".to_string(),
            age: 19,
            age_nat: 19,
            rank: 17,
            active: false,
            ..Default::default()
        },
    ]
}

/// Build one deterministic blob payload for perf fixture rows.
fn perf_blob(seed: u8, len: usize) -> Blob {
    Blob::from(
        (0u8..=250)
            .cycle()
            .take(len)
            .map(|offset| seed.wrapping_add(offset))
            .collect::<Vec<_>>(),
    )
}

/// Build the deterministic blob fixture batch used by SQL perf audit queries.
fn perf_audit_blobs() -> Vec<PerfAuditBlob> {
    vec![
        PerfAuditBlob {
            id: 1,
            label: "avatar-a".to_string(),
            bucket: 10,
            thumbnail: perf_blob(11, 1_024),
            chunk: perf_blob(31, 16_384),
            ..Default::default()
        },
        PerfAuditBlob {
            id: 2,
            label: "avatar-b".to_string(),
            bucket: 10,
            thumbnail: perf_blob(12, 2_048),
            chunk: perf_blob(32, 32_768),
            ..Default::default()
        },
        PerfAuditBlob {
            id: 3,
            label: "avatar-c".to_string(),
            bucket: 10,
            thumbnail: perf_blob(13, 4_096),
            chunk: perf_blob(33, 65_536),
            ..Default::default()
        },
        PerfAuditBlob {
            id: 4,
            label: "archive-a".to_string(),
            bucket: 20,
            thumbnail: perf_blob(14, 1_024),
            chunk: perf_blob(34, 16_384),
            ..Default::default()
        },
        PerfAuditBlob {
            id: 5,
            label: "archive-b".to_string(),
            bucket: 20,
            thumbnail: perf_blob(15, 2_048),
            chunk: perf_blob(35, 32_768),
            ..Default::default()
        },
        PerfAuditBlob {
            id: 6,
            label: "archive-c".to_string(),
            bucket: 30,
            thumbnail: perf_blob(16, 4_096),
            chunk: perf_blob(36, 65_536),
            ..Default::default()
        },
    ]
}

/// Build the deterministic account fixture batch used by the perf audit.
fn perf_audit_accounts() -> Vec<PerfAuditAccount> {
    vec![
        PerfAuditAccount {
            id: 1,
            handle: "Bravo".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 91,
            ..Default::default()
        },
        PerfAuditAccount {
            id: 2,
            handle: "alpha".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 75,
            ..Default::default()
        },
        PerfAuditAccount {
            id: 3,
            handle: "bravo".to_string(),
            tier: "silver".to_string(),
            active: true,
            score: 78,
            ..Default::default()
        },
        PerfAuditAccount {
            id: 4,
            handle: "Delta".to_string(),
            tier: "silver".to_string(),
            active: false,
            score: 66,
            ..Default::default()
        },
        PerfAuditAccount {
            id: 5,
            handle: "brick".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 88,
            ..Default::default()
        },
        PerfAuditAccount {
            id: 6,
            handle: "azure".to_string(),
            tier: "bronze".to_string(),
            active: true,
            score: 63,
            ..Default::default()
        },
    ]
}

canic_cdk::export_candid!();
