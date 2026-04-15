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
    db::{
        PersistedRow, QueryExecutionAttribution, SqlQueryExecutionAttribution,
        response::QueryResponse, sql::SqlQueryResult,
    },
    error::{ErrorKind, ErrorOrigin, QueryErrorKind},
    prelude::{FieldRef, Predicate, count},
};
use icydb_testing_audit_sql_perf_fixtures::{PerfAuditAccount, PerfAuditCanister, PerfAuditUser};

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
#[expect(clippy::too_many_arguments)]
fn average_attribution(
    total_compile_local_instructions: u64,
    total_planner_local_instructions: u64,
    total_executor_local_instructions: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_sql_compiled_command_cache_hits: u64,
    total_sql_compiled_command_cache_misses: u64,
    total_sql_select_plan_cache_hits: u64,
    total_sql_select_plan_cache_misses: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    runs: u32,
) -> SqlQueryExecutionAttribution {
    let divisor = u64::from(runs);

    SqlQueryExecutionAttribution {
        compile_local_instructions: total_compile_local_instructions / divisor,
        planner_local_instructions: total_planner_local_instructions / divisor,
        executor_local_instructions: total_executor_local_instructions / divisor,
        response_decode_local_instructions: total_response_decode_local_instructions / divisor,
        execute_local_instructions: total_execute_local_instructions / divisor,
        total_local_instructions: total_local_instructions / divisor,
        sql_compiled_command_cache_hits: total_sql_compiled_command_cache_hits,
        sql_compiled_command_cache_misses: total_sql_compiled_command_cache_misses,
        sql_select_plan_cache_hits: total_sql_select_plan_cache_hits,
        sql_select_plan_cache_misses: total_sql_select_plan_cache_misses,
        shared_query_plan_cache_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_cache_misses: total_shared_query_plan_cache_misses,
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
fn average_fluent_attribution(
    total_compile_local_instructions: u64,
    total_runtime_local_instructions: u64,
    total_finalize_local_instructions: u64,
    total_direct_data_row_scan_local_instructions: u64,
    total_direct_data_row_key_stream_local_instructions: u64,
    total_direct_data_row_row_read_local_instructions: u64,
    total_direct_data_row_key_encode_local_instructions: u64,
    total_direct_data_row_store_get_local_instructions: u64,
    total_direct_data_row_order_window_local_instructions: u64,
    total_direct_data_row_page_window_local_instructions: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    runs: u32,
) -> QueryExecutionAttribution {
    let divisor = u64::from(runs);

    QueryExecutionAttribution {
        compile_local_instructions: total_compile_local_instructions / divisor,
        runtime_local_instructions: total_runtime_local_instructions / divisor,
        finalize_local_instructions: total_finalize_local_instructions / divisor,
        direct_data_row_scan_local_instructions: total_direct_data_row_scan_local_instructions
            / divisor,
        direct_data_row_key_stream_local_instructions:
            total_direct_data_row_key_stream_local_instructions / divisor,
        direct_data_row_row_read_local_instructions:
            total_direct_data_row_row_read_local_instructions / divisor,
        direct_data_row_key_encode_local_instructions:
            total_direct_data_row_key_encode_local_instructions / divisor,
        direct_data_row_store_get_local_instructions:
            total_direct_data_row_store_get_local_instructions / divisor,
        direct_data_row_order_window_local_instructions:
            total_direct_data_row_order_window_local_instructions / divisor,
        direct_data_row_page_window_local_instructions:
            total_direct_data_row_page_window_local_instructions / divisor,
        response_decode_local_instructions: total_response_decode_local_instructions / divisor,
        execute_local_instructions: total_execute_local_instructions / divisor,
        total_local_instructions: total_local_instructions / divisor,
        shared_query_plan_cache_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_cache_misses: total_shared_query_plan_cache_misses,
    }
}

#[cfg(feature = "sql")]
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
    let mut total_planner_local_instructions = 0_u64;
    let mut total_executor_local_instructions = 0_u64;
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_sql_compiled_command_cache_hits = 0_u64;
    let mut total_sql_compiled_command_cache_misses = 0_u64;
    let mut total_sql_select_plan_cache_hits = 0_u64;
    let mut total_sql_select_plan_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_sql_query_with_attribution::<E>(sql)?;
        if first_result.is_none() {
            first_result = Some(result);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_planner_local_instructions =
            total_planner_local_instructions.saturating_add(attribution.planner_local_instructions);
        total_executor_local_instructions = total_executor_local_instructions
            .saturating_add(attribution.executor_local_instructions);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_sql_compiled_command_cache_hits = total_sql_compiled_command_cache_hits
            .saturating_add(attribution.sql_compiled_command_cache_hits);
        total_sql_compiled_command_cache_misses = total_sql_compiled_command_cache_misses
            .saturating_add(attribution.sql_compiled_command_cache_misses);
        total_sql_select_plan_cache_hits =
            total_sql_select_plan_cache_hits.saturating_add(attribution.sql_select_plan_cache_hits);
        total_sql_select_plan_cache_misses = total_sql_select_plan_cache_misses
            .saturating_add(attribution.sql_select_plan_cache_misses);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.shared_query_plan_cache_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.shared_query_plan_cache_misses);
    }

    Ok(SqlQueryPerfResult {
        result: first_result.expect("perf loop with runs > 0 should record one result"),
        attribution: average_attribution(
            total_compile_local_instructions,
            total_planner_local_instructions,
            total_executor_local_instructions,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_sql_compiled_command_cache_hits,
            total_sql_compiled_command_cache_misses,
            total_sql_select_plan_cache_hits,
            total_sql_select_plan_cache_misses,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
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
            let query = session.load::<PerfAuditUser>().order_by("id").limit(2);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_by("age")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit2.parity" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_by("age")
                .order_by("id")
                .limit(2);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.active_true.order_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("active").eq(true))
                .order_by("age")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_compare.age_eq_age_nat.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("age").eq_field("age_nat"))
                .order_by("age")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_between.rank_age_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("rank").between_fields("age", "age"))
                .order_by("age")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.rank.in_list.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter(FieldRef::new("rank").in_list([17_i32, 28_i32, 30_i32]))
                .order_by("age")
                .order_by("id")
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
                .order_by("age")
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
                .order_by("handle")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.gold_active.order_handle.asc.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter(Predicate::and(vec![
                    FieldRef::new("active").eq(true),
                    FieldRef::new("tier").eq("gold"),
                ]))
                .order_by("handle")
                .order_by("id")
                .limit(3);
            let (result, attribution) =
                session.execute_query_result_with_attribution(query.query())?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.score_gte_75.order_score.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter(FieldRef::new("score").gte(75_u64))
                .order_by("score")
                .order_by("id")
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
    let mut total_runtime_local_instructions = 0_u64;
    let mut total_finalize_local_instructions = 0_u64;
    let mut total_direct_data_row_scan_local_instructions = 0_u64;
    let mut total_direct_data_row_key_stream_local_instructions = 0_u64;
    let mut total_direct_data_row_row_read_local_instructions = 0_u64;
    let mut total_direct_data_row_key_encode_local_instructions = 0_u64;
    let mut total_direct_data_row_store_get_local_instructions = 0_u64;
    let mut total_direct_data_row_order_window_local_instructions = 0_u64;
    let mut total_direct_data_row_page_window_local_instructions = 0_u64;
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;

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
        total_runtime_local_instructions =
            total_runtime_local_instructions.saturating_add(attribution.runtime_local_instructions);
        total_finalize_local_instructions = total_finalize_local_instructions
            .saturating_add(attribution.finalize_local_instructions);
        total_direct_data_row_scan_local_instructions =
            total_direct_data_row_scan_local_instructions
                .saturating_add(attribution.direct_data_row_scan_local_instructions);
        total_direct_data_row_key_stream_local_instructions =
            total_direct_data_row_key_stream_local_instructions
                .saturating_add(attribution.direct_data_row_key_stream_local_instructions);
        total_direct_data_row_row_read_local_instructions =
            total_direct_data_row_row_read_local_instructions
                .saturating_add(attribution.direct_data_row_row_read_local_instructions);
        total_direct_data_row_key_encode_local_instructions =
            total_direct_data_row_key_encode_local_instructions
                .saturating_add(attribution.direct_data_row_key_encode_local_instructions);
        total_direct_data_row_store_get_local_instructions =
            total_direct_data_row_store_get_local_instructions
                .saturating_add(attribution.direct_data_row_store_get_local_instructions);
        total_direct_data_row_order_window_local_instructions =
            total_direct_data_row_order_window_local_instructions
                .saturating_add(attribution.direct_data_row_order_window_local_instructions);
        total_direct_data_row_page_window_local_instructions =
            total_direct_data_row_page_window_local_instructions
                .saturating_add(attribution.direct_data_row_page_window_local_instructions);
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
            total_runtime_local_instructions,
            total_finalize_local_instructions,
            total_direct_data_row_scan_local_instructions,
            total_direct_data_row_key_stream_local_instructions,
            total_direct_data_row_row_read_local_instructions,
            total_direct_data_row_key_encode_local_instructions,
            total_direct_data_row_store_get_local_instructions,
            total_direct_data_row_order_window_local_instructions,
            total_direct_data_row_page_window_local_instructions,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            runs,
        ),
    })
}

/// Clear all dedicated perf fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<PerfAuditAccount>().execute()?;
    db().delete::<PerfAuditUser>().execute()?;

    Ok(())
}

/// Load one deterministic fixture batch tuned for SQL perf audit queries.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;
    db().insert_many_atomic(perf_audit_users())?;
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
