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
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
    error::{ErrorKind, ErrorOrigin, QueryErrorKind},
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

#[cfg(feature = "sql")]
fn invalid_perf_loop_runs_error() -> icydb::Error {
    icydb::Error::new(
        ErrorKind::Query(QueryErrorKind::Validate),
        ErrorOrigin::Query,
        "sql perf loop requires runs > 0",
    )
}

#[cfg(feature = "sql")]
fn average_attribution(
    total_compile_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    runs: u32,
) -> SqlQueryExecutionAttribution {
    let divisor = u64::from(runs);

    SqlQueryExecutionAttribution {
        compile_local_instructions: total_compile_local_instructions / divisor,
        execute_local_instructions: total_execute_local_instructions / divisor,
        total_local_instructions: total_local_instructions / divisor,
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
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_sql_query_with_attribution::<E>(sql)?;
        if first_result.is_none() {
            first_result = Some(result);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
    }

    Ok(SqlQueryPerfResult {
        result: first_result.expect("perf loop with runs > 0 should record one result"),
        attribution: average_attribution(
            total_compile_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
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
