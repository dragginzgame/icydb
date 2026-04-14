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
use icydb::db::sql::SqlQueryResult;
use icydb_testing_audit_sql_perf_fixtures::{PerfAuditAccount, PerfAuditUser};

icydb::start!();

// SqlQueryPerfResult
//
// Dedicated audit envelope that preserves the SQL result payload while
// attaching one canister-local instruction delta for the measured query call.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
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
    let start = ic_cdk::api::performance_counter(1);
    let result = db().execute_sql_query::<PerfAuditUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlQueryPerfResult {
        result,
        instructions,
    })
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
    let start = ic_cdk::api::performance_counter(1);
    let result = db().execute_sql_query::<PerfAuditAccount>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlQueryPerfResult {
        result,
        instructions,
    })
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
