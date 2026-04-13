//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

extern crate canic_cdk as ic_cdk;

use candid::CandidType;
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_test_sql_fixtures::sql::SqlTestUser;

icydb::start!();

// SqlQueryPerfResult
//
// Lightweight dev-shell envelope that preserves the normal SQL result payload
// while attaching one canister-local instruction delta for the query call.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

/// Clear all lightweight SQL smoke-test fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<SqlTestUser>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;
    db().insert_many_atomic(sql_users())?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
fn sql_users() -> Vec<SqlTestUser> {
    vec![
        SqlTestUser {
            name: "alice".to_string(),
            age: 31,
            rank: 28,
            ..Default::default()
        },
        SqlTestUser {
            name: "bob".to_string(),
            age: 24,
            rank: 25,
            ..Default::default()
        },
        SqlTestUser {
            name: "charlie".to_string(),
            age: 43,
            rank: 43,
            ..Default::default()
        },
    ]
}

/// Execute one SqlTestUser-only reduced SQL statement against the smoke canister.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<SqlTestUser>(sql.as_str())
}

/// Execute one SqlTestUser-only reduced SQL query and return one dev-shell
/// instruction delta alongside the normal SQL result payload.
#[cfg(feature = "sql")]
#[query]
fn query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db().execute_sql_query::<SqlTestUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlQueryPerfResult {
        result,
        instructions,
    })
}

/// Execute one SqlTestUser-only reduced SQL mutation against the smoke canister.
#[cfg(feature = "sql")]
#[update]
fn update(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_update::<SqlTestUser>(sql.as_str())
}

canic_cdk::export_candid!();
