//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

extern crate canic_cdk as ic_cdk;

use candid::CandidType;
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(all(feature = "sql", not(feature = "diagnostics")))]
use icydb::db::sql::SqlQueryResult;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
use icydb::db::{SqlQueryExecutionAttribution, sql::SqlQueryResult};
use icydb_testing_test_sql_fixtures::sql::SqlTestUser;

icydb::start!();

// SqlQueryPerfResult
//
// Lightweight dev-shell envelope that preserves the normal SQL result payload
// while attaching the current SQL compile/planner/store/executor/decode split.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
    planner_instructions: u64,
    store_instructions: u64,
    executor_instructions: u64,
    pure_covering_decode_instructions: u64,
    pure_covering_row_assembly_instructions: u64,
    decode_instructions: u64,
    compiler_instructions: u64,
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
impl SqlQueryPerfResult {
    fn from_attribution(result: SqlQueryResult, attribution: SqlQueryExecutionAttribution) -> Self {
        Self {
            result,
            instructions: attribution.total_local_instructions,
            planner_instructions: attribution.planner_local_instructions,
            store_instructions: attribution.store_local_instructions,
            executor_instructions: attribution.executor_local_instructions,
            pure_covering_decode_instructions: attribution.pure_covering_decode_local_instructions,
            pure_covering_row_assembly_instructions: attribution
                .pure_covering_row_assembly_local_instructions,
            decode_instructions: attribution.response_decode_local_instructions,
            compiler_instructions: attribution.compile_local_instructions,
        }
    }
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
/// compile/planner/store/executor/decode attribution split alongside the
/// normal SQL result payload.
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[query]
fn query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<SqlTestUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult::from_attribution(result, attribution))
}

/// Execute one SqlTestUser-only reduced SQL mutation against the smoke canister.
#[cfg(feature = "sql")]
#[update]
fn update(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_update::<SqlTestUser>(sql.as_str())
}

canic_cdk::export_candid!();
