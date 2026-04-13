//!
//! Character-only RPG demo canister used by local demos and fixture loading.
//!

extern crate canic_cdk as ic_cdk;

use candid::CandidType;
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_demo_rpg_fixtures::{fixtures, schema::Character};

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

/// Clear all fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<Character>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    db().insert_many_atomic(fixtures::characters())?;

    Ok(())
}

/// Execute one Character-only reduced SQL query against the demo canister.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<Character>(sql.as_str())
}

/// Execute one Character-only reduced SQL query and return one dev-shell
/// instruction delta alongside the normal SQL result payload.
#[cfg(feature = "sql")]
#[query]
fn query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db().execute_sql_query::<Character>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlQueryPerfResult {
        result,
        instructions,
    })
}

/// Execute one Character-only reduced SQL mutation against the demo canister.
#[cfg(feature = "sql")]
#[update]
fn update(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_update::<Character>(sql.as_str())
}

canic_cdk::export_candid!();
