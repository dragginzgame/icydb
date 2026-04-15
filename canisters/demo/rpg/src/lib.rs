//!
//! Character-only RPG demo canister used by local demos and fixture loading.
//!

extern crate canic_cdk as ic_cdk;

use candid::CandidType;
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(all(feature = "sql", not(feature = "perf-attribution")))]
use icydb::db::sql::SqlQueryResult;
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
use icydb::db::{SqlQueryExecutionAttribution, sql::SqlQueryResult};
use icydb_testing_demo_rpg_fixtures::{fixtures, schema::Character};

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
    decode_instructions: u64,
    compiler_instructions: u64,
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
impl SqlQueryPerfResult {
    const fn from_attribution(
        result: SqlQueryResult,
        attribution: SqlQueryExecutionAttribution,
    ) -> Self {
        Self {
            result,
            instructions: attribution.total_local_instructions,
            planner_instructions: attribution.planner_local_instructions,
            store_instructions: attribution.store_local_instructions,
            executor_instructions: attribution.executor_local_instructions,
            decode_instructions: attribution.response_decode_local_instructions,
            compiler_instructions: attribution.compile_local_instructions,
        }
    }
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
/// compile/planner/store/executor/decode attribution split alongside the
/// normal SQL result payload.
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
#[query]
fn query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db().execute_sql_query_with_attribution::<Character>(sql.as_str())?;

    Ok(SqlQueryPerfResult::from_attribution(result, attribution))
}

/// Execute one Character-only reduced SQL mutation against the demo canister.
#[cfg(feature = "sql")]
#[update]
fn update(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_update::<Character>(sql.as_str())
}

canic_cdk::export_candid!();
