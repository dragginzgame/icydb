//!
//! Character-only RPG demo canister used by local demos and demo-local perf
//! probes.
//!

#[cfg(feature = "sql")]
mod perf;

extern crate canic_cdk as ic_cdk;

#[cfg(feature = "sql")]
use crate::perf::{
    SqlPerfAttributionRequest, SqlPerfAttributionSample, SqlPerfRequest, SqlPerfSample,
};
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_demo_rpg_fixtures::{fixtures, schema::Character};

icydb::start!();

/// Return one list of fixture entity names accepted by the SQL endpoints.
#[cfg(feature = "sql")]
#[query]
fn sql_entities() -> Vec<String> {
    sql_dispatch::entities()
}

/// Execute one reduced SQL statement against fixture entities.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

/// Measure one repeated SQL surface invocation inside wasm and return local
/// instruction totals plus one compact outcome summary.
#[cfg(feature = "sql")]
#[query]
fn sql_perf(request: SqlPerfRequest) -> Result<SqlPerfSample, icydb::Error> {
    perf::sample_sql_surface(request)
}

/// Attribute one representative SQL surface into fixed-cost wasm phases.
#[cfg(feature = "sql")]
#[query]
fn sql_perf_attribution(
    request: SqlPerfAttributionRequest,
) -> Result<SqlPerfAttributionSample, icydb::Error> {
    perf::attribute_sql_surface(request)
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

canic_cdk::export_candid!();
