//!
//! Test-only SQL parity canister used for broad generated-vs-typed parity checks.
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
use icydb::traits::Path;
use icydb_core::db::IndexState;
use icydb_testing_test_sql_parity_fixtures::{
    fixtures,
    schema::{
        Customer, CustomerAccount, CustomerOrder, PlannerChoice, PlannerPrefixChoice,
        PlannerUniquePrefixChoice, SqlWriteProbe,
    },
};

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

/// Clear all fixture rows from this test canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<PlannerUniquePrefixChoice>().execute()?;
    db().delete::<PlannerPrefixChoice>().execute()?;
    db().delete::<PlannerChoice>().execute()?;
    db().delete::<SqlWriteProbe>().execute()?;
    db().delete::<CustomerOrder>().execute()?;
    db().delete::<CustomerAccount>().execute()?;
    db().delete::<Customer>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    let customers = fixtures::customers();
    #[cfg(feature = "sql")]
    let customer_orders = fixtures::customer_orders();

    db().insert_many_atomic(customers)?;
    db().insert_many_atomic(fixtures::customer_accounts())?;
    db().insert_many_atomic(fixtures::sql_write_probes())?;
    db().insert_many_atomic(fixtures::planner_unique_prefix_choices())?;
    db().insert_many_atomic(fixtures::planner_prefix_choices())?;
    db().insert_many_atomic(fixtures::planner_choices())?;
    #[cfg(feature = "sql")]
    db().insert_many_atomic(customer_orders)?;
    #[cfg(not(feature = "sql"))]
    db().insert_many_atomic(fixtures::customer_orders())?;

    Ok(())
}

/// Load one larger deterministic fixture dataset for perf audits.
#[update]
fn fixtures_load_perf_audit() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    db().insert_many_atomic(fixtures::perf_audit_customers())?;
    db().insert_many_atomic(fixtures::perf_audit_customer_accounts())?;
    db().insert_many_atomic(fixtures::perf_audit_sql_write_probes())?;
    db().insert_many_atomic(fixtures::planner_unique_prefix_choices())?;
    db().insert_many_atomic(fixtures::planner_prefix_choices())?;
    db().insert_many_atomic(fixtures::planner_choices())?;
    db().insert_many_atomic(fixtures::perf_audit_customer_orders())?;

    Ok(())
}

/// Mark the shared sql_parity store index state as Building so canister-level
/// tests can lock the fail-closed explain surface for previously probe-free
/// covering cohorts.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_mark_customer_index_building() -> Result<(), icydb::Error> {
    icydb_core::db::debug_mark_store_index_state(
        &core_db(),
        <Customer as icydb::traits::EntityPlacement>::Store::PATH,
        IndexState::Building,
    )?;

    Ok(())
}

#[cfg(all(test, feature = "sql"))]
include!("tests.rs");

canic_cdk::export_candid!();
