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
#[cfg(feature = "sql")]
use icydb::types::Ulid;
use icydb_testing_test_sql_parity_fixtures::{
    fixtures,
    schema::{Customer, CustomerAccount, CustomerOrder, SqlParityCanister},
};
#[cfg(feature = "sql")]
use std::cell::RefCell;

icydb::start!();

#[cfg(feature = "sql")]
thread_local! {
    static DEFAULT_CUSTOMER_NAME_ORDER_LEADING_ID: RefCell<Option<Ulid>> = const {
        RefCell::new(None)
    };
}

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
    db().delete::<CustomerOrder>().execute()?;
    db().delete::<CustomerAccount>().execute()?;
    db().delete::<Customer>().execute()?;
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_NAME_ORDER_LEADING_ID.with_borrow_mut(|id| *id = None);

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    let customers = fixtures::customers();
    #[cfg(feature = "sql")]
    let default_customer_name_order_leading_id = customers
        .iter()
        .find(|customer| customer.name == "alice")
        .map(|customer| customer.id)
        .expect("default Customer fixtures should include 'alice'");

    db().insert_many_atomic(customers)?;
    db().insert_many_atomic(fixtures::customer_accounts())?;
    db().insert_many_atomic(fixtures::customer_orders())?;
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_NAME_ORDER_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_name_order_leading_id));

    Ok(())
}

/// Remove the leading Customer base row while leaving the secondary `name`
/// index entry intact so tests can exercise stale-row fallback.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_make_customer_name_order_stale() -> Result<(), icydb::Error> {
    // Phase 1: resolve the cached default `alice` id recorded when the
    // default fixture dataset was loaded.
    let customer_id = DEFAULT_CUSTOMER_NAME_ORDER_LEADING_ID
        .with_borrow(|id| *id)
        .expect("expected cached default Customer fixture row 'alice'");

    // Phase 2: remove only the base row bytes and keep the secondary index
    // entry intact so the stale-fallback covering route becomes observable.
    icydb::db::debug_remove_entity_row_data_only::<SqlParityCanister, Customer>(
        &core_db(),
        &customer_id,
    )?;

    Ok(())
}

#[cfg(all(test, feature = "sql"))]
include!("tests.rs");

canic_cdk::export_candid!();
