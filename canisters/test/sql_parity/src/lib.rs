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
#[cfg(feature = "sql")]
use icydb::types::Ulid;
use icydb_core::db::IndexState;
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
    static DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_ASC_LEADING_ID: RefCell<Option<Ulid>> = const {
        RefCell::new(None)
    };
    static DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_DESC_LEADING_ID: RefCell<Option<Ulid>> = const {
        RefCell::new(None)
    };
    static DEFAULT_CUSTOMER_ORDER_COMPOSITE_ASC_LEADING_ID: RefCell<Option<Ulid>> = const {
        RefCell::new(None)
    };
    static DEFAULT_CUSTOMER_ORDER_COMPOSITE_DESC_LEADING_ID: RefCell<Option<Ulid>> = const {
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
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_ASC_LEADING_ID.with_borrow_mut(|id| *id = None);
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_DESC_LEADING_ID.with_borrow_mut(|id| *id = None);
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_COMPOSITE_ASC_LEADING_ID.with_borrow_mut(|id| *id = None);
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_COMPOSITE_DESC_LEADING_ID.with_borrow_mut(|id| *id = None);

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    let customers = fixtures::customers();
    #[cfg(feature = "sql")]
    let customer_orders = fixtures::customer_orders();
    #[cfg(feature = "sql")]
    let default_customer_name_order_leading_id = customers
        .iter()
        .find(|customer| customer.name == "alice")
        .map(|customer| customer.id)
        .expect("default Customer fixtures should include 'alice'");
    #[cfg(feature = "sql")]
    let default_customer_order_composite_asc_leading_id = customer_orders
        .iter()
        .find(|order| order.name == "A-100")
        .map(|order| order.id)
        .expect("default CustomerOrder fixtures should include 'A-100'");
    #[cfg(feature = "sql")]
    let default_customer_order_priority20_status_asc_leading_id = customer_orders
        .iter()
        .find(|order| order.name == "A-101")
        .map(|order| order.id)
        .expect("default CustomerOrder fixtures should include 'A-101'");
    #[cfg(feature = "sql")]
    let default_customer_order_priority20_status_desc_leading_id = customer_orders
        .iter()
        .find(|order| order.name == "C-300")
        .map(|order| order.id)
        .expect("default CustomerOrder fixtures should include 'C-300'");
    #[cfg(feature = "sql")]
    let default_customer_order_composite_desc_leading_id = customer_orders
        .iter()
        .find(|order| order.name == "Z-900")
        .map(|order| order.id)
        .expect("default CustomerOrder fixtures should include 'Z-900'");

    db().insert_many_atomic(customers)?;
    db().insert_many_atomic(fixtures::customer_accounts())?;
    #[cfg(feature = "sql")]
    db().insert_many_atomic(customer_orders)?;
    #[cfg(not(feature = "sql"))]
    db().insert_many_atomic(fixtures::customer_orders())?;
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_NAME_ORDER_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_name_order_leading_id));
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_ASC_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_order_priority20_status_asc_leading_id));
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_DESC_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_order_priority20_status_desc_leading_id));
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_COMPOSITE_ASC_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_order_composite_asc_leading_id));
    #[cfg(feature = "sql")]
    DEFAULT_CUSTOMER_ORDER_COMPOSITE_DESC_LEADING_ID
        .with_borrow_mut(|id| *id = Some(default_customer_order_composite_desc_leading_id));

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

/// Remove the leading CustomerOrder composite-order base row while leaving the
/// secondary `(priority, status)` index entry intact so tests can exercise the
/// stale order-only composite path.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_make_customer_order_order_only_composite_stale() -> Result<(), icydb::Error> {
    // Phase 1: resolve the cached default leading composite-order row id
    // recorded when the default fixture dataset was loaded.
    let order_id = DEFAULT_CUSTOMER_ORDER_COMPOSITE_ASC_LEADING_ID
        .with_borrow(|id| *id)
        .expect("expected cached default CustomerOrder composite-order row 'A-100'");

    // Phase 2: remove only the base row bytes and keep the composite
    // secondary index entry intact so the stale order-only route becomes
    // measurable.
    icydb::db::debug_remove_entity_row_data_only::<SqlParityCanister, CustomerOrder>(
        &core_db(),
        &order_id,
    )?;

    Ok(())
}

/// Remove the leading `priority = 20` CustomerOrder base row while leaving the
/// secondary `(priority, status)` index entry intact so tests can exercise the
/// stale equality-prefix suffix-order path.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_make_customer_order_numeric_equality_stale() -> Result<(), icydb::Error> {
    // Phase 1: resolve the cached default leading equality-prefix row id
    // recorded when the default fixture dataset was loaded.
    let order_id = DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_ASC_LEADING_ID
        .with_borrow(|id| *id)
        .expect("expected cached default CustomerOrder numeric-equality row 'A-101'");

    // Phase 2: remove only the base row bytes and keep the composite
    // secondary index entry intact so the stale equality-prefix route becomes
    // measurable.
    icydb::db::debug_remove_entity_row_data_only::<SqlParityCanister, CustomerOrder>(
        &core_db(),
        &order_id,
    )?;

    Ok(())
}

/// Remove the leading descending `priority = 20` CustomerOrder base row while
/// leaving the secondary `(priority, status)` index entry intact so tests can
/// exercise the stale descending equality-prefix suffix-order path.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_make_customer_order_numeric_equality_desc_stale() -> Result<(), icydb::Error> {
    // Phase 1: resolve the cached default leading descending equality-prefix
    // row id recorded when the default fixture dataset was loaded.
    let order_id = DEFAULT_CUSTOMER_ORDER_PRIORITY20_STATUS_DESC_LEADING_ID
        .with_borrow(|id| *id)
        .expect("expected cached default descending CustomerOrder numeric-equality row 'C-300'");

    // Phase 2: remove only the base row bytes and keep the composite
    // secondary index entry intact so the stale descending equality-prefix
    // route becomes measurable.
    icydb::db::debug_remove_entity_row_data_only::<SqlParityCanister, CustomerOrder>(
        &core_db(),
        &order_id,
    )?;

    Ok(())
}

/// Remove the leading descending CustomerOrder composite-order base row while
/// leaving the secondary `(priority, status)` index entry intact so tests can
/// exercise the stale descending composite order-only path.
#[cfg(feature = "sql")]
#[doc(hidden)]
#[update]
fn fixtures_make_customer_order_order_only_composite_desc_stale() -> Result<(), icydb::Error> {
    // Phase 1: resolve the cached default leading descending composite-order
    // row id recorded when the default fixture dataset was loaded.
    let order_id = DEFAULT_CUSTOMER_ORDER_COMPOSITE_DESC_LEADING_ID
        .with_borrow(|id| *id)
        .expect("expected cached default descending CustomerOrder composite-order row 'Z-900'");

    // Phase 2: remove only the base row bytes and keep the composite
    // secondary index entry intact so the stale descending order-only route
    // becomes measurable.
    icydb::db::debug_remove_entity_row_data_only::<SqlParityCanister, CustomerOrder>(
        &core_db(),
        &order_id,
    )?;

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
