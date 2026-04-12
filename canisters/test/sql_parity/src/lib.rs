//!
//! Test-only SQL parity canister used for typed and fluent SQL fixture checks.
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
use icydb::traits::Path;
#[cfg(feature = "sql")]
use icydb::{
    db::sql::SqlQueryResult,
    db::{SqlStatementRoute, identifiers_tail_match},
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::EntitySchema,
};
use icydb_core::db::IndexState;
use icydb_testing_test_sql_parity_fixtures::{
    fixtures,
    schema::{
        Customer, CustomerAccount, CustomerOrder, PlannerChoice, PlannerPrefixChoice,
        PlannerUniquePrefixChoice, SqlWriteProbe,
    },
};

icydb::start!();

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

#[cfg(feature = "sql")]
const fn sql_entity_route_names() -> [&'static str; 7] {
    [
        "Customer",
        "CustomerAccount",
        "CustomerOrder",
        "SqlWriteProbe",
        "PlannerChoice",
        "PlannerPrefixChoice",
        "PlannerUniquePrefixChoice",
    ]
}

#[cfg(feature = "sql")]
fn unsupported_query_entity_error(entity: &str) -> icydb::Error {
    let supported_entities = sql_entity_route_names().join(", ");

    icydb::Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        format!(
            "query endpoint does not support entity '{entity}'; supported entities: {supported_entities}"
        ),
    )
}

#[cfg(feature = "sql")]
pub(crate) enum RoutedSqlEntity {
    Customer,
    CustomerAccount,
    CustomerOrder,
    SqlWriteProbe,
    PlannerChoice,
    PlannerPrefixChoice,
    PlannerUniquePrefixChoice,
}

#[cfg(feature = "sql")]
pub(crate) fn routed_sql_entity(entity: &str) -> Result<RoutedSqlEntity, icydb::Error> {
    if identifiers_tail_match(entity, Customer::MODEL.name()) {
        Ok(RoutedSqlEntity::Customer)
    } else if identifiers_tail_match(entity, CustomerAccount::MODEL.name()) {
        Ok(RoutedSqlEntity::CustomerAccount)
    } else if identifiers_tail_match(entity, CustomerOrder::MODEL.name()) {
        Ok(RoutedSqlEntity::CustomerOrder)
    } else if identifiers_tail_match(entity, SqlWriteProbe::MODEL.name()) {
        Ok(RoutedSqlEntity::SqlWriteProbe)
    } else if identifiers_tail_match(entity, PlannerChoice::MODEL.name()) {
        Ok(RoutedSqlEntity::PlannerChoice)
    } else if identifiers_tail_match(entity, PlannerPrefixChoice::MODEL.name()) {
        Ok(RoutedSqlEntity::PlannerPrefixChoice)
    } else if identifiers_tail_match(entity, PlannerUniquePrefixChoice::MODEL.name()) {
        Ok(RoutedSqlEntity::PlannerUniquePrefixChoice)
    } else {
        Err(unsupported_query_entity_error(entity))
    }
}

#[cfg(feature = "sql")]
pub(crate) fn execute_entity_routed_sql(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
    let route = db().sql_statement_route(sql)?;

    match route {
        SqlStatementRoute::ShowEntities => db().execute_entity_sql::<Customer>(sql),
        SqlStatementRoute::Query { entity }
        | SqlStatementRoute::Insert { entity }
        | SqlStatementRoute::Update { entity }
        | SqlStatementRoute::Explain { entity }
        | SqlStatementRoute::Describe { entity }
        | SqlStatementRoute::ShowIndexes { entity }
        | SqlStatementRoute::ShowColumns { entity } => match routed_sql_entity(entity.as_str())? {
            RoutedSqlEntity::Customer => db().execute_entity_sql::<Customer>(sql),
            RoutedSqlEntity::CustomerAccount => db().execute_entity_sql::<CustomerAccount>(sql),
            RoutedSqlEntity::CustomerOrder => db().execute_entity_sql::<CustomerOrder>(sql),
            RoutedSqlEntity::SqlWriteProbe => db().execute_entity_sql::<SqlWriteProbe>(sql),
            RoutedSqlEntity::PlannerChoice => db().execute_entity_sql::<PlannerChoice>(sql),
            RoutedSqlEntity::PlannerPrefixChoice => {
                db().execute_entity_sql::<PlannerPrefixChoice>(sql)
            }
            RoutedSqlEntity::PlannerUniquePrefixChoice => {
                db().execute_entity_sql::<PlannerUniquePrefixChoice>(sql)
            }
        },
    }
}

/// Execute one reduced SQL statement against the parity canister.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    execute_entity_routed_sql(sql.as_str())
}

/// Measure one repeated SQL surface invocation inside wasm.
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

canic_cdk::export_candid!();
