//!
//! One-simple SQL canister used for wasm-footprint auditing.
//!

extern crate canic_cdk as ic_cdk;

#[cfg(feature = "sql")]
use canic_cdk::query;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;

icydb::start!();

#[cfg_attr(doc, doc = "Query.")]
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

#[cfg(all(test, feature = "sql"))]
icydb_testing_wasm_helpers::define_generated_sql_dispatch_surface_stability_test!();

canic_cdk::export_candid!();
