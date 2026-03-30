//!
//! Minimal SQL canister used for wasm-footprint auditing.
//!

#[cfg(debug_assertions)]
use canic::export_candid;
#[cfg(feature = "sql")]
use ic_cdk::query;
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
icydb_testing_wasm_fixtures::define_generated_sql_dispatch_surface_stability_test!();

#[cfg(debug_assertions)]
export_candid!();
