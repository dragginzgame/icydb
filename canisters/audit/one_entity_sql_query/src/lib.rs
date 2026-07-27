//!
//! One-entity SQL query endpoint used for wasm-footprint auditing.
//!

#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
icydb::start!();

#[cfg(feature = "sql")]
#[ic_cdk::query]
fn query_one_entity_sql() -> Result<SqlQueryResult, icydb::Error> {
    db()?.execute_trusted_sql_query("SELECT COUNT(*) FROM OneSimpleEntity01")
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
