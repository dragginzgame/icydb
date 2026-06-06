//!
//! One hand-written SQL query endpoint used for wasm-footprint auditing.
//!

#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
#[cfg(feature = "sql")]
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[cfg(feature = "sql")]
#[ic_cdk::query]
fn query_one_sql() -> Result<SqlQueryResult, icydb::Error> {
    db().execute_sql_query::<OneSimpleEntity01>("SELECT COUNT(*) FROM OneSimpleEntity01")
}

ic_cdk::export_candid!();
