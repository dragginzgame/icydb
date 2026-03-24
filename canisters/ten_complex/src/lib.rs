//!
//! Ten-complex SQL canister used for wasm-footprint auditing.
//!

use ic_cdk::export_candid;
#[cfg(feature = "sql")]
use ic_cdk::query;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;

icydb::start!();

/// Execute one reduced SQL statement against the ten-complex audit set.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests {
    #[test]
    fn generated_sql_dispatch_surface_is_stable() {
        icydb_testing_wasm_fixtures::assert_generated_sql_dispatch_surface_from_out_dir!();
    }
}

export_candid!();
