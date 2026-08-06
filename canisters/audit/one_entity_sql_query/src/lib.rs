//!
//! One-entity SQL query endpoint used for wasm-footprint auditing.
//!

#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
icydb::start!();

#[cfg(feature = "sql")]
#[ic_cdk::query]
fn query_one_entity_sql() -> u32 {
    icydb::db::with_request_execution(|| {
        let Ok(database) = db() else {
            return 0;
        };
        let Ok(result) = database.execute_trusted_sql_query(
            "SELECT * FROM OneSimpleEntity01 WHERE id = '00000000000000000000000000'",
        ) else {
            return 0;
        };

        match result {
            SqlQueryResult::Projection(output) => output.row_count,
            _ => 0,
        }
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
