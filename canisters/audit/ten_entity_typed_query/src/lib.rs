//!
//! Ten-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::asc;
use icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_ten_entity_typed_rows() -> Result<u32, String> {
    let rows = db()
        .map_err(|error| error.to_string())?
        .query::<TenSimpleEntity01>()
        .map_err(|error| error.to_string())?
        .order_by(asc("id"))
        .limit(1)
        .execute_rows()
        .map_err(|error| error.to_string())?;

    u32::try_from(rows.len()).map_err(|_| "typed query row count exceeds u32".to_string())
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
