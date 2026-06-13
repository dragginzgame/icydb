//!
//! Ten-entity rows-only fluent-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::asc;
use icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_ten_entity_fluent_rows() -> Result<u32, icydb::Error> {
    let rows = db()
        .load::<TenSimpleEntity01>()
        .order_term(asc("id"))
        .limit(1)
        .execute_rows()?;

    Ok(rows.count())
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
