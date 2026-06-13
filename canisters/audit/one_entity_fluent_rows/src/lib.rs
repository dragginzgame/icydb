//!
//! One-entity rows-only fluent-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::asc;
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_one_entity_fluent_rows() -> Result<u32, icydb::Error> {
    let rows = db()
        .load::<OneSimpleEntity01>()
        .order_term(asc("id"))
        .limit(1)
        .execute_rows()?;

    Ok(rows.count())
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
