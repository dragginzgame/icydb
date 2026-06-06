//!
//! One hand-written fluent query endpoint used for wasm-footprint auditing.
//!

use icydb::db::query::asc;
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_one_fluent() -> Result<u32, icydb::Error> {
    let rows = db()
        .load::<OneSimpleEntity01>()
        .order_term(asc("id"))
        .limit(1)
        .execute()?
        .into_rows()?;

    Ok(rows.count())
}

ic_cdk::export_candid!();
