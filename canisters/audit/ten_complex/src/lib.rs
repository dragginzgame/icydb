//!
//! Ten-complex fluent-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::asc;
use icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_ten_complex_fluent() -> Result<u32, icydb::Error> {
    let rows = db()
        .load::<TenComplexEntity01>()
        .order_term(asc("id"))
        .limit(1)
        .execute()?
        .into_rows()?;

    Ok(rows.count())
}

ic_cdk::export_candid!();
