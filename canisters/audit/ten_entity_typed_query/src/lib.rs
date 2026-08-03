//!
//! Ten-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::FieldRef;
use icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_ten_entity_typed_rows() -> u32 {
    let Ok(database) = db() else {
        return 0;
    };
    let Ok(query) = database.query::<TenSimpleEntity01>() else {
        return 0;
    };
    let Ok(rows) = query
        .filter(FieldRef::new("id").eq(icydb::types::Ulid::MIN))
        .execute_rows()
    else {
        return 0;
    };

    u32::try_from(rows.len()).unwrap_or(u32::MAX)
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
