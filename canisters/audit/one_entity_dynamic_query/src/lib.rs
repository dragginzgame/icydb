//!
//! One-entity dynamic-query canister used for wasm-footprint attribution.
//!

use icydb::{
    db::{DynamicQuery, query::FieldRef},
    types::Ulid,
};

icydb::start!();

#[ic_cdk::query]
fn query_one_entity_dynamic_rows() -> u32 {
    let Ok(database) = db() else {
        return 0;
    };
    let request = DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(Ulid::MIN));
    let Ok(output) = database.execute_public_dynamic_query(&request) else {
        return 0;
    };

    output.row_count
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
