//!
//! Ten-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::types::{Id, Ulid};
use icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleEntity01;

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.ten_simple",
            "icydb.ten_simple.",
        )?],
        vec![],
    )
}

icydb::start!();

#[ic_cdk::query]
fn query_ten_entity_typed_rows() -> u32 {
    icydb::db::with_request_execution(|| {
        let Ok(database) = db() else {
            return 0;
        };
        database
            .get::<TenSimpleEntity01>(Id::from_key(Ulid::MIN))
            .map_or(0, |row| u32::from(row.is_some()))
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
