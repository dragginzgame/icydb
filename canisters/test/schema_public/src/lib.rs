//! One-entity public generated-schema evidence canister.

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.one_simple",
            "icydb.one_simple.",
        )?],
        vec![],
    )
}

icydb::start!();

icydb::endpoints! {
    icydb_schema(authorization = public);
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
