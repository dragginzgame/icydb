//!
//! Metrics-enabled default empty canister used for wasm-footprint auditing.
//!

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.default_empty",
            "icydb.default_empty.",
        )?],
        vec![],
    )
}

icydb::start!();

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
