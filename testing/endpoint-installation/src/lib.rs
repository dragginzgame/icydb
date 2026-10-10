//! Released-package-shaped canister installation without IcyDB configuration.

runtime_api::endpoints! {
    icydb_metrics(authorization = public);
    icydb_schema(authorization = controller);
}

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<runtime_api::db::MemoryAllocationPool, runtime_api::db::MemoryAllocationPoolError> {
    runtime_api::db::MemoryAllocationPool::new(
        vec![runtime_api::db::MemoryAuthority::new(
            "icydb.default_empty",
            "icydb.default_empty.",
        )?],
        vec![],
    )
}

runtime_api::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
