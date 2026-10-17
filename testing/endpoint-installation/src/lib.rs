//! Released-package-shaped canister installation without IcyDB configuration.

canic_icydb::endpoints! {
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

canic_icydb::start!(runtime_api);
