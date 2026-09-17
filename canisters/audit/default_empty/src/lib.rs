//!
//! Default empty canister used for wasm-footprint baseline auditing.
//!

// The host grants the pool; schema declarations name only permanent keys.
icydb::ic_memory_range!(
    authority = "icydb.default_empty",
    start = 100,
    end = 254,
    mode = Allowed
);

icydb::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
