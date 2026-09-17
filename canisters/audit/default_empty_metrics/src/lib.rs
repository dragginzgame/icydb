//!
//! Metrics-enabled default empty canister used for wasm-footprint auditing.
//!

// The host grants the pool; schema declarations name only permanent keys.
icydb::ic_memory_range!(
    authority = "icydb.default_empty",
    start = 100,
    end = 254,
    mode = Allowed
);

icydb::start!();

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
