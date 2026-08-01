//!
//! Metrics-enabled default empty canister used for wasm-footprint auditing.
//!

icydb::start!();

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
