//!
//! Metrics-enabled default empty canister used for wasm-footprint auditing.
//!

icydb::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
