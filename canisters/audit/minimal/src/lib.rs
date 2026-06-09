//!
//! Minimal no-endpoint canister used for wasm-footprint baseline auditing.
//!

icydb::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
