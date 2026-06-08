//!
//! Minimal SQL canister used for wasm-footprint auditing.
//!

icydb::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
