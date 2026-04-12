//!
//! Ten-simple SQL canister used for wasm-footprint auditing.
//!

extern crate canic_cdk as ic_cdk;

icydb::start!();

canic_cdk::export_candid!();
