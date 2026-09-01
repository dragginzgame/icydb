//!
//! Extended-metrics default empty canister used for wasm-footprint auditing.
//!

icydb::start!();

icydb::endpoints! {
    icydb_metrics(authorization = public);
    #[cfg(feature = "metrics-extended")]
    icydb_metrics_extended(authorization = public);
    icydb_metrics_reset;
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
