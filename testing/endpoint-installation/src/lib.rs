//! Released-package-shaped canister installation without IcyDB configuration.

runtime_api::endpoints! {
    icydb_metrics(authorization = public);
    icydb_schema(authorization = controller);
}

runtime_api::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
