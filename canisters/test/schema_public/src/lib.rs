//! One-entity public generated-schema evidence canister.

icydb::start!();

icydb::endpoints! {
    icydb_schema(authorization = public);
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
