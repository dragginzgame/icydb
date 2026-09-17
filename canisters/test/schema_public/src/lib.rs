//! One-entity public generated-schema evidence canister.

// The host grants the pool; schema declarations name only permanent keys.
icydb::ic_memory_range!(
    authority = "icydb.one_simple",
    start = 100,
    end = 254,
    mode = Allowed
);

icydb::start!();

icydb::endpoints! {
    icydb_schema(authorization = public);
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
