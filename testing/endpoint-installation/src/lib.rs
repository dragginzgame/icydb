//! Released-package-shaped canister installation without IcyDB configuration.

canic_icydb::mount! {
    icydb_metrics(authorization = public);
    icydb_schema(authorization = controller);
}

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants()
-> Result<runtime_api::db::AllocationGrants, runtime_api::db::AdmissionError> {
    runtime_api::db::AllocationGrants::new(
        vec![runtime_api::db::MemoryAuthority::new(
            "icydb_default_empty",
            "icydb.default_empty.",
        )?],
    )
}

canic_icydb::start!(runtime_api);
