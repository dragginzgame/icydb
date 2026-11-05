//! One-entity public generated-schema evidence canister.

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants() -> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(vec![icydb::db::MemoryAuthority::new(
        "icydb_one_simple",
        "icydb.one_simple.",
    )?])
}

canic_icydb::start!(icydb);

canic_icydb::mount! {
    icydb_schema(authorization = public);
}
