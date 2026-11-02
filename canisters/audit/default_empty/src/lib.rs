//!
//! Default empty canister used for wasm-footprint baseline auditing.
//!

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants()
-> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb_default_empty",
            "icydb.default_empty.",
        )?],
    )
}

canic_icydb::start!(icydb);
