//! Single-valued nested-shape control for the 0.253 measurement matrix.

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants()
-> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb_relation_cost_shallow",
            "icydb.relation_cost_shallow.",
        )?],
    )
}

icydb_testing_audit_nested_relation_fixtures::define_relation_cost_measurement_actor!(shallow);
