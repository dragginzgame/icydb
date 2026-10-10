//! Single-valued nested-shape control for the 0.253 measurement matrix.

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.relation_cost_shallow",
            "icydb.relation_cost_shallow.",
        )?],
        vec![],
    )
}

icydb_testing_audit_nested_relation_fixtures::define_relation_cost_measurement_actor!(shallow);
