//! Direct-relation control for the 0.253 measurement matrix.

icydb::ic_memory_range!(
    authority = "icydb.relation_cost_direct",
    start = 100,
    end = 254
);

icydb_testing_audit_nested_relation_fixtures::define_relation_cost_measurement_actor!();
