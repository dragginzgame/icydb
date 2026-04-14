fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister,
    >();

    icydb::build!("icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister");

    Ok(())
}
