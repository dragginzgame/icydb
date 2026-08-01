fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister
    )?;

    Ok(())
}
