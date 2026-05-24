fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister,
        "icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister",
        "PerfAuditCanister"
    );

    Ok(())
}
