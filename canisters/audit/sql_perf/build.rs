fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister,
    >();

    let config =
        icydb_config_build::emit_config_for_canister("PerfAuditCanister", &["PerfAuditCanister"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("PerfAuditCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("PerfAuditCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister",
        options
    );

    Ok(())
}
