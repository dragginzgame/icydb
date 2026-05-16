fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister,
    >();

    let config = icydb_config_build::emit_config_for_canister(
        "TenComplexCanister",
        &["TenComplexCanister"],
    )?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("TenComplexCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("TenComplexCanister"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("TenComplexCanister"))
        .with_metrics_enabled(config.canister_metrics_enabled("TenComplexCanister"))
        .with_metrics_reset_enabled(config.canister_metrics_reset_enabled("TenComplexCanister"))
        .with_snapshot_enabled(config.canister_snapshot_enabled("TenComplexCanister"))
        .with_schema_enabled(config.canister_schema_enabled("TenComplexCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister",
        options
    );

    Ok(())
}
