fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister,
    >();

    let config =
        icydb_config_build::emit_config_for_canister("TenSimpleCanister", &["TenSimpleCanister"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("TenSimpleCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("TenSimpleCanister"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("TenSimpleCanister"))
        .with_metrics_enabled(config.canister_metrics_enabled("TenSimpleCanister"))
        .with_metrics_reset_enabled(config.canister_metrics_reset_enabled("TenSimpleCanister"))
        .with_snapshot_enabled(config.canister_snapshot_enabled("TenSimpleCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister",
        options
    );

    Ok(())
}
