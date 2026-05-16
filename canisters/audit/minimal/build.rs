fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ =
        std::any::TypeId::of::<icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister>();

    let config =
        icydb_config_build::emit_config_for_canister("MinimalCanister", &["MinimalCanister"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("MinimalCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("MinimalCanister"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("MinimalCanister"))
        .with_metrics_enabled(config.canister_metrics_enabled("MinimalCanister"))
        .with_metrics_reset_enabled(config.canister_metrics_reset_enabled("MinimalCanister"))
        .with_snapshot_enabled(config.canister_snapshot_enabled("MinimalCanister"))
        .with_schema_enabled(config.canister_schema_enabled("MinimalCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister",
        options
    );

    Ok(())
}
