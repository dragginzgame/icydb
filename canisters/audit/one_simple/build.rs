fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister,
    >();

    let config =
        icydb_config_build::emit_config_for_canister("OneSimpleCanister", &["OneSimpleCanister"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("OneSimpleCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("OneSimpleCanister"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("OneSimpleCanister"))
        .with_metrics_enabled(config.canister_metrics_enabled("OneSimpleCanister"))
        .with_metrics_reset_enabled(config.canister_metrics_reset_enabled("OneSimpleCanister"))
        .with_snapshot_enabled(config.canister_snapshot_enabled("OneSimpleCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister",
        options
    );

    Ok(())
}
