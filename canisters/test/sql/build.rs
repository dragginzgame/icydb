fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<icydb_testing_test_sql_fixtures::sql::SqlTestCanister>();

    let config = icydb_config_build::emit_config_for_canister("test_sql", &["test_sql"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("test_sql"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("test_sql"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("test_sql"))
        .with_metrics_enabled(config.canister_metrics_enabled("test_sql"))
        .with_metrics_reset_enabled(config.canister_metrics_reset_enabled("test_sql"))
        .with_snapshot_enabled(config.canister_snapshot_enabled("test_sql"));
    icydb::build_with_options!(
        "icydb_testing_test_sql_fixtures::sql::SqlTestCanister",
        options
    );

    Ok(())
}
