fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister,
    >();

    let config = icydb_config_build::emit_config_for_canister("demo_rpg", &["demo_rpg"])?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("demo_rpg"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("demo_rpg"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("demo_rpg"));
    icydb::build_with_options!(
        "icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister",
        options
    );

    Ok(())
}
