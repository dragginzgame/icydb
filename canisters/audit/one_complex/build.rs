fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister,
    >();

    let config = icydb_config_build::emit_config_for_canister(
        "OneComplexCanister",
        &["OneComplexCanister"],
    )?;
    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled("OneComplexCanister"))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled("OneComplexCanister"))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled("OneComplexCanister"));
    icydb::build_with_options!(
        "icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister",
        options
    );

    Ok(())
}
