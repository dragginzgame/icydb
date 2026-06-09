fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ =
        std::any::TypeId::of::<icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister>();

    let options = icydb::build::BuildOptions::default()
        .with_sql_readonly_enabled(false)
        .with_sql_ddl_enabled(false)
        .with_sql_fixtures_enabled(false)
        .with_metrics_enabled(false)
        .with_snapshot_enabled(false)
        .with_schema_enabled(false);
    icydb::build_with_options!(
        "icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister",
        options
    );

    Ok(())
}
