fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ =
        std::any::TypeId::of::<icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister>();

    let options = icydb::build::BuildOptions::default().with_metrics_enabled(true);
    icydb::build_with_options!(
        "icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister",
        options
    );

    Ok(())
}
