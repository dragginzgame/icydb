fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister,
    >();

    let options = icydb::build::BuildOptions::default();
    icydb::build_with_options!(
        "icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister",
        options
    );

    Ok(())
}
