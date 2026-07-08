fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_configured_canister!(
        icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister,
        "icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister",
        "default_empty_metrics"
    );

    Ok(())
}
