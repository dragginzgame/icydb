fn main() -> Result<(), Box<dyn std::error::Error>> {
    runtime_api::build::build_canister!(
        icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister
    )?;

    Ok(())
}
