fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_default_empty_fixtures::default_empty::DefaultEmptyCanister
    )?;

    Ok(())
}
