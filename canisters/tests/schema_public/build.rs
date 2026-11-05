fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister
    )?;

    Ok(())
}
