fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister
    )?;

    Ok(())
}
