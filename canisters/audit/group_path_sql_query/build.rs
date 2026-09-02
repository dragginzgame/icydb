fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_group_path_fixtures::group_path::GroupPathAuditCanister
    )?;

    Ok(())
}
