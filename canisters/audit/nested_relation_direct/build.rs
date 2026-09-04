fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_audit_nested_relation_fixtures::nested_relation::direct::RelationCostDirectCanister
    )?;

    Ok(())
}
