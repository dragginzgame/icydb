fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_config::build_configured_canister!(
        icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister,
        "icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister",
        "one_entity_sql_query"
    );

    Ok(())
}
