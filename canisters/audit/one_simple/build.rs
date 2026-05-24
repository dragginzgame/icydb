fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister,
        "icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister",
        "OneSimpleCanister"
    );

    Ok(())
}
