fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister,
        "icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister",
        "MinimalCanister"
    );

    Ok(())
}
