fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister,
        "icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister",
        "TenSimpleCanister"
    );

    Ok(())
}
