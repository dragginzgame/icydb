fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister,
        "icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister",
        "TenComplexCanister"
    );

    Ok(())
}
