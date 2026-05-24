fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister,
        "icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister",
        "OneComplexCanister"
    );

    Ok(())
}
