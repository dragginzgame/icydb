fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister,
    >();

    icydb::build!("icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister");

    Ok(())
}
