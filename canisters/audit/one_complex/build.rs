fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister,
    >();

    icydb::build!("icydb_testing_audit_one_complex_fixtures::one_complex::OneComplexCanister");

    Ok(())
}
