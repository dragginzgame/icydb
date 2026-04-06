fn main() -> std::io::Result<()> {
    use icydb_testing_audit_ten_complex_fixtures as _;

    icydb::build!("icydb_testing_audit_ten_complex_fixtures::ten_complex::TenComplexCanister");

    Ok(())
}
