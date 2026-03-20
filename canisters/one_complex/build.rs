fn main() -> std::io::Result<()> {
    use icydb_testing_one_complex_fixtures as _;

    icydb::build!("icydb_testing_one_complex_fixtures::one_complex::OneComplexCanister");

    Ok(())
}
