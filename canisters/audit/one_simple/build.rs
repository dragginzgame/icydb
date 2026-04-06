fn main() -> std::io::Result<()> {
    use icydb_testing_audit_one_simple_fixtures as _;

    icydb::build!("icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister");

    Ok(())
}
