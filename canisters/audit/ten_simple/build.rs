fn main() -> std::io::Result<()> {
    use icydb_testing_audit_ten_simple_fixtures as _;

    icydb::build!("icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister");

    Ok(())
}
