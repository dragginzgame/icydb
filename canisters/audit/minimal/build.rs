fn main() -> std::io::Result<()> {
    use icydb_testing_audit_minimal_fixtures as _;

    icydb::build!("icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister");

    Ok(())
}
