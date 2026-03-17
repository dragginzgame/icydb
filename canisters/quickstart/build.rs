fn main() -> std::io::Result<()> {
    use icydb_testing_quickstart_fixtures as _;

    icydb::build!("icydb_testing_quickstart_fixtures::schema::relations::QuickstartCanister");

    Ok(())
}
