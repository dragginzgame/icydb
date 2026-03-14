fn main() -> std::io::Result<()> {
    use icydb_testing_fixtures as _;

    icydb::build!("icydb_testing_fixtures::schema::relations::SqlTestCanister");

    Ok(())
}
