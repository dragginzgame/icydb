fn main() -> std::io::Result<()> {
    use icydb_testing_sql_test_fixtures as _;

    icydb::build!("icydb_testing_sql_test_fixtures::schema::relations::SqlTestCanister");

    Ok(())
}
