fn main() -> std::io::Result<()> {
    use icydb_testing_test_sql_fixtures as _;

    icydb::build!("icydb_testing_test_sql_fixtures::sql::SqlTestCanister");

    Ok(())
}
