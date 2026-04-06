fn main() -> std::io::Result<()> {
    use icydb_testing_test_sql_parity_fixtures as _;

    icydb::build!("icydb_testing_test_sql_parity_fixtures::schema::SqlParityCanister");

    Ok(())
}
