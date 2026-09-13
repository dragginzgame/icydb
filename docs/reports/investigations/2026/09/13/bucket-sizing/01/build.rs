fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(icydb_testing_test_sql_fixtures::sql::SqlTestCanister)?;
    Ok(())
}
