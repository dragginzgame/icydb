fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<icydb_testing_test_sql_fixtures::sql::SqlTestCanister>();

    icydb::build!("icydb_testing_test_sql_fixtures::sql::SqlTestCanister");

    Ok(())
}
