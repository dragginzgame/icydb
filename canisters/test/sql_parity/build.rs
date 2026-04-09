fn main() -> std::io::Result<()> {
    let _ =
        std::any::TypeId::of::<icydb_testing_test_sql_parity_fixtures::schema::SqlParityCanister>();

    icydb::build!("icydb_testing_test_sql_parity_fixtures::schema::SqlParityCanister");

    Ok(())
}
