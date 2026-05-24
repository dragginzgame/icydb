fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_test_sql_fixtures::sql::SqlTestCanister,
        "icydb_testing_test_sql_fixtures::sql::SqlTestCanister",
        "test_sql"
    );

    Ok(())
}
