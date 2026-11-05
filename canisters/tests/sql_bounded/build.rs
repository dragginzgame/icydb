fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(icydb_bounded_update)");
    println!("cargo:rustc-cfg=icydb_bounded_update");
    icydb::build::build_canister!(icydb_testing_test_sql_fixtures::sql::SqlTestCanister)?;

    Ok(())
}
