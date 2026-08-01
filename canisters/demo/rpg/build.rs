fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb::build::build_canister!(
        icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister
    )?;

    Ok(())
}
