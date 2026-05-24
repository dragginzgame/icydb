fn main() -> Result<(), Box<dyn std::error::Error>> {
    icydb_testing_wasm_helpers::build_configured_canister!(
        icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister,
        "icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister",
        "demo_rpg"
    );

    Ok(())
}
