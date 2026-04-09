fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister,
    >();

    icydb::build!("icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister");

    Ok(())
}
