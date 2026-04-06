fn main() -> std::io::Result<()> {
    use icydb_testing_demo_rpg_fixtures as _;

    icydb::build!("icydb_testing_demo_rpg_fixtures::schema::relations::DemoRpgCanister");

    Ok(())
}
