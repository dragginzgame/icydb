fn main() -> std::io::Result<()> {
    use icydb_testing_twenty_fixtures as _;

    icydb::build!("icydb_testing_twenty_fixtures::twenty::TwentyCanister");

    Ok(())
}
