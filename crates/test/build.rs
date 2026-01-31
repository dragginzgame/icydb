use test_design as _;

fn main() -> std::io::Result<()> {
    icydb::build!("test_design::schema::test::TestCanister");

    Ok(())
}
