fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister,
    >();

    icydb::build!("icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleCanister");

    Ok(())
}
