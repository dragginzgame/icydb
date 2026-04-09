fn main() -> std::io::Result<()> {
    let _ = std::any::TypeId::of::<
        icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister,
    >();

    icydb::build!("icydb_testing_audit_ten_simple_fixtures::ten_simple::TenSimpleCanister");

    Ok(())
}
