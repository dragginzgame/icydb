fn main() -> std::io::Result<()> {
    let _ =
        std::any::TypeId::of::<icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister>();

    icydb::build!("icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister");

    Ok(())
}
