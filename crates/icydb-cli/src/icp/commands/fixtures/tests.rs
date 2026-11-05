use super::fixture_load_result;

#[test]
fn deployed_fixture_method_absence_remains_an_authoritative_failure() {
    assert!(fixture_load_result(Err("CanisterMethodNotFound".into())).is_err());
}
