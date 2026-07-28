use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::install_fixture_canister;

fn typed_query_row_count(fixture: &StandaloneCanisterFixture, method: &str) -> Result<u32, String> {
    fixture
        .query_call(method, ())
        .expect("typed query endpoint response should decode")
}

#[test]
fn query_only_typed_canisters_execute_without_sql() {
    for (canister, method) in [
        ("one_entity_typed_query", "query_one_entity_typed_rows"),
        ("ten_entity_typed_query", "query_ten_entity_typed_rows"),
    ] {
        let fixture = install_fixture_canister(canister);
        let row_count = typed_query_row_count(&fixture, method)
            .unwrap_or_else(|error| panic!("{canister} typed query should succeed: {error}"));

        assert_eq!(
            row_count, 0,
            "{canister} should execute its accepted-schema query over the empty initial store",
        );
    }
}
