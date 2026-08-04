use candid::{CandidType, Deserialize};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::{install_fixture_canister, upgrade_fixture_canister};

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct SchemaApplicationPerfResult {
    local_instructions: u64,
    reconcile_checks: u64,
    first_create: u64,
    exact_match: u64,
}

fn query_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, icydb::Error> = fixture
        .query_candid("measure_schema_application_query", ())
        .expect("schema-application query probe should decode");
    result.expect("schema-application query probe should succeed")
}

fn update_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, icydb::Error> = fixture
        .update_candid("measure_schema_application_update", ())
        .expect("schema-application update probe should decode");
    result.expect("schema-application update probe should succeed")
}

#[test]
fn schema_application_lifecycle_distinguishes_query_rollback_update_and_upgrade() {
    let fixture = install_fixture_canister("sql_perf");

    let first_query = query_probe(&fixture);
    let second_query = query_probe(&fixture);
    eprintln!("schema application install queries: first={first_query:?} second={second_query:?}");
    assert!(first_query.local_instructions > 0);
    assert_eq!(first_query.reconcile_checks, first_query.first_create);
    assert!(first_query.first_create > 0);
    assert_eq!(first_query.exact_match, 0);
    assert_eq!(second_query.reconcile_checks, second_query.first_create);
    assert_eq!(second_query.first_create, first_query.first_create);
    assert_eq!(second_query.exact_match, 0);

    let update = update_probe(&fixture);
    eprintln!("schema application update: {update:?}");
    assert_eq!(update.reconcile_checks, update.first_create);
    assert_eq!(update.first_create, first_query.first_create);
    assert_eq!(update.exact_match, 0);

    let post_update_query = query_probe(&fixture);
    eprintln!("schema application post-update query: {post_update_query:?}");
    assert_eq!(
        post_update_query.reconcile_checks,
        post_update_query.exact_match
    );
    assert_eq!(post_update_query.first_create, 0);
    assert!(post_update_query.exact_match > 0);

    upgrade_fixture_canister(&fixture, "sql_perf");
    let post_upgrade_query = query_probe(&fixture);
    let repeated_post_upgrade_query = query_probe(&fixture);
    eprintln!(
        "schema application upgrade queries: first={post_upgrade_query:?} second={repeated_post_upgrade_query:?}"
    );
    assert_eq!(
        post_upgrade_query.reconcile_checks,
        post_upgrade_query.exact_match
    );
    assert!(post_upgrade_query.exact_match > 0);
    assert_eq!(post_upgrade_query.first_create, 0);
    assert_eq!(
        repeated_post_upgrade_query.reconcile_checks,
        repeated_post_upgrade_query.exact_match,
    );
    assert_eq!(
        repeated_post_upgrade_query.exact_match,
        post_upgrade_query.exact_match,
    );
    assert_eq!(repeated_post_upgrade_query.first_create, 0);

    eprintln!(
        "schema application lifecycle: first_query={} second_query={} update={} post_update={} post_upgrade={} repeated_post_upgrade={}",
        first_query.local_instructions,
        second_query.local_instructions,
        update.local_instructions,
        post_update_query.local_instructions,
        post_upgrade_query.local_instructions,
        repeated_post_upgrade_query.local_instructions,
    );
}
