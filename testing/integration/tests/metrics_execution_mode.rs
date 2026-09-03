use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, db::sql::SqlQueryResult, metrics::MetricsReport};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};

const QUERY_SQL: &str = "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2";

fn metrics_activity(fixture: &StandaloneCanisterFixture) -> u64 {
    let report: Result<MetricsReport, Error> = fixture
        .query_candid("icydb_metrics", ())
        .expect("metrics response should decode");
    let report = report.expect("public metrics endpoint should succeed");
    report
        .entities()
        .iter()
        .fold(0_u64, |total, entity| total.saturating_add(entity.hits()))
}

fn call_generated_query(fixture: &StandaloneCanisterFixture) {
    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("icydb_query", (QUERY_SQL.to_string(),))
        .expect("generated SQL query response should decode");
    result.expect("generated SQL query should succeed");
}

fn trap_metrics_query(fixture: &StandaloneCanisterFixture) {
    let call = fixture.query_candid::<Result<(), Error>, _>("audit_metrics_query_trap", ());
    assert!(call.is_err(), "the audit query must trap intentionally");
}

#[test]
fn query_execution_records_nothing_and_cannot_contaminate_later_methods() {
    let fixture = install_fixture_canister("sql");
    reset_icydb_fixtures(&fixture);

    let reset: Result<(), Error> = fixture
        .update_candid("icydb_metrics_reset", ())
        .expect("metrics reset response should decode");
    reset.expect("controller metrics reset should succeed");

    call_generated_query(&fixture);
    assert_eq!(metrics_activity(&fixture), 0);

    trap_metrics_query(&fixture);
    call_generated_query(&fixture);
    assert_eq!(
        metrics_activity(&fixture),
        0,
        "a trapped query must not contaminate the following generated query",
    );

    trap_metrics_query(&fixture);
    let update: Result<(), Error> = fixture
        .update_candid("icydb_fixtures_reset", ())
        .expect("generated fixture-reset response should decode");
    update.expect("update after a trapped query should succeed");
    assert!(
        metrics_activity(&fixture) > 0,
        "the later update must retain ordinary durable global metrics",
    );
}
