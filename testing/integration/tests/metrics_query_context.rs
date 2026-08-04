use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, db::sql::SqlQueryPerfResult, metrics::CompactMetricsReport};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};

const QUERY_SQL: &str = "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2";

fn metrics_activity(fixture: &StandaloneCanisterFixture) -> u64 {
    let report: Result<CompactMetricsReport, Error> = fixture
        .query_candid("icydb_metrics", (None::<u64>,))
        .expect("compact metrics response should decode");
    let report = report.expect("public compact metrics endpoint should succeed");
    let global = report
        .counters()
        .expect("unfiltered report should include global counters")
        .metrics()
        .iter()
        .fold(0_u64, |total, metric| total.saturating_add(metric.value()));
    report
        .entity_counters()
        .iter()
        .flat_map(icydb::metrics::CompactEntityMetrics::metrics)
        .fold(global, |total, metric| total.saturating_add(metric.value()))
}

fn call_generated_query(fixture: &StandaloneCanisterFixture) {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("icydb_query", (QUERY_SQL.to_string(),))
        .expect("generated SQL query response should decode");
    result.expect("generated SQL query should succeed");
}

fn trap_context_query(fixture: &StandaloneCanisterFixture) {
    let call = fixture.query_candid::<Result<(), Error>, _>("audit_query_metrics_context_trap", ());
    assert!(call.is_err(), "the audit query must trap intentionally");
}

#[test]
fn query_metrics_context_suppresses_global_state_and_cannot_contaminate_later_methods() {
    let fixture = install_fixture_canister("sql");
    reset_icydb_fixtures(&fixture);

    let reset: Result<(), Error> = fixture
        .update_candid("icydb_metrics_reset", ())
        .expect("metrics reset response should decode");
    reset.expect("controller metrics reset should succeed");

    call_generated_query(&fixture);
    assert_eq!(metrics_activity(&fixture), 0);

    trap_context_query(&fixture);
    call_generated_query(&fixture);
    assert_eq!(
        metrics_activity(&fixture),
        0,
        "a trapped query must not contaminate the following generated query",
    );

    trap_context_query(&fixture);
    let update: Result<(), Error> = fixture
        .update_candid("icydb_fixtures_reset", ())
        .expect("generated fixture-reset response should decode");
    update.expect("update after a trapped query should succeed");
    assert!(
        metrics_activity(&fixture) > 0,
        "the later update must retain ordinary durable global metrics",
    );
}
