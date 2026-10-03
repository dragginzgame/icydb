use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, db::sql::SqlQueryResult, metrics::MetricsReport};
use icydb_testing_integration::{
    MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES, deliver_fixture_startup_watchdog,
    deliver_startup_watchdog_message, install_fixture_canister, reset_icydb_fixtures,
    upgrade_fixture_canister,
};
use std::time::Duration;

const QUERY_SQL: &str = "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2";

fn metrics_activity(fixture: &StandaloneCanisterFixture) -> u64 {
    report(fixture)
        .entities()
        .iter()
        .fold(0_u64, |total, entity| total.saturating_add(entity.hits()))
}

fn report(fixture: &StandaloneCanisterFixture) -> MetricsReport {
    let report: Result<MetricsReport, Error> = fixture
        .query_candid("icydb_metrics", ())
        .expect("metrics response should decode");
    report.expect("public metrics endpoint should succeed")
}

fn reset_metrics(fixture: &StandaloneCanisterFixture) {
    let reset: Result<(), Error> = fixture
        .update_candid("icydb_metrics_reset", ())
        .expect("metrics reset should decode");
    reset.expect("controller metrics reset should succeed");
}

fn drain_canonical_debt(fixture: &StandaloneCanisterFixture) -> MetricsReport {
    for _ in 0..MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        let current = report(fixture);
        if tuple(current.journal_debt()) == [0; 3] {
            return current;
        }
        fixture.pocket_ic().advance_time(Duration::from_secs(1));
        deliver_startup_watchdog_message(fixture);
    }
    panic!("canonical debt must drain within the existing delivery bound");
}

const fn tuple(debt: icydb::db::ExactBacklogMeasurement) -> [u64; 3] {
    [
        debt.batch_count(),
        debt.record_count(),
        debt.encoded_batch_bytes(),
    ]
}

fn assert_conservation(before: &MetricsReport, after: &MetricsReport) {
    assert!(before.window_id().is_some());
    assert_eq!(before.window_id(), after.window_id());
    assert!(!before.convergence().overflowed());
    assert!(!after.convergence().overflowed());
    let initial = tuple(before.journal_debt());
    let final_debt = tuple(after.journal_debt());
    let appended_before = tuple(before.convergence().appended());
    let appended_after = tuple(after.convergence().appended());
    let retired_before = tuple(before.convergence().retired());
    let retired_after = tuple(after.convergence().retired());
    for dimension in 0..3 {
        assert_eq!(
            initial[dimension] + appended_after[dimension] - appended_before[dimension],
            final_debt[dimension] + retired_after[dimension] - retired_before[dimension],
        );
    }
}

#[test]
fn canonical_debt_conserves_across_writes_reset_fold_and_upgrade() {
    let fixture = install_fixture_canister("sql");
    reset_icydb_fixtures(&fixture);
    deliver_fixture_startup_watchdog(&fixture);
    reset_metrics(&fixture);
    let empty = report(&fixture);
    assert_eq!(tuple(empty.journal_debt()), [0; 3]);

    reset_icydb_fixtures(&fixture);
    let written = report(&fixture);
    assert!(written.journal_debt().batch_count() > 0);
    assert!(written.journal_debt().record_count() > 0);
    assert!(written.journal_debt().encoded_batch_bytes() > 0);
    assert_conservation(&empty, &written);
    call_generated_query(&fixture);
    let queried = report(&fixture);
    assert_eq!(queried.journal_debt(), written.journal_debt());
    assert_eq!(queried.convergence(), written.convergence());

    reset_metrics(&fixture);
    let reset = report(&fixture);
    assert_eq!(reset.journal_debt(), written.journal_debt());
    assert_ne!(reset.window_id(), written.window_id());
    assert_eq!(tuple(reset.convergence().appended()), [0; 3]);
    assert_eq!(tuple(reset.convergence().retired()), [0; 3]);
    let folded = drain_canonical_debt(&fixture);
    assert_eq!(tuple(folded.journal_debt()), [0; 3]);
    assert_conservation(&reset, &folded);
    assert!(folded.convergence().journal_fold().samples() > 0);
    assert!(folded.convergence().journal_fold().instructions_total() > 0);

    let batches: Result<u32, Error> = fixture
        .update_candid("seed_convergence_metrics_backlog", ())
        .expect("fixed recovery backlog should decode");
    assert_eq!(batches.expect("full backlog should publish"), 64);
    let before_upgrade = report(&fixture);
    assert_eq!(before_upgrade.journal_debt().batch_count(), 64);
    upgrade_fixture_canister(&fixture, "sql");
    let restarted = report(&fixture);
    assert!(
        restarted.journal_debt().batch_count() > 0,
        "bounded backlog must cross heap replacement"
    );
    assert_eq!(tuple(restarted.convergence().appended()), [0; 3]);
    // Heap replacement invalidates comparisons, even if numeric IDs happen to match.
    let recovered = drain_canonical_debt(&fixture);
    assert_eq!(tuple(recovered.journal_debt()), [0; 3]);
    assert_conservation(&restarted, &recovered);
    assert!(recovered.schema_lifecycle().startup_recovery().samples() > 0);
    assert!(recovered.convergence().journal_fold().instructions_total() > 0);
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
