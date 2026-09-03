//! Total-only instruction coverage for the SQL performance actor.

use candid::CandidType;
use icydb::{Error, db::sql::SqlQueryResult};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::Deserialize;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

fn query_perf(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    method: &str,
    sql: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid(method, (sql.to_string(),))
        .unwrap_or_else(|error| panic!("{method} should decode: {error}"));

    result.unwrap_or_else(|error| panic!("{method} should execute: {error}"))
}

#[test]
fn sql_perf_reports_total_instructions_for_query_warmup_and_loop_execution() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);
    let sql = "SELECT COUNT(*) FROM PerfAuditUser";

    let cold = query_perf(&fixture, "query_user_with_perf", sql);
    let warmed: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (sql.to_string(),))
        .expect("warm SQL measurement should decode");
    let warmed = warmed.expect("warm SQL measurement should execute");
    let looped: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("query_user_loop_with_perf", (sql.to_string(), 8_u32))
        .expect("looped SQL measurement should decode");
    let looped = looped.expect("looped SQL measurement should execute");

    assert_eq!(cold.result, warmed.result);
    assert_eq!(cold.result, looped.result);
    assert!(cold.instructions > 0);
    assert!(warmed.instructions > 0);
    assert!(looped.instructions > 0);
}

#[test]
fn sql_perf_total_measurement_covers_distinct_entity_surfaces() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);

    for (method, sql) in [
        (
            "query_heap_user_with_perf",
            "SELECT COUNT(*) FROM PerfAuditHeapUser",
        ),
        (
            "query_journaled_user_with_perf",
            "SELECT COUNT(*) FROM PerfAuditJournaledUser",
        ),
        (
            "query_token_with_perf",
            "SELECT COUNT(*) FROM PerfAuditToken",
        ),
    ] {
        let sample = query_perf(&fixture, method, sql);
        assert!(sample.instructions > 0, "{method}");
    }
}

#[test]
fn sql_blob_scalar_query_executes_equality_and_octet_length() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);

    let sample = query_perf(
        &fixture,
        "query_blob_with_perf",
        "SELECT id, label, bucket, OCTET_LENGTH(chunk) FROM PerfAuditBlob \
         WHERE bucket = 10 ORDER BY bucket ASC, label ASC, id ASC LIMIT 3",
    );
    let SqlQueryResult::Projection(output) = sample.result else {
        panic!("blob scalar query should return a projection");
    };

    assert_eq!(output.row_count, 3);
    assert!(output.rows.iter().all(|row| row.len() == 4));
}
