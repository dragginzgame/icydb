//! SQL audit coverage for indexed scalars through upgrade-driven journal folding.

use super::{
    IndexedBigIntegerAttempt, preparation_measurement_wasm, query_perf, settle_measurement_rounds,
};
use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, ErrorCode, db::sql::SqlQueryResult};
use icydb_testing_integration::{
    MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES, deliver_startup_watchdog_message,
    install_prebuilt_fixture_canister, startup_watchdog_armed, startup_watchdog_perf_snapshot,
};
use serde::Deserialize;

const ALL_ROWS: &str = "SELECT id, signed, unsigned FROM PerfAuditIndexedBigInteger ORDER BY id";

#[derive(CandidType, Deserialize)]
struct RecoveryBacklog {
    admitted_batches: u32,
}

/// A normal committed batch leaves journal debt, not a pending commit marker.
/// This measures its reconstruction, not marker-owned final-effect verification.
#[test]
#[ignore = "manual wasm-release indexed scalar recovery instruction and cycle measurement"]
fn indexed_big_integer_recovery_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    for negative in [false, true] {
        for digits in [20_u32, 300, if negative { 4_092 } else { 4_093 }] {
            for rows in [1_u16, 32] {
                let fixture = install_prebuilt_fixture_canister("sql_perf", module.clone());
                let prepared: Result<(), Error> = fixture
                    .update_candid("prepare_indexed_big_integer_fixture", ())
                    .expect("fixed index setup should decode");
                prepared.expect("accepted indexes should publish");
                settle_measurement_rounds(&fixture);
                queue_recovery_backlog(&fixture);
                let sample: Result<IndexedBigIntegerAttempt, Error> = fixture
                    .update_candid(
                        "measure_indexed_big_integer_write",
                        (negative, digits, rows),
                    )
                    .expect("accepted write should decode");
                let sample = sample.expect("fixture input should be valid");
                assert_eq!(
                    sample.outcome.expect("indexed values should fit"),
                    u32::from(rows)
                );
                let expected = query_perf(&fixture, "query_user_with_perf", ALL_ROWS).result;
                let SqlQueryResult::Projection(projection) = &expected else {
                    panic!("fixture inspection should return projected rows");
                };
                assert_eq!(projection.rows.len(), usize::from(rows));

                // The target is the last batch, behind the fixed small-row
                // backlog, so it still needs folding when upgrade starts.
                // The second upgrade has no debt and must not rebuild it again.
                for debt in [true, false] {
                    fixture
                        .pocket_ic()
                        .upgrade_canister(
                            fixture.canister_id(),
                            module.clone(),
                            candid::encode_args(()).expect("empty upgrade args should encode"),
                            None,
                        )
                        .expect("same-Wasm upgrade should succeed");
                    let pending: Result<SqlQueryResult, Error> = fixture
                        .query_candid("query_user", (ALL_ROWS.to_string(),))
                        .expect("startup admission should decode");
                    assert_eq!(
                        pending
                            .expect_err("ordinary reads must wait for recovery")
                            .code(),
                        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
                    );
                    let cycles_before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
                    for _ in 0..MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
                        deliver_startup_watchdog_message(&fixture);
                        if !startup_watchdog_armed(&fixture) {
                            break;
                        }
                    }
                    let cycles = cycles_before
                        .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
                        .expect("convergence should charge cycles");
                    assert!(!startup_watchdog_armed(&fixture));
                    let recovered = startup_watchdog_perf_snapshot(&fixture);
                    assert_eq!(recovered.retryable_failures, 0);
                    assert_eq!(recovered.invariant_failures, 0);
                    assert_eq!(recovered.work_started, recovered.work_completed);
                    assert_eq!(recovered.succeeded, recovered.work_completed);
                    if debt {
                        assert!(
                            recovered.work_samples >= 3,
                            "replay, folding and verification must run: {recovered:?}"
                        );
                    } else {
                        // The generated driver first restores readiness, then
                        // checks optional cardinality work before quiescing.
                        assert_eq!(
                            recovered.work_samples, 2,
                            "debt-free startup should only restore readiness and quiesce"
                        );
                    }
                    let maximum = recovered
                        .work_maximum_instructions
                        .expect("work must be measured");
                    assert!(maximum > 0);
                    assert!(
                        maximum <= 30_000_000_000,
                        "existing callback instruction envelope"
                    );
                    assert_eq!(
                        query_perf(&fixture, "query_user_with_perf", ALL_ROWS).result,
                        expected
                    );
                    assert_recovered_index(&fixture, negative, usize::from(rows));
                    println!(
                        "indexed_big_integer_recovery_cost negative={negative} digits={digits} rows={rows} debt={debt} write_instructions={} work_samples={} total_instructions={} maximum_instructions={maximum} convergence_cycles={cycles}",
                        sample.instructions,
                        recovered.work_samples,
                        recovered.work_total_instructions,
                    );
                }
            }
        }
    }
}

// A lone small batch can drain while PocketIC completes the upgrade request.
// Reuse the maintained 64-batch fixture, then deliver one bounded tick bundle
// to free write capacity. The target batch is appended after the remaining debt.
fn queue_recovery_backlog(fixture: &StandaloneCanisterFixture) {
    let backlog: Result<RecoveryBacklog, Error> = fixture
        .update_candid("load_convergence_closeout_debt", (100_001_i32,))
        .expect("fixed backlog facts should decode");
    assert_eq!(
        backlog
            .expect("fixed backlog should publish")
            .admitted_batches,
        64
    );
    deliver_startup_watchdog_message(fixture);
    assert!(
        startup_watchdog_armed(fixture),
        "backlog must remain pending"
    );
}

// Check a real secondary range, not just row survival through a primary scan.
fn assert_recovered_index(fixture: &StandaloneCanisterFixture, negative: bool, rows: usize) {
    let (field, predicate) = if negative {
        ("signed", "signed < 0")
    } else {
        ("unsigned", "unsigned > 0")
    };
    let sql =
        format!("SELECT id FROM PerfAuditIndexedBigInteger WHERE {predicate} ORDER BY {field}, id");
    let explained = query_perf(
        fixture,
        "query_user_with_perf",
        &format!("EXPLAIN JSON {sql}"),
    );
    let SqlQueryResult::Explain { explain, .. } = explained.result else {
        panic!("index inspection should return explain JSON");
    };
    let plan: serde_json::Value =
        serde_json::from_str(&explain).expect("explain JSON should decode");
    assert_eq!(
        plan["access_decision"]["selected"]["index_name"],
        format!("perf_big_{field}_idx")
    );
    let SqlQueryResult::Projection(selected) =
        query_perf(fixture, "query_user_with_perf", &sql).result
    else {
        panic!("secondary range should return projected rows");
    };
    assert_eq!(selected.rows.len(), rows);
}
