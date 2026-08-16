use std::{io::Write, time::Duration};

use candid::CandidType;
use flate2::{Compression, write::GzEncoder};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, ErrorCode, db::sql::SqlQueryResult};
use icydb_testing_integration::{
    CanisterBuildOptions, build_fixture_canister_wasm_bytes_with_options, install_fixture_canister,
};
use pocket_ic::common::rest::BlobCompression;
use serde::Deserialize;

const JOURNAL_BATCH_MAGIC: &[u8; 4] = b"IJBT";
const JOURNAL_BATCH_VERSION_CURRENT: u8 = 3;
const JOURNAL_BATCH_VERSION_PREDECESSOR: u8 = 2;

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum ScalePayloadProfile {
    #[serde(rename = "not_applicable")]
    NotApplicable,
    #[serde(rename = "blob_cycle_v1")]
    BlobCycleV1,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ScaleFixtureFacts {
    profile_version: u32,
    surface: String,
    fixture_rows: u32,
    zero_match_rows: u32,
    one_match_rows: u32,
    quarter_match_rows: u32,
    all_match_rows: u32,
    payload_profile: ScalePayloadProfile,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct StartupWatchdogPerfSnapshot {
    scheduler_samples: u64,
    scheduler_total_instructions: u64,
    scheduler_maximum_instructions: Option<u64>,
    work_samples: u64,
    work_total_instructions: u64,
    work_latest_instructions: Option<u64>,
    work_maximum_instructions: Option<u64>,
    work_started: u64,
    work_completed: u64,
    succeeded: u64,
    retryable_failures: u64,
    invariant_failures: u64,
}

fn current_sql_perf_wasm() -> Vec<u8> {
    build_fixture_canister_wasm_bytes_with_options("sql_perf", CanisterBuildOptions::default())
}

fn upgrade_with_wasm(fixture: &StandaloneCanisterFixture, wasm: Vec<u8>) {
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            wasm,
            candid::encode_args(()).expect("empty upgrade args should encode"),
            None,
        )
        .expect("current sql-perf Wasm should upgrade");
}

fn advance_startup_watchdog_until_ready(fixture: &StandaloneCanisterFixture) {
    for _ in 0..32 {
        let probe: Result<(), Error> = fixture
            .update_candid("initialize_startup_observation_fixture", ())
            .expect("ordinary startup probe should decode");
        match probe {
            Ok(()) => return,
            Err(error)
                if error.code()
                    == ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
            {
                fixture.pocket_ic().advance_time(Duration::from_secs(1));
                fixture.pocket_ic().tick();
                fixture.pocket_ic().tick();
            }
            Err(error) => panic!("startup driver returned terminal error: {error}"),
        }
    }
    panic!("startup driver should finish within 32 delivered watchdog ticks");
}

fn stable_memory_fingerprint(fixture: &StandaloneCanisterFixture) -> ([u8; 32], usize) {
    let stable = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    (*blake3::hash(&stable).as_bytes(), stable.len())
}

fn query_total_only(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
) -> SqlQueryResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_candid(method, (sql.to_string(),))
        .expect("total-only query should decode");
    result.expect("total-only query should succeed").result
}

fn assert_count(result: SqlQueryResult, expected: u32) {
    match result {
        SqlQueryResult::Count { row_count, .. } => assert_eq!(row_count, expected),
        SqlQueryResult::Projection(rows) => {
            assert_eq!(rows.rendered_rows(), vec![vec![expected.to_string()]]);
        }
        _ => panic!("expected count result"),
    }
}

#[test]
fn complete_batch_recovery_trap_rolls_back_and_the_canonical_watchdog_retries() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_joint_three_index_boundary_fixture", ())
        .expect("three-index fixture facts should decode");
    let loaded = loaded.expect("three-index fixture should load");
    assert_eq!(loaded.fixture_rows, 2_048);

    let wasm = current_sql_perf_wasm();
    upgrade_with_wasm(&fixture, wasm);
    let stable_before_trap = stable_memory_fingerprint(&fixture);

    let trapped =
        fixture.update_candid::<Result<(), Error>, _>("trap_after_complete_startup_recovery", ());
    assert!(
        trapped.is_err(),
        "the audit call must trap only after recovery reaches Ready",
    );
    assert_eq!(
        stable_memory_fingerprint(&fixture),
        stable_before_trap,
        "the trapped recovery message must roll back every stable write",
    );

    let pending: Result<(), Error> = fixture
        .update_candid("initialize_startup_observation_fixture", ())
        .expect("post-trap startup probe should decode");
    assert_eq!(
        pending
            .expect_err("rolled-back recovery must remain pending")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    advance_startup_watchdog_until_ready(&fixture);
    let watchdog: StartupWatchdogPerfSnapshot = fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("watchdog closeout snapshot should decode");
    assert_eq!(watchdog.work_samples, 1);
    assert_eq!(watchdog.succeeded, 1);
    assert!(
        watchdog
            .work_maximum_instructions
            .is_some_and(|instructions| instructions < 40_000_000_000),
    );

    assert_count(
        query_total_only(
            &fixture,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_048,
    );
    assert_count(
        query_total_only(
            &fixture,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE age >= 31 AND age < 35",
        ),
        512,
    );
    let exact_name = query_total_only(
        &fixture,
        "query_user_total_only_perf",
        "SELECT id FROM PerfAuditUser WHERE name = 'scale-group-001' ORDER BY id ASC",
    );
    let SqlQueryResult::Projection(exact_name) = exact_name else {
        panic!("exact-name recovery proof should return a projection");
    };
    assert_eq!(exact_name.row_count, 21);

    let indexes = query_total_only(
        &fixture,
        "query_user_total_only_perf",
        "SHOW INDEXES FROM PerfAuditUser",
    );
    let SqlQueryResult::ShowIndexes { indexes, .. } = indexes else {
        panic!("recovered schema should expose its indexes");
    };
    assert_eq!(indexes.len(), 4);
}

#[test]
fn predecessor_journal_bytes_fail_closed_and_explicit_reinstall_recreates_state() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<(), Error> = fixture
        .update_candid("load_journaled_reentry_probe_fixture", ())
        .expect("journaled predecessor fixture should decode");
    loaded.expect("journaled predecessor fixture should load");

    let mut stable = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    let mut replaced = 0_u32;
    for offset in 0..stable.len().saturating_sub(JOURNAL_BATCH_MAGIC.len()) {
        if stable[offset..].starts_with(JOURNAL_BATCH_MAGIC)
            && stable.get(offset + JOURNAL_BATCH_MAGIC.len())
                == Some(&JOURNAL_BATCH_VERSION_CURRENT)
        {
            stable[offset + JOURNAL_BATCH_MAGIC.len()] = JOURNAL_BATCH_VERSION_PREDECESSOR;
            replaced = replaced.saturating_add(1);
        }
    }
    assert!(
        replaced > 0,
        "fixture should retain a current journal envelope"
    );
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(&stable)
        .expect("predecessor stable memory should compress");
    let stable = encoder
        .finish()
        .expect("predecessor stable-memory gzip should finish");
    fixture
        .pocket_ic()
        .set_stable_memory(fixture.canister_id(), stable, BlobCompression::Gzip);

    let wasm = current_sql_perf_wasm();
    upgrade_with_wasm(&fixture, wasm.clone());
    fixture.pocket_ic().advance_time(Duration::from_secs(1));
    fixture.pocket_ic().tick();
    fixture.pocket_ic().tick();

    let rejected: Result<(), Error> = fixture
        .update_candid("initialize_startup_observation_fixture", ())
        .expect("incompatible-format startup response should decode");
    assert_eq!(
        rejected
            .expect_err("predecessor journal bytes must fail closed")
            .code(),
        ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT,
    );

    fixture
        .pocket_ic()
        .reinstall_canister(
            fixture.canister_id(),
            wasm,
            candid::encode_args(()).expect("empty reinstall args should encode"),
            None,
        )
        .expect("explicit reinstall should recreate the current database");
    advance_startup_watchdog_until_ready(&fixture);
    assert_count(
        query_total_only(
            &fixture,
            "query_journaled_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditJournaledUser",
        ),
        0,
    );
}
