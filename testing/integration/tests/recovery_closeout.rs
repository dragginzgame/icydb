use std::time::Duration;

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode,
    db::{DatabaseStartupState, sql::SqlQueryResult},
    diagnostic::DiagnosticFactTag,
};
use icydb_testing_integration::{
    CanisterBuildOptions, build_fixture_canister_wasm_bytes_with_options, install_fixture_canister,
};
use serde::Deserialize;

const CONVERGENCE_CALLBACK_INSTRUCTION_LIMIT: u64 = 30_000_000_000;
const CONVERGENCE_CALLBACK_WASM_MEMORY_LIMIT: u64 = 768 * 1_024 * 1_024;
const CONVERGENCE_RESIDUAL_MESSAGE_LIMIT: u64 = 42;
const FIRST_CLOSEOUT_ID: i32 = 100_001;
const SECOND_CLOSEOUT_ID: i32 = 200_001;

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

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ConvergenceCloseoutDebtFacts {
    admitted_batches: u32,
    first_id: i32,
    last_admitted_id: i32,
    rejected_id: i32,
    pressure: Error,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct StartupObservationPerfResult {
    state: DatabaseStartupState,
    local_instructions: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConvergenceWatchdogObservation {
    work_samples: u64,
    total_instructions: u64,
    maximum_instructions: u64,
    stable_memory_before: u64,
    stable_memory_after: u64,
    wasm_memory_after: u64,
}

fn report_convergence_observation(label: &str, observation: ConvergenceWatchdogObservation) {
    println!(
        "0.229 closeout {label}: samples={}, total_instructions={}, maximum_instructions={}, stable_before={}, stable_after={}, wasm_after={}",
        observation.work_samples,
        observation.total_instructions,
        observation.maximum_instructions,
        observation.stable_memory_before,
        observation.stable_memory_after,
        observation.wasm_memory_after,
    );
}

fn current_sql_perf_wasm() -> Vec<u8> {
    build_fixture_canister_wasm_bytes_with_options("sql_perf", CanisterBuildOptions::default())
}

fn canister_memory_bytes(fixture: &StandaloneCanisterFixture) -> (u64, u64) {
    fn nat_u64(value: &candid::Nat) -> u64 {
        match value.0.to_u64_digits().as_slice() {
            [] => 0,
            [value] => *value,
            _ => panic!("canister memory bytes should fit u64"),
        }
    }

    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .expect("closeout canister status should be available");
    (
        nat_u64(&status.memory_metrics.wasm_memory_size),
        nat_u64(&status.memory_metrics.stable_memory_size),
    )
}

fn startup_watchdog_snapshot(fixture: &StandaloneCanisterFixture) -> StartupWatchdogPerfSnapshot {
    fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("watchdog closeout snapshot should decode")
}

fn startup_watchdog_armed(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .query_candid("startup_watchdog_armed", ())
        .expect("watchdog scheduling state should decode")
}

fn counter_delta(before: u64, after: u64, label: &str) -> u64 {
    after
        .checked_sub(before)
        .unwrap_or_else(|| panic!("{label} should remain monotonic"))
}

fn run_bounded_convergence_watchdog(
    fixture: &StandaloneCanisterFixture,
) -> ConvergenceWatchdogObservation {
    assert!(
        startup_watchdog_armed(fixture),
        "retained debt should keep the production watchdog armed",
    );
    let before = startup_watchdog_snapshot(fixture);
    let memory_before = canister_memory_bytes(fixture);

    for _ in 0..CONVERGENCE_RESIDUAL_MESSAGE_LIMIT {
        fixture.pocket_ic().advance_time(Duration::from_secs(1));
        fixture.pocket_ic().tick();
        fixture.pocket_ic().tick();
        if !startup_watchdog_armed(fixture) {
            break;
        }
    }

    let after = startup_watchdog_snapshot(fixture);
    let memory_after = canister_memory_bytes(fixture);
    assert!(
        !startup_watchdog_armed(fixture),
        "the production watchdog should stop after draining the complete backlog",
    );
    let scheduler_samples = counter_delta(
        before.scheduler_samples,
        after.scheduler_samples,
        "scheduler samples",
    );
    let work_samples = counter_delta(before.work_samples, after.work_samples, "work samples");
    assert!((1..=CONVERGENCE_RESIDUAL_MESSAGE_LIMIT).contains(&work_samples));
    assert_eq!(scheduler_samples, work_samples);
    assert_eq!(
        counter_delta(before.work_started, after.work_started, "started work"),
        work_samples,
    );
    assert_eq!(
        counter_delta(
            before.work_completed,
            after.work_completed,
            "completed work",
        ),
        work_samples,
    );
    assert_eq!(
        counter_delta(before.succeeded, after.succeeded, "successful work"),
        work_samples,
    );
    assert_eq!(after.retryable_failures, before.retryable_failures);
    assert_eq!(after.invariant_failures, before.invariant_failures);
    let instructions = after
        .work_total_instructions
        .checked_sub(before.work_total_instructions)
        .expect("watchdog instruction total should be monotonic");
    assert!(
        instructions > 0,
        "production convergence should record positive work instructions",
    );
    let maximum_instructions = after
        .work_maximum_instructions
        .expect("production convergence should record a maximum instruction sample");
    assert!(
        maximum_instructions < CONVERGENCE_CALLBACK_INSTRUCTION_LIMIT,
        "every recorded production callback should stay below 30B instructions",
    );
    assert!(
        memory_after.1 >= memory_before.1,
        "canonical materialization must not reduce the canister stable-memory high-water",
    );
    assert!(memory_after.0 < CONVERGENCE_CALLBACK_WASM_MEMORY_LIMIT);

    ConvergenceWatchdogObservation {
        work_samples,
        total_instructions: instructions,
        maximum_instructions,
        stable_memory_before: memory_before.1,
        stable_memory_after: memory_after.1,
        wasm_memory_after: memory_after.0,
    }
}

fn load_convergence_debt(
    fixture: &StandaloneCanisterFixture,
    first_id: i32,
) -> ConvergenceCloseoutDebtFacts {
    let facts: Result<ConvergenceCloseoutDebtFacts, Error> = fixture
        .update_candid("load_convergence_closeout_debt", (first_id,))
        .expect("convergence closeout debt facts should decode");
    let facts = facts.expect("the exact batch ceiling should load");
    assert_eq!(facts.admitted_batches, 38);
    assert_eq!(facts.first_id, first_id);
    assert_eq!(facts.last_admitted_id, first_id + 37);
    assert_eq!(facts.rejected_id, first_id + 38);
    assert_eq!(
        facts.pressure.code(),
        ErrorCode::RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE,
    );
    assert_eq!(
        facts
            .pressure
            .facts()
            .iter()
            .map(|fact| (fact.tag(), fact.value()))
            .collect::<Vec<_>>(),
        vec![
            (DiagnosticFactTag::BacklogResource.raw(), 1),
            (DiagnosticFactTag::CurrentCount.raw(), 38),
            (DiagnosticFactTag::ProposedCount.raw(), 1),
            (DiagnosticFactTag::Limit.raw(), 38),
        ],
    );
    facts
}

fn retry_convergence_row(fixture: &StandaloneCanisterFixture, id: i32) {
    let retried: Result<(), Error> = fixture
        .update_candid("retry_convergence_closeout_row", (id,))
        .expect("convergence retry should decode");
    retried.expect("drained pressure should make the rejected row retryable");
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

fn assert_user_name_id(fixture: &StandaloneCanisterFixture, id: i32, present: bool) {
    let result = query_total_only(
        fixture,
        "query_user_total_only_perf",
        &format!(
            "SELECT id FROM PerfAuditUser WHERE name = 'convergence-closeout-{id}' ORDER BY id ASC"
        ),
    );
    let SqlQueryResult::Projection(rows) = result else {
        panic!("closeout name lookup should return a projection");
    };
    let expected = if present {
        vec![vec![id.to_string()]]
    } else {
        Vec::new()
    };
    assert_eq!(rows.rendered_rows(), expected);
}

fn assert_user_index_count(fixture: &StandaloneCanisterFixture) {
    let indexes = query_total_only(
        fixture,
        "query_user_total_only_perf",
        "SHOW INDEXES FROM PerfAuditUser",
    );
    let SqlQueryResult::ShowIndexes { indexes, .. } = indexes else {
        panic!("closeout schema should expose its indexes");
    };
    assert_eq!(indexes.len(), 4);
}

fn startup_observation(fixture: &StandaloneCanisterFixture) -> StartupObservationPerfResult {
    let observed: Result<StartupObservationPerfResult, icydb::db::StartupFailure> = fixture
        .query_candid("measure_startup_observation", ())
        .expect("startup observation should decode");
    observed.expect("current-form startup observation should remain readable")
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one cross-owner closeout keeps population independence, online retry, upgrade recovery, and final quiescence in causal order"
)]
fn populated_convergence_is_visible_retryable_upgrade_safe_and_quiescent() {
    let cold = install_fixture_canister("sql_perf");
    let populated = install_fixture_canister("sql_perf");

    let loaded: Result<ScaleFixtureFacts, Error> = populated
        .update_candid("load_joint_three_index_boundary_fixture", ())
        .expect("populated history fixture should decode");
    assert_eq!(
        loaded
            .expect("populated history fixture should load")
            .fixture_rows,
        2_048,
    );
    report_convergence_observation(
        "populated-history",
        run_bounded_convergence_watchdog(&populated),
    );
    assert_count(
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_048,
    );

    let cold_debt = load_convergence_debt(&cold, FIRST_CLOSEOUT_ID);
    let populated_debt = load_convergence_debt(&populated, FIRST_CLOSEOUT_ID);
    assert_eq!(cold_debt.pressure, populated_debt.pressure);

    for (fixture, expected_rows) in [(&cold, 38), (&populated, 2_086)] {
        assert_count(
            query_total_only(
                fixture,
                "query_user_total_only_perf",
                "SELECT COUNT(*) FROM PerfAuditUser",
            ),
            expected_rows,
        );
        assert_user_name_id(fixture, cold_debt.first_id, true);
        assert_user_name_id(fixture, cold_debt.last_admitted_id, true);
        assert_user_name_id(fixture, cold_debt.rejected_id, false);
        assert_user_index_count(fixture);
    }

    let cold_residue = run_bounded_convergence_watchdog(&cold);
    let populated_residue = run_bounded_convergence_watchdog(&populated);
    report_convergence_observation("cold-identical-debt", cold_residue);
    report_convergence_observation("populated-identical-debt", populated_residue);
    assert_eq!(cold_residue.work_samples, populated_residue.work_samples);
    assert!(cold_residue.total_instructions > 0);
    assert!(populated_residue.total_instructions > 0);

    for (fixture, expected_rows) in [(&cold, 38), (&populated, 2_086)] {
        assert_count(
            query_total_only(
                fixture,
                "query_user_total_only_perf",
                "SELECT COUNT(*) FROM PerfAuditUser",
            ),
            expected_rows,
        );
        assert_user_name_id(fixture, cold_debt.first_id, true);
        assert_user_name_id(fixture, cold_debt.last_admitted_id, true);
        assert_user_name_id(fixture, cold_debt.rejected_id, false);
    }

    for fixture in [&cold, &populated] {
        retry_convergence_row(fixture, cold_debt.rejected_id);
        assert_user_name_id(fixture, cold_debt.rejected_id, true);
        report_convergence_observation("pressure-retry", run_bounded_convergence_watchdog(fixture));
    }
    assert_count(
        query_total_only(
            &cold,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        39,
    );
    assert_count(
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_087,
    );

    let upgrade_debt = load_convergence_debt(&populated, SECOND_CLOSEOUT_ID);
    assert_user_name_id(&populated, upgrade_debt.first_id, true);
    assert_user_name_id(&populated, upgrade_debt.last_admitted_id, true);
    assert_user_name_id(&populated, upgrade_debt.rejected_id, false);
    let memory_before_upgrade = canister_memory_bytes(&populated);
    upgrade_with_wasm(&populated, current_sql_perf_wasm());
    let memory_after_upgrade = canister_memory_bytes(&populated);
    assert_eq!(memory_after_upgrade.1, memory_before_upgrade.1);
    assert_eq!(
        startup_observation(&populated).state,
        DatabaseStartupState::Recovering,
    );
    assert!(startup_watchdog_armed(&populated));

    let pending: Result<SqlTotalOnlyPerfResult, Error> = populated
        .query_candid(
            "query_user_total_only_perf",
            ("SELECT COUNT(*) FROM PerfAuditUser".to_string(),),
        )
        .expect("recovering query result should decode");
    assert_eq!(
        pending
            .expect_err("ordinary queries must remain gated during upgrade recovery")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    report_convergence_observation(
        "post-upgrade-debt",
        run_bounded_convergence_watchdog(&populated),
    );
    assert_eq!(
        startup_observation(&populated).state,
        DatabaseStartupState::Ready,
    );
    assert_count(
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_125,
    );
    assert_user_name_id(&populated, upgrade_debt.first_id, true);
    assert_user_name_id(&populated, upgrade_debt.last_admitted_id, true);
    assert_user_name_id(&populated, upgrade_debt.rejected_id, false);
    assert_user_index_count(&populated);

    retry_convergence_row(&populated, upgrade_debt.rejected_id);
    assert_user_name_id(&populated, upgrade_debt.rejected_id, true);
    report_convergence_observation(
        "post-upgrade-retry",
        run_bounded_convergence_watchdog(&populated),
    );
    assert_count(
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_126,
    );
    assert_user_index_count(&populated);
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
fn explicit_reinstall_recreates_clean_current_state() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<(), Error> = fixture
        .update_candid("load_journaled_reentry_probe_fixture", ())
        .expect("journaled fixture should decode");
    loaded.expect("journaled fixture should load");

    let wasm = current_sql_perf_wasm();
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
