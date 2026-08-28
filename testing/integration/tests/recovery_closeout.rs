use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode,
    db::{
        DatabaseStartupState, LiveQueryPageOutput, SqlQueryExecutionAttribution,
        sql::SqlQueryResult,
    },
    diagnostic::DiagnosticFactTag,
};
use icydb_testing_integration::{
    CanisterBuildOptions, MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES,
    build_fixture_canister_wasm_bytes_with_options, deliver_startup_watchdog_message,
    install_fixture_canister,
};
use serde::Deserialize;

const CONVERGENCE_CALLBACK_INSTRUCTION_LIMIT: u64 = 30_000_000_000;
const CONVERGENCE_CALLBACK_WASM_MEMORY_LIMIT: u64 = 768 * 1_024 * 1_024;
const CONVERGENCE_RESIDUAL_MESSAGE_LIMIT: u64 = MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES as u64;
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
struct JointFanoutFixtureFacts {
    rows: u32,
    secondary_indexes_per_row: u32,
    load_local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct LiveQueryPagePerfOutput {
    page: LiveQueryPageOutput,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct CardinalityIndexPublicationFacts {
    rows_scanned: u64,
    index_keys_written: u64,
    local_instructions: u64,
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
    elapsed_nanos: u64,
    total_instructions: u64,
    maximum_instructions: u64,
    stable_memory_before: u64,
    stable_memory_after: u64,
    wasm_memory_after: u64,
}

fn report_convergence_observation(label: &str, observation: ConvergenceWatchdogObservation) {
    println!(
        "recovery closeout {label}: samples={}, elapsed_nanos={}, total_instructions={}, maximum_instructions={}, stable_before={}, stable_after={}, wasm_after={}",
        observation.work_samples,
        observation.elapsed_nanos,
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
    let time_before = fixture.pocket_ic().get_time();

    for _ in 0..CONVERGENCE_RESIDUAL_MESSAGE_LIMIT {
        deliver_startup_watchdog_message(fixture);
        if !startup_watchdog_armed(fixture) {
            break;
        }
    }

    let after = startup_watchdog_snapshot(fixture);
    let memory_after = canister_memory_bytes(fixture);
    let elapsed_nanos = fixture
        .pocket_ic()
        .get_time()
        .as_nanos_since_unix_epoch()
        .checked_sub(time_before.as_nanos_since_unix_epoch())
        .expect("PocketIC time should remain monotonic");
    assert!(
        elapsed_nanos < 1_000_000_000,
        "healthy bounded convergence must complete before the one-second retry cadence",
    );
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
        elapsed_nanos,
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
    assert_eq!(facts.admitted_batches, 64);
    assert_eq!(facts.first_id, first_id);
    assert_eq!(facts.last_admitted_id, first_id + 63);
    assert_eq!(facts.rejected_id, first_id + 64);
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
            (DiagnosticFactTag::CurrentCount.raw(), 64),
            (DiagnosticFactTag::ProposedCount.raw(), 1),
            (DiagnosticFactTag::Limit.raw(), 64),
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
    for delivered in 0..=MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        let probe: Result<(), Error> = fixture
            .update_candid("initialize_startup_observation_fixture", ())
            .expect("ordinary startup probe should decode");
        match probe {
            Ok(()) => return,
            Err(error)
                if error.code()
                    == ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
            {
                if delivered == MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
                    break;
                }
                deliver_startup_watchdog_message(fixture);
            }
            Err(error) => panic!("startup driver returned terminal error: {error}"),
        }
    }
    panic!("startup driver should finish within its frozen residual delivery bound");
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

fn user_count_with_perf(fixture: &StandaloneCanisterFixture, sql: &str) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("query_user_with_perf", (sql.to_string(),))
        .expect("attributed user count should decode");
    result.expect("attributed user count should succeed")
}

fn warm_user_count_with_perf(fixture: &StandaloneCanisterFixture, sql: &str) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (sql.to_string(),))
        .expect("persistent attributed user count should decode");
    result.expect("persistent attributed user count should succeed")
}

fn token_count_with_perf(fixture: &StandaloneCanisterFixture, sql: &str) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("query_token_with_perf", (sql.to_string(),))
        .expect("attributed token count should decode");
    result.expect("attributed token count should succeed")
}

fn assert_metadata_backed_count(sample: SqlQueryPerfResult, expected: u32) {
    assert_count(sample.result, expected);
    assert_eq!(sample.attribution.store_get_calls, 0);
    assert_eq!(sample.attribution.index_store_entry_reads, 0);
    assert_eq!(
        sample
            .attribution
            .scalar_aggregate
            .as_ref()
            .and_then(|aggregate| aggregate.sink_mode.as_deref()),
        Some("IndexPrefixCardinality"),
    );
}

fn assert_projection_row_count(result: &SqlQueryResult, expected: usize) {
    let SqlQueryResult::Projection(rows) = result else {
        panic!("cardinality tie-break fixture should return projected rows");
    };
    assert_eq!(rows.rendered_rows().len(), expected);
}

fn explain_text(result: SqlQueryResult) -> String {
    let SqlQueryResult::Explain { explain, .. } = result else {
        panic!("cardinality tie-break fixture should return explain text");
    };
    explain
}

fn assert_within_cardinality_hot_path_gate(candidate: u64, baseline: u64, label: &str) {
    let allowance = (baseline / 20).max(2_000_000);
    assert!(
        candidate <= baseline.saturating_add(allowance),
        "{label} must stay within max(5%, 2M instructions): baseline={baseline}, candidate={candidate}",
    );
}

fn maximum_cardinality_probe_sql() -> String {
    const VALUE_BYTES: usize = 4_000;

    let value = |ordinal: u8| {
        let prefix = format!("p{ordinal:02}");
        format!(
            "{prefix}{}",
            "x".repeat(VALUE_BYTES.saturating_sub(prefix.len()))
        )
    };

    let values = (0_u8..16)
        .map(|ordinal| format!("'{}'", value(ordinal)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT id FROM PerfAuditMaxCardinalityProbes WHERE a IN ({values}) \
         AND b = '{}' AND c = '{}' \
         ORDER BY id ASC LIMIT 1",
        value(16),
        value(17),
    )
}

fn mutate_cardinality_index(
    fixture: &StandaloneCanisterFixture,
    present: bool,
    expected_version: u64,
    next_version: u64,
) -> CardinalityIndexPublicationFacts {
    let result: Result<CardinalityIndexPublicationFacts, Error> = fixture
        .update_candid(
            "mutate_cardinality_closeout_index",
            (present, expected_version, next_version),
        )
        .expect("cardinality closeout DDL facts should decode");
    result.expect("cardinality closeout DDL should publish")
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
    reason = "one causal real-canister scenario proves mid-build upgrade, fallback, Ready consumption, live maintenance, and two-slot reuse"
)]
fn populated_cardinality_build_upgrade_maintenance_and_slot_reuse_close_cleanly() {
    const fn unavailable_fallback_ceiling(predecessor: u64) -> u64 {
        predecessor.saturating_add(predecessor / 20)
    }

    const ACTIVE_COUNT_SQL: &str = "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true";
    const FALLBACK_COLD_PREDECESSOR: u64 = 116_170_059;
    const FALLBACK_WARM_PREDECESSOR: u64 = 53_841_143;
    const REBUILD_RETAINED_GROWTH_LIMIT: u64 = 4 * 1_024 * 1_024;

    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_joint_three_index_boundary_fixture", ())
        .expect("populated cardinality fixture should decode");
    assert_eq!(
        loaded
            .expect("populated cardinality fixture should load")
            .fixture_rows,
        2_048,
    );
    report_convergence_observation(
        "cardinality-base-fold",
        run_bounded_convergence_watchdog(&fixture),
    );
    assert_metadata_backed_count(
        user_count_with_perf(
            &fixture,
            "SELECT COUNT(*) FROM PerfAuditUser WHERE name = 'scale-group-001'",
        ),
        21,
    );

    let created = mutate_cardinality_index(&fixture, true, 1, 2);
    assert_eq!(created.rows_scanned, 2_048);
    assert_eq!(created.index_keys_written, 2_048);
    assert!(created.local_instructions < 40_000_000_000);
    for _ in 0..4 {
        deliver_startup_watchdog_message(&fixture);
        if startup_observation(&fixture).state == DatabaseStartupState::Ready {
            break;
        }
    }
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Ready,
        "the canonical journal must drain before the mid-build upgrade",
    );
    assert!(startup_watchdog_armed(&fixture));

    upgrade_with_wasm(&fixture, current_sql_perf_wasm());
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Recovering,
        "the existing generated-schema handoff should remain the upgrade gate",
    );
    advance_startup_watchdog_until_ready(&fixture);
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Ready,
        "the incomplete optional cardinality build must not gate ordinary readiness",
    );
    assert!(startup_watchdog_armed(&fixture));
    let conservative = warm_user_count_with_perf(&fixture, ACTIVE_COUNT_SQL);
    assert_count(conservative.result.clone(), 512);
    assert!(
        conservative.attribution.index_store_entry_reads > 0,
        "a reopened Building generation must retain the conservative index scan",
    );
    assert_eq!(
        conservative.attribution.cache.sql_compiled_command_misses, 1,
        "the first persistent fallback must install one compiled command",
    );
    assert_eq!(
        conservative.attribution.cache.shared_query_plan_misses, 1,
        "the first persistent fallback must build through the shared plan cache",
    );
    assert!(
        conservative.attribution.total_local_instructions
            <= unavailable_fallback_ceiling(FALLBACK_COLD_PREDECESSOR),
        "cold unavailable fallback exceeded its frozen five-percent regression gate",
    );
    let warm_conservative = warm_user_count_with_perf(&fixture, ACTIVE_COUNT_SQL);
    assert_eq!(warm_conservative.result, conservative.result);
    assert_eq!(
        warm_conservative.attribution.index_store_entry_reads,
        conservative.attribution.index_store_entry_reads,
        "shared fallback reuse must preserve physical work",
    );
    assert_eq!(
        warm_conservative
            .attribution
            .cache
            .sql_compiled_command_hits,
        1,
        "the warm fallback must reuse the command carrying the exact target",
    );
    assert_eq!(
        warm_conservative.attribution.cache.shared_query_plan_hits, 1,
        "the warm fallback plan must come from the existing shared cache",
    );
    assert_eq!(
        warm_conservative.attribution.cache.shared_query_plan_misses, 0,
        "the exact command entry must not force a second fallback preparation",
    );
    assert!(
        warm_conservative.attribution.total_local_instructions
            <= unavailable_fallback_ceiling(FALLBACK_WARM_PREDECESSOR),
        "warm unavailable fallback exceeded its frozen five-percent regression gate",
    );
    println!(
        "0.240 unavailable exact-target fallback: cold={} warm={} index_entries={}",
        conservative.attribution.total_local_instructions,
        warm_conservative.attribution.total_local_instructions,
        conservative.attribution.index_store_entry_reads,
    );

    report_convergence_observation(
        "cardinality-mid-build-upgrade",
        run_bounded_convergence_watchdog(&fixture),
    );
    assert_metadata_backed_count(user_count_with_perf(&fixture, ACTIVE_COUNT_SQL), 512);
    let stable_after_first_ready = canister_memory_bytes(&fixture).1;

    retry_convergence_row(&fixture, 900_001);
    assert_metadata_backed_count(user_count_with_perf(&fixture, ACTIVE_COUNT_SQL), 513);
    report_convergence_observation(
        "cardinality-live-delta-fold",
        run_bounded_convergence_watchdog(&fixture),
    );
    assert_metadata_backed_count(user_count_with_perf(&fixture, ACTIVE_COUNT_SQL), 513);

    let dropped = mutate_cardinality_index(&fixture, false, 2, 3);
    assert!(dropped.local_instructions < 40_000_000_000);
    report_convergence_observation(
        "cardinality-alternate-slot",
        run_bounded_convergence_watchdog(&fixture),
    );
    let stable_after_drop = canister_memory_bytes(&fixture).1;

    let recreated = mutate_cardinality_index(&fixture, true, 3, 4);
    assert_eq!(recreated.rows_scanned, 2_049);
    assert_eq!(recreated.index_keys_written, 2_049);
    assert!(recreated.local_instructions < 40_000_000_000);
    report_convergence_observation(
        "cardinality-reused-slot",
        run_bounded_convergence_watchdog(&fixture),
    );
    let stable_after_recreate = canister_memory_bytes(&fixture).1;
    assert!(stable_after_drop >= stable_after_first_ready);
    assert!(stable_after_recreate >= stable_after_drop);
    assert!(
        stable_after_recreate.saturating_sub(stable_after_drop) <= REBUILD_RETAINED_GROWTH_LIMIT,
        "reusing a previously filled count slot must retain bounded total stable growth",
    );
    assert_metadata_backed_count(user_count_with_perf(&fixture, ACTIVE_COUNT_SQL), 513);

    println!(
        "0.230 cardinality closeout: create_instructions={} drop_instructions={} recreate_instructions={} stable_first_ready={} stable_after_drop={} stable_after_recreate={}",
        created.local_instructions,
        dropped.local_instructions,
        recreated.local_instructions,
        stable_after_first_ready,
        stable_after_drop,
        stable_after_recreate,
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one causal IC proof keeps unavailable fallback, upgrade, Ready publication, pinned continuation, and exact selection comparable"
)]
fn exact_cardinality_tiebreak_improves_selective_work_and_survives_upgrade() {
    const SELECTIVE_SQL: &str = "SELECT id FROM PerfAuditCardinalityTie \
        WHERE common = 0 AND rare = 20 ORDER BY id ASC LIMIT 200";
    const SELECTIVE_EXPLAIN_SQL: &str = "EXPLAIN EXECUTION VERBOSE \
        SELECT id FROM PerfAuditCardinalityTie WHERE common = 0 \
        AND rare = 20 ORDER BY id ASC LIMIT 200";

    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_cardinality_tiebreak_fixture", ())
        .expect("selective cardinality fixture should decode");
    assert_eq!(
        loaded.expect("selective cardinality fixture should load"),
        10_001,
    );
    report_convergence_observation(
        "cardinality-tiebreak-base-ready",
        run_bounded_convergence_watchdog(&fixture),
    );

    let created = mutate_cardinality_index(&fixture, true, 1, 2);
    assert_eq!(created.rows_scanned, 0);
    for _ in 0..4 {
        deliver_startup_watchdog_message(&fixture);
        if startup_observation(&fixture).state == DatabaseStartupState::Ready {
            break;
        }
    }
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Ready,
    );
    assert!(startup_watchdog_armed(&fixture));

    let stable_before_upgrade = stable_memory_fingerprint(&fixture);
    upgrade_with_wasm(&fixture, current_sql_perf_wasm());
    let stable_after_upgrade = stable_memory_fingerprint(&fixture);
    assert_eq!(
        stable_after_upgrade.1, stable_before_upgrade.1,
        "the 0.236 heap-only plan state must add no stable upgrade allocation",
    );
    advance_startup_watchdog_until_ready(&fixture);
    assert!(startup_watchdog_armed(&fixture));

    let fallback: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (SELECTIVE_SQL.to_string(),))
        .expect("Building fallback sample should decode");
    let fallback = fallback.expect("Building evidence should preserve query admission");
    assert_projection_row_count(&fallback.result, 2);
    let unchanged_fallback: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (SELECTIVE_SQL.to_string(),))
        .expect("unchanged Building fallback sample should decode");
    let unchanged_fallback =
        unchanged_fallback.expect("unchanged Building fallback should remain admitted");
    assert_eq!(unchanged_fallback.result, fallback.result);
    assert_eq!(
        unchanged_fallback.attribution.index_store_entry_reads,
        fallback.attribution.index_store_entry_reads,
    );
    assert_within_cardinality_hot_path_gate(
        unchanged_fallback.attribution.total_local_instructions,
        fallback.attribution.total_local_instructions,
        "unchanged unavailable fallback",
    );
    let fallback_explain = explain_text(query_total_only(
        &fixture,
        "query_user_total_only_perf",
        SELECTIVE_EXPLAIN_SQL,
    ));
    assert!(
        fallback_explain.contains("cardinality_evidence: unavailable"),
        "{fallback_explain}",
    );
    assert!(
        fallback_explain.contains("idx_perf_audit_cardinality_tie__common"),
        "{fallback_explain}",
    );
    assert!(fallback.attribution.index_store_entry_reads >= 10_000);

    let first: Result<LiveQueryPagePerfOutput, Error> = fixture
        .query_candid(
            "query_cardinality_tiebreak_live_page_with_perf",
            (None::<String>,),
        )
        .expect("fallback cursor page should decode");
    let first = first.expect("fallback cursor page should execute");
    let first_instructions = first.instructions;
    let mut continuation = first.page.continuation;
    let mut cursor_rows = first.page.row_count;
    assert!(
        continuation.is_some(),
        "the common-prefix route should require a bounded continuation",
    );

    let before_ready: Result<LiveQueryPagePerfOutput, Error> = fixture
        .query_candid(
            "query_cardinality_tiebreak_live_page_with_perf",
            (Some(
                continuation
                    .clone()
                    .expect("the pre-Ready pinned page must have a cursor"),
            ),),
        )
        .expect("pre-Ready pinned cursor page should decode");
    let before_ready = before_ready.expect("pre-Ready pinned cursor page should execute");
    assert!(before_ready.page.continuation.is_none());

    report_convergence_observation(
        "cardinality-tiebreak-ready-publication",
        run_bounded_convergence_watchdog(&fixture),
    );

    let after_ready: Result<LiveQueryPagePerfOutput, Error> = fixture
        .query_candid(
            "query_cardinality_tiebreak_live_page_with_perf",
            (Some(
                continuation
                    .take()
                    .expect("the post-Ready pinned page must have a cursor"),
            ),),
        )
        .expect("post-Ready pinned cursor page should decode");
    let after_ready = after_ready.expect("post-Ready pinned cursor page should execute");
    assert_eq!(after_ready.page, before_ready.page);
    assert_within_cardinality_hot_path_gate(
        after_ready.instructions,
        before_ready.instructions,
        "pinned continuation across Ready publication",
    );
    cursor_rows = cursor_rows.saturating_add(after_ready.page.row_count);
    assert_eq!(cursor_rows, 2);
    assert!(after_ready.page.continuation.is_none());

    let exact: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (SELECTIVE_SQL.to_string(),))
        .expect("Ready exact sample should decode");
    let exact = exact.expect("Ready exact selection should execute");
    assert_eq!(exact.result, fallback.result);
    assert!(exact.attribution.index_store_entry_reads <= 4);
    assert!(
        exact
            .attribution
            .index_store_entry_reads
            .saturating_mul(1_000)
            < fallback.attribution.index_store_entry_reads,
    );
    assert!(
        exact.attribution.total_local_instructions < fallback.attribution.total_local_instructions,
        "the production-shaped selective route must repay its bounded planning work",
    );
    let warm_exact: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (SELECTIVE_SQL.to_string(),))
        .expect("warm exact-selected sample should decode");
    let warm_exact = warm_exact.expect("warm exact-selected plan should execute");
    assert_eq!(warm_exact.result, exact.result);
    assert_eq!(warm_exact.attribution.index_store_entry_reads, 2);
    assert_within_cardinality_hot_path_gate(
        warm_exact.attribution.total_local_instructions,
        exact.attribution.total_local_instructions,
        "warm exact-selected plan",
    );

    let explain = explain_text(query_total_only(
        &fixture,
        "query_user_total_only_perf",
        SELECTIVE_EXPLAIN_SQL,
    ));
    assert!(explain.contains("exact_cardinality_tiebreak"), "{explain}");
    assert!(
        explain.contains("cardinality_evidence: exact_at_selection"),
        "{explain}",
    );
    assert!(explain.contains("exact_prefix_entries: 10001"), "{explain}");
    assert!(explain.contains("exact_prefix_entries: 2"), "{explain}");

    println!(
        "0.236 selective tie-break: fallback_entries={} exact_entries={} fallback_instructions={} unchanged_fallback_instructions={} exact_instructions={} warm_exact_instructions={} first_cursor_instructions={} pinned_before_ready_instructions={} pinned_after_ready_instructions={} stable_before_upgrade={} stable_after_upgrade={}",
        fallback.attribution.index_store_entry_reads,
        exact.attribution.index_store_entry_reads,
        fallback.attribution.total_local_instructions,
        unchanged_fallback.attribution.total_local_instructions,
        exact.attribution.total_local_instructions,
        warm_exact.attribution.total_local_instructions,
        first_instructions,
        before_ready.instructions,
        after_ready.instructions,
        stable_before_upgrade.1,
        stable_after_upgrade.1,
    );
}

#[test]
fn exact_cardinality_tiebreak_maximum_ic_shapes_remain_bounded() {
    const MAXIMUM_CANDIDATE_SQL: &str = "SELECT id FROM PerfAuditMaxFanout \
        WHERE a = 0 AND b = 1 AND c = 2 AND d = 3 AND e = 4 \
        AND f = 5 AND g = 6 AND h = 7 AND i = 8 ORDER BY id ASC LIMIT 1";
    const IC_QUERY_INSTRUCTION_CEILING: u64 = 10_000_000_000;

    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<JointFanoutFixtureFacts, Error> = fixture
        .update_candid("load_joint_fanout_boundary_fixture", ())
        .expect("maximum cardinality fixture should decode");
    let loaded = loaded.expect("maximum cardinality fixture should load");
    assert_eq!((loaded.rows, loaded.secondary_indexes_per_row), (240, 64));
    report_convergence_observation(
        "cardinality-tiebreak-maximum-ready",
        run_bounded_convergence_watchdog(&fixture),
    );

    let maximum_candidates = user_count_with_perf(&fixture, MAXIMUM_CANDIDATE_SQL);
    assert_projection_row_count(&maximum_candidates.result, 1);
    assert!(maximum_candidates.attribution.total_local_instructions < IC_QUERY_INSTRUCTION_CEILING,);
    let candidate_explain = user_count_with_perf(
        &fixture,
        format!("EXPLAIN EXECUTION VERBOSE {MAXIMUM_CANDIDATE_SQL}").as_str(),
    );
    let candidate_explain = explain_text(candidate_explain.result);
    assert_eq!(
        candidate_explain.matches("exact_prefix_entries:").count(),
        64
    );
    assert!(
        candidate_explain.contains("cardinality_evidence: exact_at_selection"),
        "{candidate_explain}",
    );

    let loaded: Result<u32, Error> = fixture
        .update_candid("load_cardinality_probe_boundary_fixture", ())
        .expect("maximum probe fixture should decode");
    assert_eq!(loaded.expect("maximum probe fixture should load"), 1);
    report_convergence_observation(
        "cardinality-tiebreak-maximum-probes-ready",
        run_bounded_convergence_watchdog(&fixture),
    );

    let maximum_probe_sql = maximum_cardinality_probe_sql();
    let maximum_probes = user_count_with_perf(&fixture, &maximum_probe_sql);
    assert_projection_row_count(&maximum_probes.result, 1);
    assert!(maximum_probes.attribution.total_local_instructions < IC_QUERY_INSTRUCTION_CEILING);
    let probe_explain = user_count_with_perf(
        &fixture,
        format!("EXPLAIN EXECUTION VERBOSE {maximum_probe_sql}").as_str(),
    );
    let probe_explain = explain_text(probe_explain.result);
    assert_eq!(
        probe_explain.matches("exact_prefix_entries:").count(),
        16,
        "{probe_explain}",
    );
    assert!(
        probe_explain.contains("cardinality_evidence: exact_at_selection"),
        "{probe_explain}",
    );

    println!(
        "0.236 maximum tie-break: candidates=64 candidate_instructions={} probes=256 probe_instructions={} probe_value_bytes=3072000",
        maximum_candidates.attribution.total_local_instructions,
        maximum_probes.attribution.total_local_instructions,
    );
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

    for (fixture, expected_rows) in [(&cold, 64), (&populated, 2_112)] {
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

    for (fixture, expected_rows) in [(&cold, 64), (&populated, 2_112)] {
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
        65,
    );
    assert_count(
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        2_113,
    );

    let upgrade_debt = load_convergence_debt(&populated, SECOND_CLOSEOUT_ID);
    assert_user_name_id(&populated, upgrade_debt.first_id, true);
    assert_user_name_id(&populated, upgrade_debt.last_admitted_id, true);
    assert_user_name_id(&populated, upgrade_debt.rejected_id, false);
    let extrema_before_upgrade = [
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT MIN(id) FROM PerfAuditUser",
        ),
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT MAX(id) FROM PerfAuditUser",
        ),
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT MAX(age) FROM PerfAuditUser",
        ),
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT MAX(age) FROM PerfAuditUser WHERE age < 43",
        ),
    ];
    let ordered_endpoints_before_upgrade = [
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 1",
        ),
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT age FROM PerfAuditUser ORDER BY age DESC, id DESC LIMIT 1",
        ),
        query_total_only(
            &populated,
            "query_user_total_only_perf",
            "SELECT id, name FROM PerfAuditUser WHERE age < 43 ORDER BY age DESC, id DESC LIMIT 3",
        ),
    ];
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
        2_177,
    );
    assert_user_name_id(&populated, upgrade_debt.first_id, true);
    assert_user_name_id(&populated, upgrade_debt.last_admitted_id, true);
    assert_user_name_id(&populated, upgrade_debt.rejected_id, false);
    assert_eq!(
        [
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT MIN(id) FROM PerfAuditUser",
            ),
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT MAX(id) FROM PerfAuditUser",
            ),
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT MAX(age) FROM PerfAuditUser",
            ),
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT MAX(age) FROM PerfAuditUser WHERE age < 43",
            ),
        ],
        extrema_before_upgrade,
        "indexed scalar extrema must retain their accepted-schema result across recovery",
    );
    assert_eq!(
        [
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 1",
            ),
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT age FROM PerfAuditUser ORDER BY age DESC, id DESC LIMIT 1",
            ),
            query_total_only(
                &populated,
                "query_user_total_only_perf",
                "SELECT id, name FROM PerfAuditUser WHERE age < 43 ORDER BY age DESC, id DESC LIMIT 3",
            ),
        ],
        ordered_endpoints_before_upgrade,
        "secondary ordered limits must retain their accepted-schema result across recovery",
    );
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
        2_178,
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
fn count_prefix_cardinality_cap_survives_same_wasm_recovery_without_query_allocation() {
    const COUNT_SQL: &str = "\
SELECT COUNT(*) FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', \
'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', \
'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')";

    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_token_scale_fixture", (2_048_u32,))
        .expect("token recovery fixture facts should decode");
    let loaded = loaded.expect("token recovery fixture should load");
    assert_eq!(loaded.quarter_match_rows, 512);
    assert_metadata_backed_count(token_count_with_perf(&fixture, COUNT_SQL), 512);

    let stable_before_upgrade = canister_memory_bytes(&fixture).1;
    upgrade_with_wasm(&fixture, current_sql_perf_wasm());
    assert_eq!(canister_memory_bytes(&fixture).1, stable_before_upgrade);
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Recovering,
    );

    advance_startup_watchdog_until_ready(&fixture);
    assert_eq!(
        startup_observation(&fixture).state,
        DatabaseStartupState::Ready,
    );
    let stable_after_recovery = canister_memory_bytes(&fixture).1;
    assert_metadata_backed_count(token_count_with_perf(&fixture, COUNT_SQL), 512);
    assert_eq!(canister_memory_bytes(&fixture).1, stable_after_recovery);
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
    let group_seek = user_count_with_perf(
        &fixture,
        "SELECT DISTINCT age FROM PerfAuditUser ORDER BY age ASC LIMIT 3",
    );
    let SqlQueryResult::Projection(rows) = group_seek.result else {
        panic!("post-reinstall ordered DISTINCT should return a projection");
    };
    assert!(rows.rows.is_empty());
    assert_eq!(group_seek.attribution.store_get_calls, 0);
    assert_eq!(group_seek.attribution.index_store_entry_reads, 0);
    assert_eq!(group_seek.attribution.index_store_range_scan_calls, 1);
}
