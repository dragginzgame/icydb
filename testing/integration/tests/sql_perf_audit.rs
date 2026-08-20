#![allow(
    clippy::significant_drop_tightening,
    reason = "each test intentionally retains its exclusive pooled fixture lease for its full scope"
)]

use candid::CandidType;
use ic_testkit::pic::{
    CachedStandaloneCanisterFixtureGuard, CachedStandaloneCanisterFixturePool,
    StandaloneCanisterFixture,
};
use icydb::{
    Error,
    db::{
        DeepIntegrityPageStatus, IntegrityCheckError, IntegrityCheckResult, IntegrityJobError,
        IntegrityJobReceipt, IntegrityPhase, IntegrityTerminalOutcome, MutationJobAdvanceReceipt,
        MutationJobError, MutationJobPhase, MutationJobState, MutationJobStatus,
        MutationJobTargetFailureReason, ProgressJobFamily, ProgressJobInventory, SqlDescribeOutput,
        SqlIntegrityError, SqlQueryExecutionAttribution, SqlShowColumnsOutput,
        SqlStructuralWorkAttribution, sql::SqlQueryResult,
    },
    diagnostic::{
        DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
        DiagnosticFactTag, RuntimeBoundaryCode,
    },
    metrics::EventReport,
};
use icydb_testing_integration::{
    MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES, deliver_startup_watchdog_message,
    durable_mutation_job_contract::{
        DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING, DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING,
        DURABLE_INVENTORY_INSTRUCTION_REVIEW_CEILING, DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT,
        DURABLE_START_INSTRUCTION_REVIEW_CEILING, DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING,
    },
    install_fixture_canister, reset_icydb_fixtures, upgrade_fixture_canister,
};
use serde::Deserialize;
use std::time::Duration;

// Mirror the dedicated perf-audit query envelope so the testkit can decode the
// query result plus the compile/execute instruction split from the canister.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ReadTotalOnlyPerfResult {
    row_count: u32,
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
struct JointFanoutFixtureFacts {
    rows: u32,
    secondary_indexes_per_row: u32,
    load_local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct PromotionIndexPublicationFacts {
    rows_scanned: u64,
    index_keys_written: u64,
    local_instructions: u64,
}

const PROMOTION_INDEX_FIXTURE_ROWS: u32 = 65_536;
const PROMOTION_INDEX_LOAD_PAGE_ROWS: u32 = 4_096;
// Leave room below R_max for the 1,025-record accepted-index publication
// while retaining enough live rows to exercise mixed canonical/overlay staging.
const PROMOTION_INDEX_LIVE_SETUP_ROWS: u32 = 15_000;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct StorageWritePerfResult {
    first_insert_local_instructions: u64,
    steady_insert_avg_local_instructions: u64,
    steady_update_avg_local_instructions: u64,
    steady_delete_avg_local_instructions: u64,
    write_then_read_back_local_instructions: u64,
    read_back_rows: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ConstraintActivationPerfResult {
    no_check: StorageWritePerfResult,
    add_check_local_instructions: u64,
    add_check_rows_scanned: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlWriteMaterializationPerfResult {
    local_instructions: [u64; 4],
    rows: [u32; 4],
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobForwardPerfResult {
    start_local_instructions: u64,
    forward_local_instructions: Vec<u64>,
    replay_local_instructions: u64,
    forward_keys_scanned: u64,
    rows_updated: u64,
    forward_keys_scanned_per_step: Vec<u64>,
    rows_updated_per_step: Vec<u64>,
    committed_sequence: u64,
    replay_matches: bool,
    zero_candidate_keys_scanned: u64,
    zero_candidate_rows_updated: u64,
    zero_candidate_sequence: u64,
    stale_request_preserved_sequence: bool,
    operation_timestamp_groups: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobVerifyResult {
    first_verify_keys_scanned: u64,
    first_verify_local_instructions: u64,
    verify_replay_local_instructions: u64,
    unrelated_verify_keys_scanned: u64,
    unrelated_verify_local_instructions: u64,
    unrelated_preserved_verify: bool,
    drift_restart_keys_scanned: u64,
    drift_restart_local_instructions: u64,
    stable_verify_local_instructions: Vec<u64>,
    verify_restarts_total: u64,
    restarted_forward_rows_updated: u64,
    completed_sequence: u64,
    state_local_instructions: u64,
    terminal_replay_local_instructions: u64,
    acknowledgement_local_instructions: u64,
    replay: MutationJobReplayEvidence,
    acknowledgement: MutationJobAcknowledgementEvidence,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobReplayEvidence {
    verify_matches: bool,
    terminal_matches: bool,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobAcknowledgementEvidence {
    stale_rejected: bool,
    terminal_acknowledged: bool,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobStartPerfResult {
    state: MutationJobState,
    local_instructions: u64,
    target_rows_changed: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobAdvancePerfResult {
    receipt: MutationJobAdvanceReceipt,
    local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ProgressJobInventoryPerfResult {
    inventory: ProgressJobInventory,
    local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationJobCancellationPerfResult {
    local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct IntegritySqlPerfResult {
    result: IntegrityCheckResult,
    local_instructions: u64,
}

/// Candid mirror of the audit canister's deterministic scale-payload profile.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum ScalePayloadProfile {
    /// The selected scale fixture carries no blob payload.
    #[serde(rename = "not_applicable")]
    NotApplicable,
    /// The selected scale fixture uses the maintained deterministic blob cycle.
    #[serde(rename = "blob_cycle_v1")]
    BlobCycleV1,
}

/// Candid mirror of the audit canister's realized scale-fixture facts.
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

#[derive(Debug)]
struct IntegrityCleanPerfObservation {
    quick_instructions: u64,
    deep_page_instructions: Vec<u64>,
    deep_page_phases: Vec<IntegrityPhase>,
    quick_response_bytes: usize,
    max_deep_response_bytes: usize,
    memory_before: (u64, u64),
    memory_after_quick: (u64, u64),
    memory_after_deep: (u64, u64),
}

#[derive(Debug)]
struct Patch1RecoveryObservation {
    watchdog: StartupWatchdogPerfSnapshot,
    memory_before: (u64, u64),
    memory_after: (u64, u64),
}

const SQL_WRITE_MATERIALIZATION_METRICS: [&str; 4] = [
    "update count",
    "update returning",
    "delete count",
    "delete returning",
];
const SQL_WRITE_MATERIALIZATION_BUDGET: u64 = 750_000_000;
// The first mutation includes cold accepted-schema and constraint-program
// preparation; steady-state mutations retain their narrower budgets below.
const STORAGE_FIRST_INSERT_BUDGET: u64 = 35_000_000;
const INTEGRITY_QUICK_OPERATION_BUDGET: u64 = 2_000_000;
const INTEGRITY_COMPLEX_QUICK_OPERATION_BUDGET: u64 = 35_000_000;
const INTEGRITY_RELATION_COLD_QUICK_OPERATION_BUDGET: u64 = 250_000_000;
const INTEGRITY_DEEP_PAGE_BUDGET: u64 = 30_000_000;
const INTEGRITY_RESPONSE_BYTE_BUDGET: usize = 512 * 1024;
const INTEGRITY_RETAINED_MEMORY_GROWTH_BUDGET: u64 = 16 * 1024 * 1024;
const INTEGRITY_RELATION_RECOVERY_BUDGET: u64 = 15_000_000_000;
const SQL_PERF_FIXTURE_POOL_CAPACITY: usize = 4;

static SQL_PERF_FIXTURE_POOL: CachedStandaloneCanisterFixturePool<SQL_PERF_FIXTURE_POOL_CAPACITY> =
    CachedStandaloneCanisterFixturePool::new();

#[derive(Clone, Copy, Debug)]
enum SqlPerfSurface {
    Account,
    Blob,
    Token,
    User,
}

impl SqlPerfSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::Token => "token",
            Self::User => "user",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SqlPerfScenario {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlPerfOutcome {
    result_kind: &'static str,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct SqlPerfScenarioSample {
    scenario_key: String,
    compile_local_instructions: u64,
    compile_phases: SqlPerfCompilePhases,
    execute_local_instructions: u64,
    grouped_count_row_materialization_local_instructions: u64,
    grouped_count_group_lookup_local_instructions: u64,
    hybrid_covering_path_hits: u64,
    hybrid_covering_index_field_accesses: u64,
    hybrid_covering_row_field_accesses: u64,
    data_store_get_calls: u64,
    index_store_get_calls: u64,
    index_store_range_scan_calls: u64,
    index_store_entry_reads: u64,
    structural_work: SqlStructuralWorkAttribution,
    sql_compiled_command_cache_hits: u64,
    sql_compiled_command_cache_misses: u64,
    shared_query_plan_cache_hits: u64,
    shared_query_plan_cache_misses: u64,
    local_instructions: u64,
    outcome: SqlPerfOutcome,
}

const fn scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        sql,
        query_loop_count: 1,
    }
}

const fn repeat_scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        sql,
        query_loop_count,
    }
}

fn install_sql_perf_canister_fixture() -> CachedStandaloneCanisterFixtureGuard<'static> {
    // The audit scenarios mutate isolated canister state but do not depend on
    // simulator time or topology, so a restored post-install snapshot is the
    // narrowest complete reset boundary between tests.
    SQL_PERF_FIXTURE_POOL
        .acquire(|| install_fixture_canister("sql_perf"))
        .unwrap_or_else(|error| panic!("SQL perf fixture pool should restore cleanly: {error}"))
        .0
}

fn reset_sql_perf_fixtures(fixture: &StandaloneCanisterFixture) {
    // Keep each measurement independent of retained work from the preceding
    // scenario, then materialize the reset fixture before sampling.
    drain_online_watchdog_until_quiescent(fixture);
    reset_icydb_fixtures(fixture);
    drain_online_watchdog_until_quiescent(fixture);
}

fn reset_sql_perf_metrics(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_candid("icydb_metrics_reset", ())
        .expect("metrics reset response should decode");
    result.expect("controller metrics reset should succeed");
}

fn extended_sql_perf_metrics(fixture: &StandaloneCanisterFixture) -> EventReport {
    let result: Result<EventReport, Error> = fixture
        .query_candid("icydb_metrics_extended", (None::<u64>,))
        .expect("extended metrics response should decode");
    result.expect("public extended metrics endpoint should succeed")
}

fn startup_watchdog_perf_snapshot(
    fixture: &StandaloneCanisterFixture,
) -> StartupWatchdogPerfSnapshot {
    fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("startup watchdog performance snapshot should decode")
}

fn startup_watchdog_armed(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .query_candid("startup_watchdog_armed", ())
        .expect("startup watchdog scheduling state should decode")
}

fn drain_online_watchdog_until_quiescent(fixture: &StandaloneCanisterFixture) {
    for delivered in 0..=MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        if !startup_watchdog_armed(fixture) {
            let current = startup_watchdog_perf_snapshot(fixture);
            assert_eq!(current.work_started, current.work_completed);
            assert_eq!(current.work_completed, current.succeeded);
            assert_eq!(current.retryable_failures, 0);
            assert_eq!(current.invariant_failures, 0);
            return;
        }
        if delivered == MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
            break;
        }
        deliver_startup_watchdog_message(fixture);
    }
    panic!("online watchdog should quiesce within its frozen residual delivery bound");
}

fn measure_integrity_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> IntegritySqlPerfResult {
    let result: Result<IntegritySqlPerfResult, SqlIntegrityError> = fixture
        .update_candid("measure_integrity_sql_perf", (sql.to_string(),))
        .expect("integrity perf result should decode");

    result.expect("integrity perf operation should succeed")
}

fn integrity_page_phase(receipt: &IntegrityJobReceipt) -> IntegrityPhase {
    let IntegrityJobReceipt::Page(page) = receipt else {
        panic!("clean Deep measurement should return only page receipts");
    };

    page.phase()
}

fn activate_journaled_user_perf_check(fixture: &StandaloneCanisterFixture) {
    let activation: Result<ConstraintActivationPerfResult, Error> = fixture
        .update_candid("measure_journaled_user_constraint_write_perf", ())
        .expect("constraint activation perf result should decode");
    activation.expect("constraint activation should publish");

    let validation: Result<(), Error> = fixture
        .update_candid("validate_journaled_user_perf_check", ())
        .expect("constraint validation result should decode");
    validation.expect("constraint validation should promote");
}

fn load_user_scale_integrity_fixture(fixture: &StandaloneCanisterFixture, row_count: u32) {
    let result: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_user_scale_fixture", (row_count,))
        .expect("user scale fixture facts should decode");
    let facts = result.expect("user scale fixture should load");

    assert_eq!(facts.surface, "user");
    assert_eq!(facts.fixture_rows, row_count);
}

fn load_relation_integrity_fixture(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_candid("load_relation_integrity_fixture", ())
        .expect("relation integrity fixture result should decode");

    result.expect("relation integrity fixture should load");
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
        .expect("audit canister status should be available");
    let wasm = nat_u64(&status.memory_metrics.wasm_memory_size);
    let stable = nat_u64(&status.memory_metrics.stable_memory_size);

    (wasm, stable)
}

fn measure_clean_integrity_run(
    fixture: &StandaloneCanisterFixture,
    entity: &str,
    submission_key: &str,
) -> IntegrityCleanPerfObservation {
    const MAX_DEEP_STEPS: usize = 64;

    let memory_before = canister_memory_bytes(fixture);
    let quick_sql = format!("CHECK INTEGRITY {entity} QUICK");
    let quick = measure_integrity_sql(fixture, quick_sql.as_str());
    let quick_response_bytes = candid::encode_one(&quick.result)
        .expect("Quick result should encode")
        .len();
    let memory_after_quick = canister_memory_bytes(fixture);
    let start_sql = format!("CHECK INTEGRITY {entity} DEEP START '{submission_key}'");
    let start = measure_integrity_sql(fixture, start_sql.as_str());
    let IntegrityCheckResult::Deep(start_receipt) = start.result else {
        panic!("Deep start should return a Deep receipt");
    };
    let job_id = start_receipt.job_id();
    let mut sequence = start_receipt.page_sequence();
    let mut deep_page_instructions = vec![start.local_instructions];
    let mut deep_page_phases = vec![integrity_page_phase(&start_receipt)];
    let mut max_deep_response_bytes = candid::encode_one(&start_receipt)
        .expect("Deep start receipt should encode")
        .len();
    let mut terminal = None;

    for _ in 0..MAX_DEEP_STEPS {
        let sql = format!(
            "CHECK INTEGRITY DEEP CONTINUE '{}' AFTER {sequence}",
            job_id.to_hex(),
        );
        let sample = measure_integrity_sql(fixture, sql.as_str());
        let IntegrityCheckResult::Deep(receipt) = sample.result else {
            panic!("Deep continuation should return a Deep receipt");
        };
        sequence = receipt.page_sequence();
        deep_page_instructions.push(sample.local_instructions);
        deep_page_phases.push(integrity_page_phase(&receipt));
        max_deep_response_bytes = max_deep_response_bytes.max(
            candid::encode_one(&receipt)
                .expect("Deep receipt should encode")
                .len(),
        );
        if matches!(
            receipt,
            IntegrityJobReceipt::Page(ref page)
                if matches!(page.status(), DeepIntegrityPageStatus::Terminal(_))
        ) {
            terminal = Some(receipt);
            break;
        }
    }

    let memory_after_deep = canister_memory_bytes(fixture);
    let terminal = terminal.expect("small fixture should reach a bounded terminal page");
    assert!(matches!(
        terminal,
        IntegrityJobReceipt::Page(ref page)
            if page.status()
                == &DeepIntegrityPageStatus::Terminal(
                    IntegrityTerminalOutcome::DeepCompleteClean
                )
    ));

    IntegrityCleanPerfObservation {
        quick_instructions: quick.local_instructions,
        deep_page_instructions,
        deep_page_phases,
        quick_response_bytes,
        max_deep_response_bytes,
        memory_before,
        memory_after_quick,
        memory_after_deep,
    }
}

fn assert_clean_integrity_perf_stays_bounded(
    observation: &IntegrityCleanPerfObservation,
    quick_instruction_budget: u64,
) {
    assert!(
        (1..=quick_instruction_budget).contains(&observation.quick_instructions),
        "Quick integrity should stay within its instruction budget, got {} > {}",
        observation.quick_instructions,
        quick_instruction_budget,
    );
    assert!(
        observation
            .deep_page_instructions
            .iter()
            .all(|instructions| (1..=INTEGRITY_DEEP_PAGE_BUDGET).contains(instructions))
    );
    assert!(observation.quick_response_bytes <= INTEGRITY_RESPONSE_BYTE_BUDGET);
    assert!(observation.max_deep_response_bytes <= INTEGRITY_RESPONSE_BYTE_BUDGET);
    let retained_memory_growth = observation
        .memory_after_deep
        .0
        .saturating_sub(observation.memory_before.0)
        .saturating_add(
            observation
                .memory_after_deep
                .1
                .saturating_sub(observation.memory_before.1),
        );
    assert!(retained_memory_growth <= INTEGRITY_RETAINED_MEMORY_GROWTH_BUDGET);
}

fn load_journaled_reentry_probe_fixture(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_candid("load_journaled_reentry_probe_fixture", ())
        .expect("journaled reentry probe fixture load should decode");

    result.expect("journaled reentry probe fixture load should succeed");
}

fn load_journal_tail_integrity_fixture(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_candid("load_journal_tail_integrity_fixture", ())
        .expect("journal-tail integrity fixture result should decode");

    result.expect("journal-tail integrity fixture should load");
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
    query_loop_count: usize,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User if query_loop_count == 1 => fixture
            .query_candid("query_user_with_perf", (sql.to_string(),))
            .expect("query_user_with_perf should decode"),
        SqlPerfSurface::User => fixture
            .query_candid(
                "query_user_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_user_loop_with_perf should decode"),
        SqlPerfSurface::Account if query_loop_count == 1 => fixture
            .query_candid("query_account_with_perf", (sql.to_string(),))
            .expect("query_account_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .query_candid(
                "query_account_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_account_loop_with_perf should decode"),
        SqlPerfSurface::Blob if query_loop_count == 1 => fixture
            .query_candid("query_blob_with_perf", (sql.to_string(),))
            .expect("query_blob_with_perf should decode"),
        SqlPerfSurface::Blob => fixture
            .query_candid(
                "query_blob_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_blob_loop_with_perf should decode"),
        SqlPerfSurface::Token if query_loop_count == 1 => fixture
            .query_candid("query_token_with_perf", (sql.to_string(),))
            .expect("query_token_with_perf should decode"),
        SqlPerfSurface::Token => fixture
            .query_candid(
                "query_token_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_token_loop_with_perf should decode"),
    }
}

fn error_fact(error: &Error, tag: DiagnosticFactTag) -> Option<u64> {
    error
        .facts()
        .iter()
        .find(|fact| fact.tag() == tag.raw())
        .map(icydb::DiagnosticFact::value)
}

fn warm_query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User => fixture
            .update_candid("warm_user_query_with_perf", (sql.to_string(),))
            .expect("warm_user_query_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .update_candid("warm_account_query_with_perf", (sql.to_string(),))
            .expect("warm_account_query_with_perf should decode"),
        SqlPerfSurface::Blob => fixture
            .update_candid("warm_blob_query_with_perf", (sql.to_string(),))
            .expect("warm_blob_query_with_perf should decode"),
        SqlPerfSurface::Token => fixture
            .update_candid("warm_token_query_with_perf", (sql.to_string(),))
            .expect("warm_token_query_with_perf should decode"),
    }
}

fn summarize_perf_outcome(result: &SqlQueryResult) -> SqlPerfOutcome {
    match result {
        SqlQueryResult::Count { entity, row_count } => SqlPerfOutcome {
            result_kind: "count",
            entity: entity.clone(),
            row_count: usize::try_from(*row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Projection(rows) => SqlPerfOutcome {
            result_kind: "projection",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Grouped(rows) => SqlPerfOutcome {
            result_kind: "grouped",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Explain { entity, .. } => SqlPerfOutcome {
            result_kind: "explain",
            entity: entity.clone(),
            row_count: 1,
        },
        SqlQueryResult::Describe(output) => match output {
            SqlDescribeOutput::Compact { entity, columns } => SqlPerfOutcome {
                result_kind: "describe",
                entity: entity.clone(),
                row_count: columns.len(),
            },
            SqlDescribeOutput::Verbose { description } => SqlPerfOutcome {
                result_kind: "describe",
                entity: description.entity_name().to_string(),
                row_count: description.fields().len(),
            },
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => SqlPerfOutcome {
            result_kind: "show_indexes",
            entity: entity.clone(),
            row_count: indexes.len(),
        },
        SqlQueryResult::ShowConstraints {
            entity,
            constraints,
        } => SqlPerfOutcome {
            result_kind: "show_constraints",
            entity: entity.clone(),
            row_count: constraints.len(),
        },
        SqlQueryResult::ShowColumns(output) => match output {
            SqlShowColumnsOutput::Compact { entity, columns } => SqlPerfOutcome {
                result_kind: "show_columns",
                entity: entity.clone(),
                row_count: columns.len(),
            },
            SqlShowColumnsOutput::Verbose { entity, columns } => SqlPerfOutcome {
                result_kind: "show_columns",
                entity: entity.clone(),
                row_count: columns.len(),
            },
        },
        SqlQueryResult::ShowRelations(output) => SqlPerfOutcome {
            result_kind: "show_relations",
            entity: output.entity().to_string(),
            row_count: output.relations().len(),
        },
        SqlQueryResult::ShowEntities { entities, .. } => SqlPerfOutcome {
            result_kind: "show_entities",
            entity: String::new(),
            row_count: entities.len(),
        },
        SqlQueryResult::ShowStores { stores, .. } => SqlPerfOutcome {
            result_kind: "show_stores",
            entity: String::new(),
            row_count: stores.len(),
        },
        SqlQueryResult::ShowMemory { memory } => SqlPerfOutcome {
            result_kind: "show_memory",
            entity: String::new(),
            row_count: memory.len(),
        },
        SqlQueryResult::Ddl { entity, .. } => SqlPerfOutcome {
            result_kind: "icydb_ddl",
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn rendered_projection_rows(result: SqlQueryResult) -> Vec<Vec<String>> {
    match result {
        SqlQueryResult::Projection(rows) => rows.rendered_rows(),
        other => panic!("expected projection payload, got {other:?}"),
    }
}

// SqlPerfCompilePhases keeps exact compile attribution together for the
// focused route diagnostics that remain in this target. Repeated sampling and
// statistical aggregation belong to the P2 confirmation harness.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlPerfCompilePhases {
    cache_key: u64,
    cache_lookup: u64,
    parse: u64,
    tokenize: u64,
    select: u64,
    expr: u64,
    predicate: u64,
    aggregate_check: u64,
    prepare: u64,
    lower: u64,
    bind: u64,
    cache_insert: u64,
}

impl SqlPerfCompilePhases {
    const fn from_attribution(attribution: &SqlQueryExecutionAttribution) -> Self {
        let compile = &attribution.compile;

        Self {
            cache_key: compile.cache_key_local_instructions,
            cache_lookup: compile.cache_lookup_local_instructions,
            parse: compile.parse_local_instructions,
            tokenize: compile.parse_tokenize_local_instructions,
            select: compile.parse_select_local_instructions,
            expr: compile.parse_expr_local_instructions,
            predicate: compile.parse_predicate_local_instructions,
            aggregate_check: compile.aggregate_lane_check_local_instructions,
            prepare: compile.prepare_local_instructions,
            lower: compile.lower_local_instructions,
            bind: compile.bind_local_instructions,
            cache_insert: compile.cache_insert_local_instructions,
        }
    }
}

fn build_sql_perf_scenario_sample(
    scenario: SqlPerfScenario,
    sample: SqlQueryPerfResult,
) -> SqlPerfScenarioSample {
    let attribution = &sample.attribution;
    let grouped_count = attribution.grouped.map(|grouped| grouped.count);
    let hybrid = attribution.hybrid_covering;

    SqlPerfScenarioSample {
        scenario_key: scenario.scenario_key.to_string(),
        compile_local_instructions: attribution.compile_local_instructions,
        compile_phases: SqlPerfCompilePhases::from_attribution(attribution),
        execute_local_instructions: attribution.execute_local_instructions,
        grouped_count_row_materialization_local_instructions: grouped_count
            .map_or(0, |count| count.row_materialization_local_instructions),
        grouped_count_group_lookup_local_instructions: grouped_count
            .map_or(0, |count| count.group_lookup_local_instructions),
        hybrid_covering_path_hits: hybrid.map_or(0, |hybrid| hybrid.path_hits),
        hybrid_covering_index_field_accesses: hybrid
            .map_or(0, |hybrid| hybrid.index_field_accesses),
        hybrid_covering_row_field_accesses: hybrid.map_or(0, |hybrid| hybrid.row_field_accesses),
        data_store_get_calls: attribution.store_get_calls,
        index_store_get_calls: attribution.index_store_get_calls,
        index_store_range_scan_calls: attribution.index_store_range_scan_calls,
        index_store_entry_reads: attribution.index_store_entry_reads,
        structural_work: attribution.structural_work,
        sql_compiled_command_cache_hits: attribution.cache.sql_compiled_command_hits,
        sql_compiled_command_cache_misses: attribution.cache.sql_compiled_command_misses,
        shared_query_plan_cache_hits: attribution.cache.shared_query_plan_hits,
        shared_query_plan_cache_misses: attribution.cache.shared_query_plan_misses,
        local_instructions: attribution.total_local_instructions,
        outcome: summarize_perf_outcome(&sample.result),
    }
}

// sample_perf_scenario captures one exact focused result. P2 owns repeated
// cold/warm sampling, stability checks, and summary statistics.
fn sample_perf_scenario(
    fixture: &StandaloneCanisterFixture,
    scenario: SqlPerfScenario,
) -> SqlPerfScenarioSample {
    let sample = query_surface_with_perf(
        fixture,
        scenario.surface,
        scenario.sql,
        scenario.query_loop_count,
    )
    .unwrap_or_else(|err| {
        panic!(
            "perf scenario '{}' on '{}' should succeed: {err}",
            scenario.scenario_key,
            scenario.surface.label(),
        )
    });

    build_sql_perf_scenario_sample(scenario, sample)
}

const TOKEN_BRANCH_SET_PAGE_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_NONCOVERED_PAGE_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL: &str = "\
SELECT id, stage \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
  AND stage != 'Review' \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_COUNT_SQL: &str = "\
SELECT COUNT(*) \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review')";

const TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL: &str = "\
SELECT COUNT(*) \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Draft', 'Review')";

const TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_NONCOVERED_PAGE_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_WIDE_NONCOVERED_PAGE_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_OVERCAP_PRUNED_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
  AND stage NOT IN ('Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07', 'Missing08', 'Missing09', 'Missing10', 'Missing11', 'Missing12', 'Missing13', 'Missing14', 'Missing15', 'Missing16', 'Missing17', 'Missing18', 'Missing19', 'Missing20', 'Missing21', 'Missing22', 'Missing23', 'Missing24', 'Missing25', 'Missing26', 'Missing27', 'Missing28', 'Missing29', 'Missing30') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_COLLECTION_SPARSE_IN_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id IN ('01KV5N439P0000000000000000', 'missing-collection-000', 'missing-collection-001', 'missing-collection-002', 'missing-collection-003', 'missing-collection-004', 'missing-collection-005', 'missing-collection-006', 'missing-collection-007', 'missing-collection-008', 'missing-collection-009', 'missing-collection-010', 'missing-collection-011', 'missing-collection-012', 'missing-collection-013', 'missing-collection-014', 'missing-collection-015', 'missing-collection-016', 'missing-collection-017', 'missing-collection-018', 'missing-collection-019', 'missing-collection-020', 'missing-collection-021', 'missing-collection-022', 'missing-collection-023', 'missing-collection-024', 'missing-collection-025', 'missing-collection-026', 'missing-collection-027', 'missing-collection-028', 'missing-collection-029', 'missing-collection-030') \
ORDER BY id ASC \
LIMIT 50";
fn token_branch_set_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_NONCOVERED_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.count",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_COUNT_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.duplicate_count",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_NONCOVERED_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_NONCOVERED_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.overcap_pruned.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_PRUNED_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.large_in_fallback.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_id.sparse_in.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_COLLECTION_SPARSE_IN_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL,
        ),
    ]
}

fn repeated_query_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit1.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            10,
        ),
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit2.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        repeat_scenario(
            "repeat.user.name.lower.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs10",
            SqlPerfSurface::User,
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
        repeat_scenario(
            "repeat.user.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.distinct.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT DISTINCT age FROM PerfAuditUser ORDER BY age ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.case_where.order_id.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser WHERE CASE WHEN age >= 30 THEN TRUE ELSE active END ORDER BY id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.age_plus_rank.direct_order.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, age FROM PerfAuditUser ORDER BY age + rank ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.no_order.runs10",
            SqlPerfSurface::User,
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.case_sum.having_alias.order.limit5.runs10",
            SqlPerfSurface::User,
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            10,
        ),
        repeat_scenario(
            "repeat.account.active.lower.order_handle.asc.limit3.runs10",
            SqlPerfSurface::Account,
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
            10,
        ),
    ]
}

fn print_branch_set_perf_sample(label: &str, sample: &SqlPerfScenarioSample) {
    let scenario = sample.scenario_key.as_str();
    let rows = sample.outcome.row_count;
    let compile = sample.compile_local_instructions;
    let compile_phases = &sample.compile_phases;
    let compile_key = compile_phases.cache_key;
    let compile_lookup = compile_phases.cache_lookup;
    let parse = compile_phases.parse;
    let tokenize = compile_phases.tokenize;
    let select = compile_phases.select;
    let expr = compile_phases.expr;
    let predicate = compile_phases.predicate;
    let aggregate_check = compile_phases.aggregate_check;
    let prepare = compile_phases.prepare;
    let lower = compile_phases.lower;
    let bind = compile_phases.bind;
    let cache_insert = compile_phases.cache_insert;
    let execute = sample.execute_local_instructions;
    let total = sample.local_instructions;
    let data_gets = sample.data_store_get_calls;
    let index_gets = sample.index_store_get_calls;
    let index_ranges = sample.index_store_range_scan_calls;
    let index_entries = sample.index_store_entry_reads;
    let grouped_count_rows = sample.grouped_count_row_materialization_local_instructions;
    let grouped_count_lookup = sample.grouped_count_group_lookup_local_instructions;
    let hybrid_hits = sample.hybrid_covering_path_hits;
    let hybrid_index_fields = sample.hybrid_covering_index_field_accesses;
    let hybrid_row_fields = sample.hybrid_covering_row_field_accesses;
    let sql_hits = sample.sql_compiled_command_cache_hits;
    let sql_misses = sample.sql_compiled_command_cache_misses;
    let shared_hits = sample.shared_query_plan_cache_hits;
    let shared_misses = sample.shared_query_plan_cache_misses;
    let structural = sample.structural_work;

    println!(
        "branch-set perf {label}: scenario={scenario} rows={rows} compile={compile} compile_key={compile_key} compile_lookup={compile_lookup} parse={parse} tokenize={tokenize} select={select} expr={expr} predicate={predicate} agg_check={aggregate_check} prepare={prepare} lower={lower} bind={bind} cache_insert={cache_insert} execute={execute} total={total} data_gets={data_gets} index_gets={index_gets} index_ranges={index_ranges} index_entries={index_entries} authored_members={} normalized_members={} canonical_passes={} members_revisited={} branches_before={} branches_after={} exclusions_tested={} exclusions_pruned={} cap_admitted={} cap_rejected={} grouped_count_rows={grouped_count_rows} grouped_count_lookup={grouped_count_lookup} hybrid_hits={hybrid_hits} hybrid_index_fields={hybrid_index_fields} hybrid_row_fields={hybrid_row_fields} sql_hits={sql_hits} sql_misses={sql_misses} shared_hits={shared_hits} shared_misses={shared_misses}",
        structural.membership_authored_members,
        structural.membership_normalized_members,
        structural.membership_canonicalization_passes,
        structural.membership_members_revisited,
        structural.prefix_branches_before_deduplication,
        structural.prefix_branches_after_deduplication,
        structural.prefix_exclusions_tested,
        structural.prefix_exclusions_pruned,
        structural.prefix_branch_cap_admissions,
        structural.prefix_branch_cap_rejections,
    );
}

// WarmCacheContractCase keeps one update-then-query cache contract case
// together so the IC testkit audit can prove that a warm update call feeds the
// later compiled-plus-shared query cache path across more than one query family.
struct WarmCacheContractCase {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
}

// sql_perf_scenario_by_key resolves one focused cache or route contract.
fn sql_perf_scenario_by_key(scenario_key: &str) -> SqlPerfScenario {
    token_branch_set_scenarios()
        .into_iter()
        .chain(repeated_query_scenarios())
        .find(|scenario| scenario.scenario_key == scenario_key)
        .unwrap_or_else(|| panic!("sql perf scenario '{scenario_key}' should exist"))
}

// assert_repeat_scenario_keeps_compiled_and_shared_cache_path checks one exact
// in-call repeat contract; P2 owns repetition across independent canisters.
fn assert_repeat_scenario_keeps_compiled_and_shared_cache_path(
    fixture: &StandaloneCanisterFixture,
    scenario: SqlPerfScenario,
) {
    let repeated_hits =
        u64::try_from(scenario.query_loop_count.saturating_sub(1)).expect("loop count should fit");
    let sample = sample_perf_scenario(fixture, scenario);

    assert_eq!(
        sample.sql_compiled_command_cache_hits, repeated_hits,
        "scenario '{}' should keep SQL compiled-command hits for every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.sql_compiled_command_cache_misses, 1,
        "scenario '{}' should keep exactly one cold SQL compiled-command miss",
        sample.scenario_key,
    );
    assert_eq!(
        sample.shared_query_plan_cache_hits, repeated_hits,
        "scenario '{}' should surface shared lower query-plan hits on every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.shared_query_plan_cache_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once as cold-fill support",
        sample.scenario_key,
    );
}

// assert_update_warm_persists_compiled_and_shared_cache_path proves that an update-side
// warm call still fills the compiled-command cache and the shared lower
// query-plan cache for the later query-side call.
fn assert_update_warm_persists_compiled_and_shared_cache_path(
    fixture: &StandaloneCanisterFixture,
    case: WarmCacheContractCase,
) {
    let warm =
        warm_query_surface_with_perf(fixture, case.surface, case.sql).unwrap_or_else(|err| {
            panic!(
                "update warm cache contract scenario '{}' should succeed: {err}",
                case.scenario_key,
            )
        });

    // Phase 1: the update-side warm call should populate the compiled-command
    // cache and touch the shared lower cache once for cold fill.
    assert_eq!(
        warm.attribution.cache.sql_compiled_command_misses, 1,
        "scenario '{}' should populate the SQL compiled-command cache on the update warm pass",
        case.scenario_key,
    );
    assert_eq!(
        warm.attribution.cache.shared_query_plan_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once during the update warm cold fill",
        case.scenario_key,
    );

    // Phase 2: the later query call should stay entirely on the compiled SQL
    // hit path plus the shared lower query-plan hit path.
    let query = query_surface_with_perf(fixture, case.surface, case.sql, 1).unwrap_or_else(|err| {
        panic!(
            "query cache contract scenario '{}' should succeed after update warm: {err}",
            case.scenario_key,
        )
    });
    assert_eq!(
        query.attribution.cache.sql_compiled_command_hits, 1,
        "scenario '{}' should reuse the compiled SQL artifact warmed by the update call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.sql_compiled_command_misses, 0,
        "scenario '{}' should not recompile the warmed SQL artifact on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.shared_query_plan_hits, 1,
        "scenario '{}' should reuse the warmed shared lower query-plan cache on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.shared_query_plan_misses, 0,
        "scenario '{}' should not rebuild the lower shared query plan on the later query call",
        case.scenario_key,
    );
}

const HEAP_PRIMARY_LIMIT_ONE_SQL: &str =
    "SELECT id, name FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1";
const JOURNALED_PRIMARY_LIMIT_ONE_SQL: &str =
    "SELECT id, name FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1";
// The total-only endpoint includes accepted-schema observation by `db()` in
// addition to the cached SQL work. The 0.228 maximum-fanout audit entity makes
// that shared catalog deliberately wider; keep the query-owned attributed
// ceiling at 1,000,000 and freeze the complete endpoint envelope separately.
const WARMED_TOTAL_ONLY_LIMIT_ONE_BUDGET: u64 = 1_100_000;
// The first update includes heap-lost startup recovery and accepted-catalog
// reconstruction; the repeat proves that work remains one-time.
const JOURNALED_UPGRADE_FIRST_REENTRY_BUDGET: u64 = 8_000_000_000;
const JOURNALED_UPGRADE_WARM_REENTRY_BUDGET: u64 = 100_000_000;

fn query_sql_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    decode_expectation: &str,
    success_expectation: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid(method, (sql.to_string(),))
        .expect(decode_expectation);

    result.expect(success_expectation)
}

fn query_sql_loop_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    query_loop_count: u32,
    decode_expectation: &str,
    success_expectation: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid(method, (sql.to_string(), query_loop_count))
        .expect(decode_expectation);

    result.expect(success_expectation)
}

fn warm_sql_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    decode_expectation: &str,
    success_expectation: &str,
) {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid(method, (sql.to_string(),))
        .expect(decode_expectation);

    result.expect(success_expectation);
}

fn print_sql_limit_one_attribution(label: &str, perf: &SqlQueryPerfResult) {
    let attribution = &perf.attribution;
    let execution = &attribution.execution;
    let cache = &attribution.cache;

    println!(
        "{label}: compile={} plan_lookup={} planner={} store={} executor_invocation={} executor={} response_finalize={} execute={} response_decode={} total={} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
        attribution.compile_local_instructions,
        attribution.plan_lookup_local_instructions,
        execution.planner_local_instructions,
        execution.store_local_instructions,
        execution.executor_invocation_local_instructions,
        execution.executor_local_instructions,
        execution.response_finalization_local_instructions,
        attribution.execute_local_instructions,
        attribution.response_decode_local_instructions,
        attribution.total_local_instructions,
        cache.sql_compiled_command_hits,
        cache.sql_compiled_command_misses,
        cache.shared_query_plan_hits,
        cache.shared_query_plan_misses,
    );
}

fn print_storage_read_comparison(
    label: &str,
    heap: &SqlQueryPerfResult,
    journaled: &SqlQueryPerfResult,
) {
    println!(
        "{label}: heap_total={} journaled_total={} total_delta={} total_ratio={} heap_compile={} journaled_compile={} compile_delta={} heap_execute={} journaled_execute={} execute_delta={} heap_store={} journaled_store={} store_delta={} heap_executor={} journaled_executor={} executor_delta={} heap_data_store_gets={} journaled_data_store_gets={}",
        heap.attribution.total_local_instructions,
        journaled.attribution.total_local_instructions,
        signed_instruction_delta(
            journaled.attribution.total_local_instructions,
            heap.attribution.total_local_instructions,
        ),
        instruction_ratio_text(
            journaled.attribution.total_local_instructions,
            heap.attribution.total_local_instructions,
        ),
        heap.attribution.compile_local_instructions,
        journaled.attribution.compile_local_instructions,
        signed_instruction_delta(
            journaled.attribution.compile_local_instructions,
            heap.attribution.compile_local_instructions,
        ),
        heap.attribution.execute_local_instructions,
        journaled.attribution.execute_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execute_local_instructions,
            heap.attribution.execute_local_instructions,
        ),
        heap.attribution.execution.store_local_instructions,
        journaled.attribution.execution.store_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execution.store_local_instructions,
            heap.attribution.execution.store_local_instructions,
        ),
        heap.attribution.execution.executor_local_instructions,
        journaled.attribution.execution.executor_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execution.executor_local_instructions,
            heap.attribution.execution.executor_local_instructions,
        ),
        heap.attribution.store_get_calls,
        journaled.attribution.store_get_calls,
    );
}

fn signed_instruction_delta(value: u64, baseline: u64) -> String {
    if value >= baseline {
        format!("+{}", value - baseline)
    } else {
        format!("-{}", baseline - value)
    }
}

fn instruction_ratio_text(value: u64, baseline: u64) -> String {
    if baseline == 0 {
        return "n/a".to_string();
    }

    let scaled = value.saturating_mul(100) / baseline;
    format!("{}.{:02}x", scaled / 100, scaled % 100)
}

fn print_cached_journaled_sql_limit_one_attribution(perf: &SqlQueryPerfResult) {
    let attribution = &perf.attribution;
    let compile = &attribution.compile;
    let execution = &attribution.execution;
    let cache = &attribution.cache;

    println!(
        "journaled cached limit1 attribution: compile={} cache_key={} cache_lookup={} parse={} prepare={} lower={} bind={} plan_lookup={} planner={} store={} executor_invocation={} executor={} response_finalize={} execute={} response_decode={} total={} pure={:?} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
        attribution.compile_local_instructions,
        compile.cache_key_local_instructions,
        compile.cache_lookup_local_instructions,
        compile.parse_local_instructions,
        compile.prepare_local_instructions,
        compile.lower_local_instructions,
        compile.bind_local_instructions,
        attribution.plan_lookup_local_instructions,
        execution.planner_local_instructions,
        execution.store_local_instructions,
        execution.executor_invocation_local_instructions,
        execution.executor_local_instructions,
        execution.response_finalization_local_instructions,
        attribution.execute_local_instructions,
        attribution.response_decode_local_instructions,
        attribution.total_local_instructions,
        attribution.pure_covering,
        cache.sql_compiled_command_hits,
        cache.sql_compiled_command_misses,
        cache.shared_query_plan_hits,
        cache.shared_query_plan_misses,
    );
}

fn assert_storage_primary_limit_one_stays_bounded(label: &str, perf: &SqlQueryPerfResult) {
    let outcome = summarize_perf_outcome(&perf.result);

    assert_eq!(
        outcome.row_count, 1,
        "{label} primary-key LIMIT 1 perf query should return one row",
    );
    assert!(
        perf.attribution.execution.store_local_instructions < 1_000_000,
        "{label} primary-key LIMIT 1 store phase should stay bounded, got {}",
        perf.attribution.execution.store_local_instructions,
    );
}

fn assert_cached_primary_limit_one_stays_bounded(
    label: &str,
    cached: &SqlQueryPerfResult,
    cold: &SqlQueryPerfResult,
) {
    assert_eq!(
        cached.attribution.cache.sql_compiled_command_hits, 1,
        "{label} cached LIMIT 1 should reuse the compiled SQL artifact",
    );
    assert_eq!(
        cached.attribution.cache.sql_compiled_command_misses, 0,
        "{label} cached LIMIT 1 should not recompile",
    );
    assert_eq!(
        cached.attribution.cache.shared_query_plan_hits, 1,
        "{label} cached LIMIT 1 should reuse the prepared query plan",
    );
    assert_eq!(
        cached.attribution.cache.shared_query_plan_misses, 0,
        "{label} cached LIMIT 1 should not rebuild the prepared query plan",
    );
    assert!(
        cached.attribution.compile_local_instructions < 500_000,
        "{label} cached LIMIT 1 should not reload/re-fingerprint accepted schema before cache hit, got {}",
        cached.attribution.compile_local_instructions,
    );
    assert!(
        cached.attribution.plan_lookup_local_instructions < 100_000,
        "{label} cached LIMIT 1 should not re-enter the expensive plan lookup path, got {}",
        cached.attribution.plan_lookup_local_instructions,
    );
    assert!(
        cached.attribution.total_local_instructions
            <= cold
                .attribution
                .total_local_instructions
                .saturating_mul(2)
                .saturating_div(3),
        "{label} cached LIMIT 1 should stay materially below cold query cost, cold={} cached={}",
        cold.attribution.total_local_instructions,
        cached.attribution.total_local_instructions,
    );
    assert!(
        cached
            .attribution
            .execution
            .executor_invocation_local_instructions
            < 1_000_000,
        "{label} cached LIMIT 1 should not re-run recovery/schema reconciliation in executor prep, got {}",
        cached
            .attribution
            .execution
            .executor_invocation_local_instructions,
    );
    assert!(
        cached.attribution.total_local_instructions < 1_000_000,
        "{label} cached LIMIT 1 should stay bounded after caches are warm, got {}",
        cached.attribution.total_local_instructions,
    );
}

fn query_journaled_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlTotalOnlyPerfResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_candid("query_journaled_user_total_only_perf", (sql.to_string(),))
        .expect("journaled total-only LIMIT 1 perf query should decode");

    result.expect("journaled total-only LIMIT 1 perf query should succeed")
}

fn query_heap_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlTotalOnlyPerfResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_candid("query_heap_user_total_only_perf", (sql.to_string(),))
        .expect("heap total-only LIMIT 1 perf query should decode");

    result.expect("heap total-only LIMIT 1 perf query should succeed")
}

fn assert_journaled_total_only_limit_one_variants_stay_bounded(
    fixture: &StandaloneCanisterFixture,
) {
    let total_only =
        query_journaled_total_only_limit_one_perf(fixture, JOURNALED_PRIMARY_LIMIT_ONE_SQL);
    println!(
        "journaled total-only limit1 attribution: total={}",
        total_only.instructions,
    );

    for (label, sql) in [
        (
            "journaled total-only id limit1",
            "SELECT id FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1",
        ),
        (
            "journaled total-only name limit1",
            "SELECT name FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1",
        ),
    ] {
        // Compiled SQL cache identity includes the complete statement. The
        // earlier two-column sentinel does not warm either projection variant,
        // so explicitly establish each variant's own warm state before
        // enforcing the warmed ceiling.
        warm_sql_limit_one_with_perf(
            fixture,
            "warm_journaled_user_query_with_perf",
            sql,
            "journaled total-only variant warmup should decode",
            "journaled total-only variant warmup should succeed",
        );
        let variant = query_journaled_total_only_limit_one_perf(fixture, sql);
        println!("{label}: total={}", variant.instructions);
        assert!(
            variant.instructions < WARMED_TOTAL_ONLY_LIMIT_ONE_BUDGET,
            "{label} should stay under the warmed LIMIT 1 endpoint budget, got {} >= {}",
            variant.instructions,
            WARMED_TOTAL_ONLY_LIMIT_ONE_BUDGET,
        );
    }
}

fn assert_heap_total_only_limit_one_variants_stay_bounded(fixture: &StandaloneCanisterFixture) {
    let total_only = query_heap_total_only_limit_one_perf(fixture, HEAP_PRIMARY_LIMIT_ONE_SQL);
    println!(
        "heap total-only limit1 attribution: total={}",
        total_only.instructions,
    );

    for (label, sql) in [
        (
            "heap total-only id limit1",
            "SELECT id FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
        ),
        (
            "heap total-only name limit1",
            "SELECT name FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
        ),
    ] {
        // Keep this symmetric with the journaled fixture: every distinct SQL
        // text owns its own compiled-cache entry and must be warmed directly.
        warm_sql_limit_one_with_perf(
            fixture,
            "warm_heap_user_query_with_perf",
            sql,
            "heap total-only variant warmup should decode",
            "heap total-only variant warmup should succeed",
        );
        let variant = query_heap_total_only_limit_one_perf(fixture, sql);
        println!("{label}: total={}", variant.instructions);
        assert!(
            variant.instructions < WARMED_TOTAL_ONLY_LIMIT_ONE_BUDGET,
            "{label} should stay under the warmed LIMIT 1 endpoint budget, got {} >= {}",
            variant.instructions,
            WARMED_TOTAL_ONLY_LIMIT_ONE_BUDGET,
        );
    }
}

fn measure_journaled_ready_total_only_perf(
    fixture: &StandaloneCanisterFixture,
) -> ReadTotalOnlyPerfResult {
    let result: Result<ReadTotalOnlyPerfResult, Error> = fixture
        .update_candid("measure_journaled_reentry_perf", ())
        .expect("journaled ready perf update should decode");

    result.expect("journaled ready perf update should succeed")
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
                    == icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
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

fn advance_online_watchdog_until_work_sample(
    fixture: &StandaloneCanisterFixture,
    previous_work_samples: u64,
) {
    for _ in 0..MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        deliver_startup_watchdog_message(fixture);
        let snapshot = startup_watchdog_perf_snapshot(fixture);
        if snapshot.work_samples > previous_work_samples {
            assert_eq!(snapshot.work_completed, snapshot.work_samples);
            assert_eq!(snapshot.succeeded, snapshot.work_samples);
            assert_eq!(snapshot.retryable_failures, 0);
            assert_eq!(snapshot.invariant_failures, 0);
            return;
        }
    }
    panic!("online watchdog should fold the admitted batch within its residual delivery bound");
}

fn measure_startup_watchdog_recovery(
    fixture: &StandaloneCanisterFixture,
    expected_work_samples: u64,
) -> Patch1RecoveryObservation {
    let observed = observe_startup_watchdog_recovery(fixture);
    assert_eq!(observed.watchdog.scheduler_samples, expected_work_samples);
    assert_eq!(observed.watchdog.work_samples, expected_work_samples);
    assert_eq!(observed.watchdog.work_started, expected_work_samples);
    assert_eq!(observed.watchdog.work_completed, expected_work_samples);
    assert_eq!(observed.watchdog.succeeded, expected_work_samples);
    observed
}

fn observe_startup_watchdog_recovery(
    fixture: &StandaloneCanisterFixture,
) -> Patch1RecoveryObservation {
    upgrade_fixture_canister(fixture, "sql_perf");
    let memory_before = canister_memory_bytes(fixture);
    advance_startup_watchdog_until_ready(fixture);
    let memory_after = canister_memory_bytes(fixture);

    let measured: StartupWatchdogPerfSnapshot = fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("startup watchdog performance snapshot should decode");
    let replayed: StartupWatchdogPerfSnapshot = fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("replayed startup watchdog performance snapshot should decode");

    assert_eq!(replayed, measured);
    assert!(measured.scheduler_samples > 0);
    assert_eq!(measured.scheduler_samples, measured.work_samples);
    assert_eq!(measured.work_started, measured.work_samples);
    assert_eq!(measured.work_completed, measured.work_samples);
    assert_eq!(measured.succeeded, measured.work_samples);
    assert_eq!(measured.retryable_failures, 0);
    assert_eq!(measured.invariant_failures, 0);
    assert!(measured.scheduler_total_instructions > 0);
    let scheduler_maximum = measured
        .scheduler_maximum_instructions
        .expect("at least one scheduler sample should have a maximum");
    assert!(scheduler_maximum > 0);
    assert!(scheduler_maximum <= measured.scheduler_total_instructions);
    assert!(measured.work_total_instructions > 0);
    let latest = measured
        .work_latest_instructions
        .expect("at least one work sample should have a latest value");
    let maximum = measured
        .work_maximum_instructions
        .expect("at least one work sample should have a maximum");
    assert!(latest > 0);
    assert!(latest <= measured.work_total_instructions);
    assert!(maximum >= latest);
    assert!(maximum <= measured.work_total_instructions);
    assert!(maximum < 40_000_000_000);
    assert!(memory_after.0 >= memory_before.0);
    assert!(memory_after.1 >= memory_before.1);

    Patch1RecoveryObservation {
        watchdog: measured,
        memory_before,
        memory_after,
    }
}

fn measure_storage_write_matrix(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    label: &str,
) -> StorageWritePerfResult {
    let result: Result<StorageWritePerfResult, Error> = fixture
        .update_candid(method, ())
        .unwrap_or_else(|err| panic!("{label} write matrix perf result should decode: {err}"));

    result.unwrap_or_else(|err| panic!("{label} write matrix perf endpoint should succeed: {err}"))
}

fn print_storage_write_matrix(label: &str, result: &StorageWritePerfResult) {
    println!(
        "{label} write matrix: first_insert={} steady_insert_avg={} steady_update_avg={} steady_delete_avg={} write_then_read_back={} read_back_rows={}",
        result.first_insert_local_instructions,
        result.steady_insert_avg_local_instructions,
        result.steady_update_avg_local_instructions,
        result.steady_delete_avg_local_instructions,
        result.write_then_read_back_local_instructions,
        result.read_back_rows,
    );
}

fn assert_storage_write_matrix_stays_bounded(label: &str, result: &StorageWritePerfResult) {
    assert_eq!(
        result.read_back_rows, 1,
        "{label} write-then-read-back should return exactly one row",
    );

    for (metric, instructions, budget) in [
        (
            "first insert",
            result.first_insert_local_instructions,
            STORAGE_FIRST_INSERT_BUDGET,
        ),
        (
            "steady insert avg",
            result.steady_insert_avg_local_instructions,
            25_000_000,
        ),
        (
            "steady update avg",
            result.steady_update_avg_local_instructions,
            25_000_000,
        ),
        (
            "steady delete avg",
            result.steady_delete_avg_local_instructions,
            150_000_000,
        ),
        (
            "write then read back",
            result.write_then_read_back_local_instructions,
            100_000_000,
        ),
    ] {
        assert!(
            instructions < budget,
            "{label} {metric} should stay bounded, got {instructions} >= {budget}",
        );
    }
}

fn assert_storage_write_matrix_reports(fixture: &StandaloneCanisterFixture) {
    let heap = measure_storage_write_matrix(fixture, "measure_heap_user_write_matrix_perf", "heap");
    let journaled = measure_storage_write_matrix(
        fixture,
        "measure_journaled_user_write_matrix_perf",
        "journaled",
    );

    print_storage_write_matrix("heap", &heap);
    print_storage_write_matrix("journaled", &journaled);

    assert_storage_write_matrix_stays_bounded("heap", &heap);
    assert_storage_write_matrix_stays_bounded("journaled", &journaled);
}

fn measure_sql_write_materialization_matrix(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    label: &str,
) -> SqlWriteMaterializationPerfResult {
    let result: Result<SqlWriteMaterializationPerfResult, Error> =
        fixture.update_candid(method, ()).unwrap_or_else(|err| {
            panic!("{label} SQL write materialization result should decode: {err}")
        });

    result.unwrap_or_else(|err| {
        panic!("{label} SQL write materialization endpoint should succeed: {err}")
    })
}

fn print_sql_write_materialization_matrix(label: &str, result: &SqlWriteMaterializationPerfResult) {
    println!(
        "{label} SQL write materialization: update_count={} update_returning={} delete_count={} delete_returning={} rows=[{},{},{},{}]",
        result.local_instructions[0],
        result.local_instructions[1],
        result.local_instructions[2],
        result.local_instructions[3],
        result.rows[0],
        result.rows[1],
        result.rows[2],
        result.rows[3],
    );
}

fn assert_sql_write_materialization_matrix_stays_bounded(
    label: &str,
    result: &SqlWriteMaterializationPerfResult,
) {
    for (metric, rows) in SQL_WRITE_MATERIALIZATION_METRICS
        .iter()
        .copied()
        .zip(result.rows)
    {
        assert_eq!(
            rows, 32,
            "{label} {metric} should cover the broad fixture window"
        );
    }

    for (metric, instructions) in SQL_WRITE_MATERIALIZATION_METRICS
        .iter()
        .copied()
        .zip(result.local_instructions)
    {
        assert!(
            instructions < SQL_WRITE_MATERIALIZATION_BUDGET,
            "{label} SQL write materialization {metric} should stay bounded, got {instructions} >= {SQL_WRITE_MATERIALIZATION_BUDGET}",
        );
    }
}

fn assert_sql_write_materialization_matrix_reports(fixture: &StandaloneCanisterFixture) {
    let heap = measure_sql_write_materialization_matrix(
        fixture,
        "measure_heap_user_sql_write_materialization_perf",
        "heap",
    );
    let journaled = measure_sql_write_materialization_matrix(
        fixture,
        "measure_journaled_user_sql_write_materialization_perf",
        "journaled",
    );

    print_sql_write_materialization_matrix("heap", &heap);
    print_sql_write_materialization_matrix("journaled", &journaled);

    assert_sql_write_materialization_matrix_stays_bounded("heap", &heap);
    assert_sql_write_materialization_matrix_stays_bounded("journaled", &journaled);
}

fn measure_mutation_forward(fixture: &StandaloneCanisterFixture) -> MutationJobForwardPerfResult {
    let result: Result<MutationJobForwardPerfResult, MutationJobError> = fixture
        .update_candid("measure_journaled_user_mutation_forward_perf", ())
        .expect("mutation Forward perf result should decode");

    result.expect("mutation Forward perf endpoint should succeed")
}

fn verify_mutation_job_lifecycle(fixture: &StandaloneCanisterFixture) -> MutationJobVerifyResult {
    let result: Result<MutationJobVerifyResult, MutationJobError> = fixture
        .update_candid("verify_journaled_user_mutation_job_lifecycle", ())
        .expect("mutation Verify lifecycle result should decode");

    result.expect("mutation Verify lifecycle endpoint should succeed")
}

fn start_mutation_job(
    fixture: &StandaloneCanisterFixture,
    job_discriminator: u8,
    intent_discriminator: u8,
) -> Result<MutationJobStartPerfResult, MutationJobError> {
    fixture
        .update_candid(
            "start_journaled_user_mutation_job",
            (job_discriminator, intent_discriminator),
        )
        .expect("mutation-job start result should decode")
}

fn advance_mutation_job(
    fixture: &StandaloneCanisterFixture,
    job_discriminator: u8,
    expected_sequence: u64,
    idempotency_key: &str,
) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
    advance_mutation_job_with_perf(
        fixture,
        job_discriminator,
        expected_sequence,
        idempotency_key,
    )
    .map(|result| result.receipt)
}

fn advance_mutation_job_with_perf(
    fixture: &StandaloneCanisterFixture,
    job_discriminator: u8,
    expected_sequence: u64,
    idempotency_key: &str,
) -> Result<MutationJobAdvancePerfResult, MutationJobError> {
    fixture
        .update_candid(
            "advance_journaled_user_mutation_job",
            (
                job_discriminator,
                expected_sequence,
                idempotency_key.to_string(),
            ),
        )
        .expect("mutation-job advance result should decode")
}

fn cancel_mutation_job(
    fixture: &StandaloneCanisterFixture,
    job_discriminator: u8,
    expected_sequence: u64,
) -> Result<MutationJobCancellationPerfResult, MutationJobError> {
    fixture
        .update_candid(
            "cancel_journaled_user_mutation_job",
            (job_discriminator, expected_sequence),
        )
        .expect("mutation-job cancellation result should decode")
}

fn progress_job_inventory(fixture: &StandaloneCanisterFixture) -> ProgressJobInventoryPerfResult {
    let result: Result<ProgressJobInventoryPerfResult, MutationJobError> = fixture
        .update_candid("progress_job_inventory_perf", ())
        .expect("progress-job inventory result should decode");
    result.expect("progress-job inventory should succeed")
}

fn update_later_matching_mutation_row(fixture: &StandaloneCanisterFixture) -> u32 {
    let result: Result<u32, Error> = fixture
        .update_candid("update_journaled_user_after_mutation_job_start", ())
        .expect("later managed write result should decode");
    result.expect("later managed write should succeed")
}

fn load_mutation_byte_fixture(fixture: &StandaloneCanisterFixture) -> u32 {
    let mut loaded = 0;
    for row_id in 1..=20_u32 {
        let result: Result<u32, Error> = fixture
            .update_candid("load_journaled_user_mutation_byte_fixture", (row_id,))
            .expect("wide mutation fixture result should decode");
        loaded = result.expect("wide mutation fixture row should load");
        drain_online_watchdog_until_quiescent(fixture);
    }
    loaded
}

fn assert_mutation_forward_perf_stays_bounded(result: &MutationJobForwardPerfResult) {
    assert!(
        result.start_local_instructions > 0
            && result.start_local_instructions < DURABLE_START_INSTRUCTION_REVIEW_CEILING
    );
    assert_eq!(result.forward_local_instructions.len(), 3);
    assert_eq!(result.forward_keys_scanned, 512);
    assert_eq!(result.rows_updated, 512);
    assert_eq!(result.forward_keys_scanned_per_step, vec![240, 240, 32],);
    assert_eq!(result.rows_updated_per_step, vec![240, 240, 32],);
    assert_eq!(result.committed_sequence, 3);
    assert!(result.replay_matches);
    assert!(
        result.replay_local_instructions > 0
            && result.replay_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert_eq!(result.zero_candidate_keys_scanned, 512,);
    assert_eq!(result.zero_candidate_rows_updated, 0);
    assert_eq!(result.zero_candidate_sequence, 1);
    assert!(result.stale_request_preserved_sequence);
    assert_eq!(result.operation_timestamp_groups, 1);
    for instructions in &result.forward_local_instructions {
        assert!(
            *instructions < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING,
            "durable Forward step should stay bounded, got {instructions} >= {DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING}",
        );
    }
}

struct StorageLimitOneReadSamples {
    heap: SqlQueryPerfResult,
    journaled: SqlQueryPerfResult,
}

fn assert_storage_cold_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
) -> StorageLimitOneReadSamples {
    let heap = query_sql_limit_one_with_perf(
        fixture,
        "query_heap_user_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap primary LIMIT 1 perf query should decode",
        "heap primary LIMIT 1 perf query should succeed",
    );
    let journaled = query_sql_limit_one_with_perf(
        fixture,
        "query_journaled_user_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled primary LIMIT 1 perf query should decode",
        "journaled primary LIMIT 1 perf query should succeed",
    );

    print_sql_limit_one_attribution("heap limit1 attribution", &heap);
    print_sql_limit_one_attribution("journaled limit1 attribution", &journaled);
    print_storage_read_comparison("heap vs journaled primary LIMIT 1", &heap, &journaled);
    assert_storage_primary_limit_one_stays_bounded("heap", &heap);
    assert_storage_primary_limit_one_stays_bounded("journaled", &journaled);

    StorageLimitOneReadSamples { heap, journaled }
}

fn assert_heap_cached_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
    heap: &SqlQueryPerfResult,
) {
    warm_sql_limit_one_with_perf(
        fixture,
        "warm_heap_user_query_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap warm LIMIT 1 perf query should decode",
        "heap warm LIMIT 1 perf query should succeed",
    );
    let cached_heap = query_sql_limit_one_with_perf(
        fixture,
        "query_heap_user_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap cached LIMIT 1 perf query should decode",
        "heap cached LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("heap cached limit1 attribution", &cached_heap);
    assert_cached_primary_limit_one_stays_bounded("heap", &cached_heap, heap);

    let heap_looped = query_sql_loop_limit_one_with_perf(
        fixture,
        "query_heap_user_loop_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        10,
        "heap loop LIMIT 1 perf query should decode",
        "heap loop LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("heap loop limit1 attribution", &heap_looped);
}

fn assert_journaled_cached_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
    journaled: &SqlQueryPerfResult,
) {
    warm_sql_limit_one_with_perf(
        fixture,
        "warm_journaled_user_query_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled warm LIMIT 1 perf query should decode",
        "journaled warm LIMIT 1 perf query should succeed",
    );
    let cached = query_sql_limit_one_with_perf(
        fixture,
        "query_journaled_user_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled cached LIMIT 1 perf query should decode",
        "journaled cached LIMIT 1 perf query should succeed",
    );
    print_cached_journaled_sql_limit_one_attribution(&cached);
    assert_cached_primary_limit_one_stays_bounded("journaled", &cached, journaled);

    let looped = query_sql_loop_limit_one_with_perf(
        fixture,
        "query_journaled_user_loop_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        10,
        "journaled loop LIMIT 1 perf query should decode",
        "journaled loop LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("journaled loop limit1 attribution", &looped);
}

fn assert_storage_total_limit_one_reports(fixture: &StandaloneCanisterFixture) {
    assert_heap_total_only_limit_one_variants_stay_bounded(fixture);
    assert_journaled_total_only_limit_one_variants_stay_bounded(fixture);
}

fn assert_journaled_ready_perf_stays_bounded(
    label: &str,
    perf: &ReadTotalOnlyPerfResult,
    expected_rows: u32,
    instruction_budget: u64,
) {
    assert_eq!(
        perf.row_count, expected_rows,
        "{label} ready probe should return the expected journaled rows",
    );
    assert!(
        perf.instructions > 0,
        "{label} ready probe should report positive instructions",
    );
    assert!(
        perf.instructions < instruction_budget,
        "{label} ready probe should stay below the regression budget, got {} >= {}",
        perf.instructions,
        instruction_budget,
    );
}

#[test]
fn sql_perf_n_plus_one_loop_exhausts_one_shared_request_scope() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid(
            "query_user_loop_with_perf",
            (
                "SELECT id FROM PerfAuditUser WHERE id = 1 LIMIT 1".to_string(),
                257_u32,
            ),
        )
        .expect("request-budget probe should decode a typed result");
    let error = result.expect_err("the 257th query must exhaust the shared request root");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error_fact(&error, DiagnosticFactTag::BudgetResource),
        Some(DiagnosticExecutionBudgetResource::QueryExecutions.raw()),
    );
    assert_eq!(error_fact(&error, DiagnosticFactTag::Limit), Some(256));
    assert_eq!(error_fact(&error, DiagnosticFactTag::Actual), Some(257));
    assert_eq!(
        error_fact(&error, DiagnosticFactTag::ExecutionBudgetScope),
        Some(DiagnosticExecutionBudgetScope::Request.raw()),
    );
}

#[test]
fn sql_perf_update_warm_persists_compiled_and_shared_cache_across_calls() {
    let fixture = install_sql_perf_canister_fixture();

    for case in [
        WarmCacheContractCase {
            scenario_key: "user.pk.order_only.asc.limit1.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        },
        WarmCacheContractCase {
            scenario_key: "user.pk.order_only.asc.limit2.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
        },
        WarmCacheContractCase {
            scenario_key: "user.name.lower.order_only.asc.limit3.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        },
        WarmCacheContractCase {
            scenario_key: "user.age.order_only.asc.limit2.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 2",
        },
        WarmCacheContractCase {
            scenario_key: "user.grouped.age_count.limit10.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        },
        WarmCacheContractCase {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
        },
        WarmCacheContractCase {
            scenario_key: "account.active.lower.order_handle.asc.limit3.warm_after_update",
            surface: SqlPerfSurface::Account,
            sql: "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        },
    ] {
        reset_sql_perf_fixtures(&fixture);
        assert_update_warm_persists_compiled_and_shared_cache_path(&fixture, case);
    }
}

#[test]
fn sql_perf_journaled_primary_limit_one_stays_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let read_samples = assert_storage_cold_limit_one_reports(&fixture);
    assert_heap_cached_limit_one_reports(&fixture, &read_samples.heap);
    assert_journaled_cached_limit_one_reports(&fixture, &read_samples.journaled);
    assert_storage_total_limit_one_reports(&fixture);
    assert_storage_write_matrix_reports(&fixture);
    drain_online_watchdog_until_quiescent(&fixture);
    assert_sql_write_materialization_matrix_reports(&fixture);
}

#[test]
fn sql_perf_journaled_check_write_cost_is_measured() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let result: Result<ConstraintActivationPerfResult, Error> = fixture
        .update_candid("measure_journaled_user_constraint_write_perf", ())
        .expect("constraint write perf result should decode");
    let result = result.expect("constraint write perf endpoint should succeed");
    drain_online_watchdog_until_quiescent(&fixture);
    let checked: Result<StorageWritePerfResult, Error> = fixture
        .update_candid("measure_journaled_user_checked_write_perf", ())
        .expect("checked write perf result should decode");
    let checked = checked.expect("checked write perf endpoint should succeed");

    print_storage_write_matrix("journaled no-check", &result.no_check);
    print_storage_write_matrix("journaled checked", &checked);
    println!(
        "journaled ADD CHECK: instructions={} rows_scanned={}",
        result.add_check_local_instructions, result.add_check_rows_scanned,
    );
    assert_eq!(result.add_check_rows_scanned, 0);
    assert!(result.add_check_local_instructions > 0);
    // Each path pays different fixed accepted-catalog and row-validation work.
    // Keep the limits explicit so a cheaper baseline does not turn a bounded
    // additive cost into a false regression through one blanket percentage.
    for (metric, without_check, with_check, max_percent) in [
        (
            "steady insert",
            result.no_check.steady_insert_avg_local_instructions,
            checked.steady_insert_avg_local_instructions,
            5,
        ),
        (
            "steady update",
            result.no_check.steady_update_avg_local_instructions,
            checked.steady_update_avg_local_instructions,
            15,
        ),
        (
            "steady delete",
            result.no_check.steady_delete_avg_local_instructions,
            checked.steady_delete_avg_local_instructions,
            10,
        ),
        (
            "write then read back",
            result.no_check.write_then_read_back_local_instructions,
            checked.write_then_read_back_local_instructions,
            10,
        ),
    ] {
        let limit = without_check.saturating_mul(100 + max_percent) / 100;
        assert!(
            with_check <= limit,
            "journaled {metric} check overhead should stay within {max_percent}%, got {with_check} > {limit} from {without_check}",
        );
    }
}

#[test]
fn sql_perf_mutation_forward_steps_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let result = measure_mutation_forward(&fixture);
    println!(
        "durable mutation: start={} forward={:?} replay={} forward_keys={} updated={}",
        result.start_local_instructions,
        result.forward_local_instructions,
        result.replay_local_instructions,
        result.forward_keys_scanned,
        result.rows_updated,
    );
    assert_mutation_forward_perf_stays_bounded(&result);
}

#[test]
fn sql_mutation_job_verify_restarts_on_revision_drift_and_completes_stably() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);
    reset_sql_perf_metrics(&fixture);

    let result = verify_mutation_job_lifecycle(&fixture);

    println!(
        "durable mutation Verify: first={} replay={} unrelated={} drift={} stable={:?} state={} terminal_replay={} ack={}",
        result.first_verify_local_instructions,
        result.verify_replay_local_instructions,
        result.unrelated_verify_local_instructions,
        result.drift_restart_local_instructions,
        result.stable_verify_local_instructions,
        result.state_local_instructions,
        result.terminal_replay_local_instructions,
        result.acknowledgement_local_instructions,
    );

    assert_eq!(
        result.first_verify_keys_scanned,
        u64::from(DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT),
    );
    assert!(result.first_verify_local_instructions < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING);
    assert!(result.replay.verify_matches);
    assert!(result.verify_replay_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert_eq!(
        result.unrelated_verify_keys_scanned,
        u64::from(DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT),
    );
    assert!(result.unrelated_preserved_verify);
    assert!(result.unrelated_verify_local_instructions < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING);
    assert_eq!(result.drift_restart_keys_scanned, 0);
    assert!(result.drift_restart_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert_eq!(result.stable_verify_local_instructions.len(), 3);
    assert!(
        result
            .stable_verify_local_instructions
            .iter()
            .all(|instructions| *instructions < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING)
    );
    assert_eq!(result.verify_restarts_total, 1);
    assert_eq!(result.restarted_forward_rows_updated, 1);
    assert_eq!(result.completed_sequence, 12);
    assert!(result.state_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(result.terminal_replay_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(result.replay.terminal_matches);
    assert!(result.acknowledgement.stale_rejected);
    assert!(result.acknowledgement_local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(result.acknowledgement.terminal_acknowledged);

    let inventory = progress_job_inventory(&fixture);
    assert_eq!(inventory.inventory.retained_count, 0);
    let metrics = extended_sql_perf_metrics(&fixture);
    let jobs = metrics
        .counters()
        .expect("current metrics window should include counters")
        .ops()
        .mutation_jobs();
    assert_eq!(jobs.starts_inserted(), 1);
    assert!(jobs.states_loaded() >= 1);
    assert!(jobs.advances_exact_replayed() >= 2);
    assert!(jobs.forward_steps_committed() >= 1);
    assert!(jobs.verify_steps_committed() >= 1);
    assert!(jobs.forward_to_verify_transitions() >= 2);
    assert_eq!(jobs.verify_restarts_revision_drift(), 1);
    assert_eq!(jobs.verify_restarts_residual_work(), 0);
    assert_eq!(jobs.completions(), 1);
    assert!(jobs.keys_scanned() > 0);
    assert!(jobs.scan_bytes() > 0);
    assert!(jobs.staged_bytes() > 0);
    assert_eq!(
        jobs.target_failure_count(MutationJobTargetFailureReason::Other),
        0
    );
    assert_eq!(jobs.inventories_loaded(), 1);
    assert_eq!(jobs.retained_count(), 0);
    assert_eq!(jobs.hard_limit(), 64);
    assert_eq!(jobs.reserved_integrity_headroom(), 8);
    assert_eq!(jobs.retained_record_bytes(), 0);
}

#[test]
fn sql_mutation_job_converges_a_row_written_after_start_and_replays_exactly() {
    const JOB_DISCRIMINATOR: u8 = 76;
    const MAX_ADVANCES: usize = 24;

    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);
    let started = start_mutation_job(&fixture, JOB_DISCRIMINATOR, 0)
        .expect("managed-time mutation job should start");

    fixture.pocket_ic().advance_time(Duration::from_secs(1));
    assert_eq!(update_later_matching_mutation_row(&fixture), 1);
    fixture.pocket_ic().advance_time(Duration::from_secs(1));

    let first = advance_mutation_job(&fixture, JOB_DISCRIMINATOR, 0, "managed-time-first")
        .expect("later matching row should admit with the advance-message timestamp");
    assert_eq!(first.request_sequence, started.state.sequence);
    assert_eq!(first.status, MutationJobStatus::Active);
    assert_eq!(first.phase, MutationJobPhase::Forward);
    assert!(first.rows_updated > 0);
    let replay = advance_mutation_job(&fixture, JOB_DISCRIMINATOR, 0, "managed-time-first")
        .expect("committed managed-time advance should replay");
    assert_eq!(replay, first);

    let mut receipt = first;
    for _ in 0..MAX_ADVANCES {
        if receipt.status == MutationJobStatus::Completed {
            break;
        }
        fixture.pocket_ic().advance_time(Duration::from_secs(1));
        let sequence = receipt.committed_sequence;
        receipt = advance_mutation_job(
            &fixture,
            JOB_DISCRIMINATOR,
            sequence,
            format!("managed-time-{sequence}").as_str(),
        )
        .expect("managed-time job should converge across messages");
    }

    assert_eq!(receipt.status, MutationJobStatus::Completed);
    assert_eq!(receipt.rows_updated_total, 512);
    let managed_time_groups = query_sql_limit_one_with_perf(
        &fixture,
        "query_journaled_user_with_perf",
        "SELECT updated_at, COUNT(*) FROM PerfAuditJournaledUser \
         WHERE name = 'durable-start' GROUP BY updated_at ORDER BY updated_at ASC LIMIT 16",
        "managed-time group query should decode",
        "managed-time group query should succeed",
    );
    let SqlQueryResult::Grouped(groups) = managed_time_groups.result else {
        panic!("managed-time proof should return grouped rows");
    };
    assert_eq!(groups.row_count, 3);
}

#[test]
fn sql_perf_mutation_job_start_is_durable_replayable_and_non_mutating() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let first = start_mutation_job(&fixture, 71, 0).expect("first durable start should succeed");
    let replay = start_mutation_job(&fixture, 71, 3)
        .expect("canonically equivalent start should return retained state");
    let conflict = start_mutation_job(&fixture, 71, 1);
    let heap = start_mutation_job(&fixture, 72, 2);

    println!(
        "durable mutation start: first={} replay={} sequence={} changed={}",
        first.local_instructions,
        replay.local_instructions,
        first.state.sequence,
        first.target_rows_changed,
    );

    assert_eq!(first.state, replay.state);
    assert_eq!(first.state.sequence, 0);
    assert_eq!(first.state.status, MutationJobStatus::Active);
    assert_eq!(first.target_rows_changed, 0);
    assert_eq!(replay.target_rows_changed, 0);
    assert_eq!(conflict, Err(MutationJobError::IdentityConflict));
    assert_eq!(heap, Err(MutationJobError::IneligibleIntent));
    assert!(
        first.local_instructions < DURABLE_START_INSTRUCTION_REVIEW_CEILING,
        "first durable start should remain below the frozen review ceiling"
    );
    assert!(
        replay.local_instructions < DURABLE_START_INSTRUCTION_REVIEW_CEILING,
        "same-intent replay should remain below the frozen review ceiling"
    );
}

#[test]
fn sql_mutation_job_sequence_zero_cancellation_is_idempotent_and_incarnation_safe() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    start_mutation_job(&fixture, 90, 0).expect("initial cancellable job should start");
    assert_eq!(
        cancel_mutation_job(&fixture, 90, 1),
        Err(MutationJobError::StaleSequence {
            expected: 1,
            actual: 0,
        }),
    );
    assert_eq!(progress_job_inventory(&fixture).inventory.retained_count, 1);

    let cancelled = cancel_mutation_job(&fixture, 90, 0)
        .expect("exact sequence-zero cancellation should succeed");
    let lost_response_retry =
        cancel_mutation_job(&fixture, 90, 0).expect("absent cancellation retry should succeed");
    assert_eq!(progress_job_inventory(&fixture).inventory.retained_count, 0);

    start_mutation_job(&fixture, 91, 0).expect("fresh logical job identity should start");
    cancel_mutation_job(&fixture, 90, 0)
        .expect("delayed retry for the retired identity should remain harmless");
    let retained = progress_job_inventory(&fixture);
    assert_eq!(retained.inventory.retained_count, 1);
    assert_eq!(retained.inventory.mutation_count, 1);
    assert_eq!(retained.inventory.records[0].job_id[31], 91);

    start_mutation_job(&fixture, 92, 0).expect("nonzero-sequence proof job should start");
    let forward = advance_mutation_job(&fixture, 92, 0, "cancel-nonzero-forward")
        .expect("empty Forward page should commit");
    assert_eq!(
        cancel_mutation_job(&fixture, 92, forward.committed_sequence),
        Err(MutationJobError::StaleSequence {
            expected: 0,
            actual: forward.committed_sequence,
        }),
    );

    println!(
        "mutation recovery operations: cancel={} absent_retry={} inventory={}",
        cancelled.local_instructions,
        lost_response_retry.local_instructions,
        retained.local_instructions,
    );
    assert!(cancelled.local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(lost_response_retry.local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(retained.local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
}

#[test]
fn sql_progress_capacity_reserves_exact_integrity_headroom() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    for job_discriminator in 100..=154 {
        start_mutation_job(&fixture, job_discriminator, 0)
            .expect("the first 55 non-integrity jobs should start");
    }
    let at_fifty_five = progress_job_inventory(&fixture);
    assert_eq!(at_fifty_five.inventory.retained_count, 55);
    assert!(at_fifty_five.inventory.retained_record_bytes > 0);

    start_mutation_job(&fixture, 155, 0).expect("the 56th non-integrity job should start");
    let at_fifty_six = progress_job_inventory(&fixture);
    assert_eq!(at_fifty_six.inventory.retained_count, 56);
    assert_eq!(at_fifty_six.inventory.reserved_integrity_headroom, 8);
    assert_eq!(
        start_mutation_job(&fixture, 156, 0),
        Err(MutationJobError::CapacityExceeded),
    );
    start_mutation_job(&fixture, 100, 3)
        .expect("a retained exact start replay should survive reserved capacity");

    for ordinal in 0..7 {
        measure_integrity_sql(
            &fixture,
            &format!("CHECK INTEGRITY PerfAuditJournaledUser DEEP START 'capacity-{ordinal}'"),
        );
    }
    let at_sixty_three = progress_job_inventory(&fixture);
    assert_eq!(at_sixty_three.inventory.retained_count, 63);
    assert_eq!(at_sixty_three.inventory.integrity_count, 7);

    measure_integrity_sql(
        &fixture,
        "CHECK INTEGRITY PerfAuditJournaledUser DEEP START 'capacity-7'",
    );
    let at_sixty_four = progress_job_inventory(&fixture);
    assert_eq!(at_sixty_four.inventory.retained_count, 64);
    assert_eq!(at_sixty_four.inventory.integrity_count, 8);
    assert_eq!(at_sixty_four.inventory.mutation_count, 56);
    assert_eq!(at_sixty_four.inventory.records.len(), 64);
    assert!(
        at_sixty_four.inventory.retained_record_bytes
            > at_fifty_six.inventory.retained_record_bytes
    );
    assert!(
        at_sixty_four
            .inventory
            .records
            .iter()
            .any(|record| record.family == ProgressJobFamily::Integrity)
    );

    let overflow: Result<IntegritySqlPerfResult, SqlIntegrityError> = fixture
        .update_candid(
            "measure_integrity_sql_perf",
            ("CHECK INTEGRITY PerfAuditJournaledUser DEEP START 'capacity-8'".to_string(),),
        )
        .expect("integrity capacity result should decode");
    assert_eq!(
        overflow,
        Err(SqlIntegrityError::Integrity(IntegrityCheckError::Job(
            IntegrityJobError::CapacityExceeded,
        ))),
    );
    println!(
        "progress inventory instructions: at_55={} at_56={} at_63={} at_64={} bytes_64={}",
        at_fifty_five.local_instructions,
        at_fifty_six.local_instructions,
        at_sixty_three.local_instructions,
        at_sixty_four.local_instructions,
        at_sixty_four.inventory.retained_record_bytes,
    );
    assert!(at_sixty_four.local_instructions < DURABLE_INVENTORY_INSTRUCTION_REVIEW_CEILING);
}

#[test]
fn sql_mutation_job_byte_packing_revisits_boundaries_and_converges() {
    const ROWS: u64 = 20;

    let fixture = install_sql_perf_canister_fixture();
    assert_eq!(u64::from(load_mutation_byte_fixture(&fixture)), ROWS);

    let nonmatching =
        start_mutation_job(&fixture, 81, 4).expect("the nonmatching wide-row job should start");
    assert_eq!(nonmatching.state.sequence, 0);
    let first_nonmatching = advance_mutation_job(&fixture, 81, 0, "wide-nonmatching-0")
        .expect("the first nonmatching byte-bounded page should commit");
    let nonmatching_replay = advance_mutation_job(&fixture, 81, 0, "wide-nonmatching-0")
        .expect("the first nonmatching page should replay exactly");
    assert_eq!(first_nonmatching, nonmatching_replay);
    assert_eq!(first_nonmatching.rows_updated, 0);
    assert_eq!(first_nonmatching.keys_scanned, 18);

    let mut receipt = first_nonmatching;
    for step in 1..16_u64 {
        if receipt.status == MutationJobStatus::Completed {
            break;
        }
        receipt = advance_mutation_job(
            &fixture,
            81,
            receipt.committed_sequence,
            &format!("wide-nonmatching-{step}"),
        )
        .expect("the nonmatching job should converge across scan-byte pages");
        drain_online_watchdog_until_quiescent(&fixture);
    }
    assert_eq!(receipt.status, MutationJobStatus::Completed);
    assert_eq!(receipt.rows_updated_total, 0);

    let matching =
        start_mutation_job(&fixture, 82, 5).expect("the matching wide-row job should start");
    let first_matching =
        advance_mutation_job(&fixture, 82, matching.state.sequence, "wide-match-0")
            .expect("the first exact-staging page should commit");
    assert_eq!(first_matching.status, MutationJobStatus::Active);
    assert_eq!(first_matching.phase, MutationJobPhase::Forward);
    assert_eq!(first_matching.rows_updated, 9);
    assert_eq!(first_matching.keys_scanned, 10);
    drain_online_watchdog_until_quiescent(&fixture);

    let mut receipt = first_matching;
    for step in 1..20_u64 {
        if receipt.status == MutationJobStatus::Completed {
            break;
        }
        receipt = advance_mutation_job(
            &fixture,
            82,
            receipt.committed_sequence,
            &format!("wide-match-{step}"),
        )
        .expect("the matching job should revisit every excluded candidate and converge");
        drain_online_watchdog_until_quiescent(&fixture);
    }
    assert_eq!(receipt.status, MutationJobStatus::Completed);
    assert_eq!(receipt.rows_updated_total, ROWS);
}

#[test]
fn sql_mutation_job_admits_the_maximum_index_fanout_page() {
    let fixture = install_sql_perf_canister_fixture();
    let loaded: Result<JointFanoutFixtureFacts, Error> = fixture
        .update_candid("load_joint_fanout_boundary_fixture", ())
        .expect("maximum-fanout fixture result should decode");
    assert_eq!(
        loaded
            .expect("maximum-fanout fixture should load")
            .secondary_indexes_per_row,
        64
    );

    let started = start_mutation_job(&fixture, 83, 6)
        .expect("the maximum-index-fanout mutation job should start");
    let first =
        advance_mutation_job_with_perf(&fixture, 83, started.state.sequence, "max-fanout-0")
            .expect("the maximum-index-fanout page should fit one Forward advance");
    println!(
        "durable maximum-fanout Forward: instructions={} keys={} rows={}",
        first.local_instructions, first.receipt.keys_scanned, first.receipt.rows_updated,
    );
    assert!(first.local_instructions < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING);
    let mut receipt = first.receipt;
    for step in 1..12_u64 {
        if receipt.status == MutationJobStatus::Completed {
            break;
        }
        receipt = advance_mutation_job(
            &fixture,
            83,
            receipt.committed_sequence,
            &format!("max-fanout-{step}"),
        )
        .expect("the maximum-index-fanout mutation job should converge");
        drain_online_watchdog_until_quiescent(&fixture);
    }
    assert_eq!(receipt.status, MutationJobStatus::Completed);
    assert_eq!(receipt.rows_updated_total, 240);
}

#[test]
fn sql_perf_integrity_quick_and_deep_pages_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_journaled_reentry_probe_fixture(&fixture);
    let observation =
        measure_clean_integrity_run(&fixture, "PerfAuditJournaledUser", "closeout-evidence");

    println!(
        "integrity closeout: quick_instructions={} deep_page_instructions={:?} \
         quick_response_bytes={} max_deep_response_bytes={} \
         memory_bytes_wasm_stable={:?}->{:?}->{:?}",
        observation.quick_instructions,
        observation.deep_page_instructions,
        observation.quick_response_bytes,
        observation.max_deep_response_bytes,
        observation.memory_before,
        observation.memory_after_quick,
        observation.memory_after_deep,
    );
    assert_clean_integrity_perf_stays_bounded(&observation, INTEGRITY_QUICK_OPERATION_BUDGET);
}

#[test]
fn sql_perf_integrity_live_journal_tail_pages_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_journal_tail_integrity_fixture(&fixture);
    let observation =
        measure_clean_integrity_run(&fixture, "PerfAuditJournaledUser", "journal-tail-evidence");
    let journal_pages = observation
        .deep_page_phases
        .iter()
        .filter(|phase| **phase == IntegrityPhase::JournalTails)
        .count();

    println!(
        "integrity live journal tail: quick_instructions={} \
         deep_page_instructions={:?} deep_page_phases={:?} journal_pages={} \
         quick_response_bytes={} max_deep_response_bytes={}",
        observation.quick_instructions,
        observation.deep_page_instructions,
        observation.deep_page_phases,
        journal_pages,
        observation.quick_response_bytes,
        observation.max_deep_response_bytes,
    );
    assert_clean_integrity_perf_stays_bounded(&observation, INTEGRITY_QUICK_OPERATION_BUDGET);
    assert!(
        journal_pages > 1,
        "six live journal batches should require multiple bounded journal pages",
    );
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::FinalProofVectorCheck)
    );
}

#[test]
fn sql_perf_integrity_accepted_check_pages_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    activate_journaled_user_perf_check(&fixture);
    load_journaled_reentry_probe_fixture(&fixture);
    upgrade_fixture_canister(&fixture, "sql_perf");
    advance_startup_watchdog_until_ready(&fixture);
    let ready_probe = measure_journaled_ready_total_only_perf(&fixture);
    let observation = measure_clean_integrity_run(
        &fixture,
        "PerfAuditJournaledUser",
        "accepted-check-evidence",
    );

    println!(
        "integrity accepted check: ready_probe_instructions={} quick_instructions={} \
         deep_page_instructions={:?} \
         quick_response_bytes={} max_deep_response_bytes={}",
        ready_probe.instructions,
        observation.quick_instructions,
        observation.deep_page_instructions,
        observation.quick_response_bytes,
        observation.max_deep_response_bytes,
    );
    assert_journaled_ready_perf_stays_bounded(
        "accepted-check setup",
        &ready_probe,
        1,
        JOURNALED_UPGRADE_FIRST_REENTRY_BUDGET,
    );
    assert_clean_integrity_perf_stays_bounded(&observation, INTEGRITY_QUICK_OPERATION_BUDGET);
}

#[test]
fn sql_perf_integrity_many_index_pages_stay_bounded() {
    const FIXTURE_ROWS: u32 = 16;

    let fixture = install_sql_perf_canister_fixture();
    load_user_scale_integrity_fixture(&fixture, FIXTURE_ROWS);
    upgrade_fixture_canister(&fixture, "sql_perf");
    advance_startup_watchdog_until_ready(&fixture);
    let ready_probe = measure_journaled_ready_total_only_perf(&fixture);
    let observation = measure_clean_integrity_run(&fixture, "PerfAuditUser", "many-index-evidence");

    println!(
        "integrity many-index: ready_probe_instructions={} quick_instructions={} \
         deep_page_instructions={:?} deep_page_phases={:?} \
         quick_response_bytes={} max_deep_response_bytes={}",
        ready_probe.instructions,
        observation.quick_instructions,
        observation.deep_page_instructions,
        observation.deep_page_phases,
        observation.quick_response_bytes,
        observation.max_deep_response_bytes,
    );
    assert_journaled_ready_perf_stays_bounded(
        "many-index setup",
        &ready_probe,
        0,
        JOURNALED_UPGRADE_FIRST_REENTRY_BUDGET,
    );
    assert_clean_integrity_perf_stays_bounded(
        &observation,
        INTEGRITY_COMPLEX_QUICK_OPERATION_BUDGET,
    );
    assert!(observation.deep_page_phases.contains(&IntegrityPhase::Rows));
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::IndexEntries)
    );
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::JournalTails)
    );
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::FinalProofVectorCheck)
    );
}

#[test]
fn sql_perf_integrity_relation_pages_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_relation_integrity_fixture(&fixture);
    upgrade_fixture_canister(&fixture, "sql_perf");
    advance_startup_watchdog_until_ready(&fixture);
    let ready_probe = measure_journaled_ready_total_only_perf(&fixture);
    let cold_quick =
        measure_integrity_sql(&fixture, "CHECK INTEGRITY PerfAuditRelationSource QUICK");
    let cold_quick_response_bytes = candid::encode_one(&cold_quick.result)
        .expect("cold relation Quick result should encode")
        .len();
    let observation =
        measure_clean_integrity_run(&fixture, "PerfAuditRelationSource", "relation-evidence");

    println!(
        "integrity relation: ready_probe_instructions={} \
         cold_quick_instructions={} warm_quick_instructions={} \
         deep_page_instructions={:?} deep_page_phases={:?} \
         cold_quick_response_bytes={} warm_quick_response_bytes={} \
         max_deep_response_bytes={}",
        ready_probe.instructions,
        cold_quick.local_instructions,
        observation.quick_instructions,
        observation.deep_page_instructions,
        observation.deep_page_phases,
        cold_quick_response_bytes,
        observation.quick_response_bytes,
        observation.max_deep_response_bytes,
    );
    assert_journaled_ready_perf_stays_bounded(
        "relation setup",
        &ready_probe,
        0,
        INTEGRITY_RELATION_RECOVERY_BUDGET,
    );
    assert!(
        (1..=INTEGRITY_RELATION_COLD_QUICK_OPERATION_BUDGET)
            .contains(&cold_quick.local_instructions)
    );
    assert!(matches!(cold_quick.result, IntegrityCheckResult::Quick(_)));
    assert!(cold_quick_response_bytes <= INTEGRITY_RESPONSE_BYTE_BUDGET);
    assert_clean_integrity_perf_stays_bounded(
        &observation,
        INTEGRITY_COMPLEX_QUICK_OPERATION_BUDGET,
    );
    assert!(observation.deep_page_phases.contains(&IntegrityPhase::Rows));
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::ReverseRelations)
    );
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::JournalTails)
    );
    assert!(
        observation
            .deep_page_phases
            .contains(&IntegrityPhase::FinalProofVectorCheck)
    );
}

#[test]
fn sql_perf_integrity_proof_invalidation_stays_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_journaled_reentry_probe_fixture(&fixture);
    let start = measure_integrity_sql(
        &fixture,
        "CHECK INTEGRITY PerfAuditJournaledUser DEEP START 'invalidation-evidence'",
    );
    let IntegrityCheckResult::Deep(start_receipt) = start.result else {
        panic!("Deep start should return a Deep receipt");
    };

    load_journaled_reentry_probe_fixture(&fixture);
    let continue_sql = format!(
        "CHECK INTEGRITY DEEP CONTINUE '{}' AFTER {}",
        start_receipt.job_id().to_hex(),
        start_receipt.page_sequence(),
    );
    let invalidated = measure_integrity_sql(&fixture, continue_sql.as_str());
    let invalidated_response_bytes = candid::encode_one(&invalidated.result)
        .expect("invalidated result should encode")
        .len();
    assert!(matches!(
        invalidated.result,
        IntegrityCheckResult::Deep(IntegrityJobReceipt::Page(ref page))
            if page.status()
                == &DeepIntegrityPageStatus::Terminal(
                    IntegrityTerminalOutcome::Invalidated
                )
    ));

    println!(
        "integrity invalidation: start_instructions={} invalidation_instructions={} \
         invalidation_response_bytes={invalidated_response_bytes}",
        start.local_instructions, invalidated.local_instructions,
    );
    assert!((1..=INTEGRITY_DEEP_PAGE_BUDGET).contains(&start.local_instructions));
    assert!((1..=INTEGRITY_DEEP_PAGE_BUDGET).contains(&invalidated.local_instructions));
    assert!(invalidated_response_bytes <= INTEGRITY_RESPONSE_BYTE_BUDGET);
}

#[test]
fn sql_perf_journaled_upgrade_driver_then_ready_probe_stays_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_journaled_reentry_probe_fixture(&fixture);

    upgrade_fixture_canister(&fixture, "sql_perf");
    advance_startup_watchdog_until_ready(&fixture);

    let first = measure_journaled_ready_total_only_perf(&fixture);
    let second = measure_journaled_ready_total_only_perf(&fixture);

    println!(
        "journaled ready probe after upgrade: first_total={} second_total={} first_rows={} second_rows={}",
        first.instructions, second.instructions, first.row_count, second.row_count,
    );

    assert_journaled_ready_perf_stays_bounded(
        "first",
        &first,
        1,
        JOURNALED_UPGRADE_FIRST_REENTRY_BUDGET,
    );
    assert_journaled_ready_perf_stays_bounded(
        "second",
        &second,
        1,
        JOURNALED_UPGRADE_WARM_REENTRY_BUDGET,
    );
}

#[test]
fn patch1_current_2048_record_recovery_uses_the_real_watchdog_instruction_accounting() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_journaled_user_scale_fixture", (2_048_u32,))
        .expect("journaled scale fixture facts should decode");
    let loaded = loaded.expect("journaled scale fixture should load");
    assert_eq!(loaded.surface, "journaled_user");
    assert_eq!(loaded.fixture_rows, 2_048);

    let observed = measure_startup_watchdog_recovery(&fixture, 1);
    let measured = &observed.watchdog;
    assert!(
        measured
            .work_maximum_instructions
            .is_some_and(|instructions| instructions <= 20_000_000_000)
    );

    println!(
        "0.228 Patch 1 current 2,048-record recovery baseline: scheduler_instructions={} work_instructions={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        measured.scheduler_total_instructions,
        measured.work_total_instructions,
        observed.memory_before.0,
        observed.memory_after.0,
        observed.memory_before.1,
        observed.memory_after.1,
    );
}

#[test]
fn joint_admitted_4096_zero_index_rows_fit_one_real_watchdog_work_message() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_joint_zero_index_boundary_fixture", ())
        .expect("zero-index joint-boundary fixture facts should decode");
    let loaded = loaded.expect("zero-index joint-boundary fixture should load");
    assert_eq!(loaded.surface, "journaled_user");
    assert_eq!(loaded.fixture_rows, 4_096);

    let observed = measure_startup_watchdog_recovery(&fixture, 1);
    let measured = &observed.watchdog;
    assert!(
        measured
            .work_maximum_instructions
            .is_some_and(|instructions| instructions <= 20_000_000_000)
    );

    println!(
        "0.228 joint-admitted 4,096-row zero-index recovery boundary: scheduler_instructions={} work_instructions={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        measured.scheduler_total_instructions,
        measured.work_total_instructions,
        observed.memory_before.0,
        observed.memory_after.0,
        observed.memory_before.1,
        observed.memory_after.1,
    );
}

#[test]
fn patch1_near_maximum_row_and_derived_indexes_fit_one_real_watchdog_work_message() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_patch1_wide_row_recovery_fixture", ())
        .expect("Patch 1 wide-row fixture result should decode");
    assert_eq!(
        loaded.expect("Patch 1 wide-row fixture should load"),
        (4 * 1024 * 1024) - 1024,
    );

    let observed = measure_startup_watchdog_recovery(&fixture, 1);
    let measured = &observed.watchdog;
    assert!(
        measured
            .work_maximum_instructions
            .is_some_and(|instructions| instructions <= 20_000_000_000)
    );

    println!(
        "0.228 Patch 1 near-maximum row recovery: scheduler_instructions={} work_instructions={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        measured.scheduler_total_instructions,
        measured.work_total_instructions,
        observed.memory_before.0,
        observed.memory_after.0,
        observed.memory_before.1,
        observed.memory_after.1,
    );
}

#[test]
fn joint_admitted_2048_rows_with_three_derived_indexes_finish_in_one_recovery_message() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<ScaleFixtureFacts, Error> = fixture
        .update_candid("load_joint_three_index_boundary_fixture", ())
        .expect("three-index joint-boundary fixture facts should decode");
    let loaded = loaded.expect("three-index joint-boundary fixture should load");
    assert_eq!(loaded.surface, "user");
    assert_eq!(loaded.fixture_rows, 2_048);

    let observed = measure_startup_watchdog_recovery(&fixture, 1);
    let measured = &observed.watchdog;
    assert!(
        measured
            .work_maximum_instructions
            .is_some_and(|instructions| instructions < 30_000_000_000)
    );

    println!(
        "0.228 joint-admitted 2,048-row three-index recovery: scheduler_instructions={} work_instructions={} latest_work={} maximum_work={} work_samples={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        measured.scheduler_total_instructions,
        measured.work_total_instructions,
        measured.work_latest_instructions.unwrap_or_default(),
        measured.work_maximum_instructions.unwrap_or_default(),
        measured.work_samples,
        observed.memory_before.0,
        observed.memory_after.0,
        observed.memory_before.1,
        observed.memory_after.1,
    );
}

#[test]
fn joint_admission_rejects_the_first_over_limit_max_fanout_batch_before_publication() {
    let fixture = install_fixture_canister("sql_perf");
    let rejected: Result<u32, Error> = fixture
        .update_candid("reject_joint_fanout_over_boundary_fixture", ())
        .expect("over-limit maximum-fanout result should decode");
    let error = rejected.expect_err("the 241st maximum-fanout row must reject the batch");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
        })
    ));
    assert_eq!(
        error_fact(&error, DiagnosticFactTag::ActualCount),
        Some(16_388)
    );
    assert_eq!(error_fact(&error, DiagnosticFactTag::Limit), Some(16_384));

    let rejected_count = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::User,
        "SELECT id FROM PerfAuditMaxFanout ORDER BY id ASC LIMIT 1",
        1,
    )
    .expect("rejected maximum-fanout target should remain queryable");
    let rejected_outcome = summarize_perf_outcome(&rejected_count.result);
    assert_eq!(rejected_outcome.entity, "PerfAuditMaxFanout");
    assert_eq!(rejected_outcome.row_count, 0);
    let startup_probe: Result<(), Error> = fixture
        .update_candid("initialize_startup_observation_fixture", ())
        .expect("post-rejection startup probe should decode");
    startup_probe.expect("rejection before marker publication must leave startup ready");

    let loaded: Result<JointFanoutFixtureFacts, Error> = fixture
        .update_candid("load_joint_fanout_boundary_fixture", ())
        .expect("admitted maximum-fanout fixture result should decode");
    let loaded = loaded.expect("admitted maximum-fanout fixture should load");
    assert_eq!(loaded.rows, 240);
    assert_eq!(loaded.secondary_indexes_per_row, 64);
    assert!(loaded.load_local_instructions > 0);
    assert!(loaded.load_local_instructions < 10_000_000_000);
    let accepted_indexes = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::User,
        "SHOW INDEXES FROM PerfAuditMaxFanout",
        1,
    )
    .expect("Patch 1 maximum-fanout accepted indexes should remain queryable");
    let SqlQueryResult::ShowIndexes { entity, indexes } = accepted_indexes.result else {
        panic!("Patch 1 maximum-fanout fixture should return accepted indexes");
    };
    assert_eq!(entity, "PerfAuditMaxFanout");
    assert_eq!(indexes.len(), 65);

    let observed = measure_startup_watchdog_recovery(&fixture, 1);
    let measured = &observed.watchdog;
    assert!(
        measured
            .work_maximum_instructions
            .is_some_and(|instructions| instructions < 20_000_000_000)
    );

    println!(
        "0.228 joint-admitted 240-row 64-secondary-index batch: load_instructions={} scheduler_instructions={} work_instructions={} latest_work={} maximum_work={} work_samples={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        loaded.load_local_instructions,
        measured.scheduler_total_instructions,
        measured.work_total_instructions,
        measured.work_latest_instructions.unwrap_or_default(),
        measured.work_maximum_instructions.unwrap_or_default(),
        measured.work_samples,
        observed.memory_before.0,
        observed.memory_after.0,
        observed.memory_before.1,
        observed.memory_after.1,
    );
}

#[test]
fn maximum_accepted_index_publication_records_canonical_watchdog_promotion_evidence() {
    let fixture = install_fixture_canister("sql_perf");
    let mut first_id = 1_u32;
    let mut retained_load_pages = 0_u64;
    while first_id <= PROMOTION_INDEX_FIXTURE_ROWS {
        let remaining = PROMOTION_INDEX_FIXTURE_ROWS - first_id + 1;
        let canonical_setup_rows = remaining.saturating_sub(PROMOTION_INDEX_LIVE_SETUP_ROWS);
        let row_count = PROMOTION_INDEX_LOAD_PAGE_ROWS.min(if canonical_setup_rows == 0 {
            remaining
        } else {
            canonical_setup_rows
        });
        let loaded: Result<u32, Error> = fixture
            .update_candid("append_promotion_index_fixture_page", (first_id, row_count))
            .expect("promotion index load page should decode");
        assert_eq!(
            loaded.expect("promotion index load page should succeed"),
            row_count,
        );
        if canonical_setup_rows == 0 {
            retained_load_pages += 1;
        } else {
            let previous_work_samples: StartupWatchdogPerfSnapshot = fixture
                .query_candid("startup_watchdog_perf_snapshot", ())
                .expect("pre-drain watchdog performance snapshot should decode");
            advance_online_watchdog_until_work_sample(&fixture, previous_work_samples.work_samples);
        }
        first_id = first_id
            .checked_add(row_count)
            .expect("bounded fixture page should advance");
    }

    let published: Result<PromotionIndexPublicationFacts, Error> = fixture
        .update_candid("publish_promotion_index_fixture", ())
        .expect("promotion index publication facts should decode");
    let published = published.expect("promotion index publication should succeed");
    assert_eq!(
        published.rows_scanned,
        u64::from(PROMOTION_INDEX_FIXTURE_ROWS)
    );
    assert_eq!(
        published.index_keys_written,
        u64::from(PROMOTION_INDEX_FIXTURE_ROWS)
    );
    assert!(published.local_instructions > 0);
    assert!(published.local_instructions < 40_000_000_000);

    let recovered = measure_startup_watchdog_recovery(&fixture, retained_load_pages + 1);

    println!(
        "0.228 Patch 4 fingerprint-bound maximum accepted-index recovery: rows={} keys={} producer_instructions={} recovery_total={} recovery_maximum={} recovery_samples={} wasm_before={} wasm_after={} stable_before={} stable_after={}",
        published.rows_scanned,
        published.index_keys_written,
        published.local_instructions,
        recovered.watchdog.work_total_instructions,
        recovered
            .watchdog
            .work_maximum_instructions
            .unwrap_or_default(),
        recovered.watchdog.work_samples,
        recovered.memory_before.0,
        recovered.memory_after.0,
        recovered.memory_before.1,
        recovered.memory_after.1,
    );
}

#[test]
fn sql_perf_repeated_query_contracts_keep_compiled_and_shared_cache_path() {
    let fixture = install_sql_perf_canister_fixture();

    // Every repeated call should keep the same compiled-plus-shared cache path,
    // including guarded, grouped, DISTINCT, CASE, and expression-order variants.
    for scenario in repeated_query_scenarios() {
        reset_sql_perf_fixtures(&fixture);
        assert_repeat_scenario_keeps_compiled_and_shared_cache_path(&fixture, scenario);
    }
}

#[test]
fn sql_perf_membership_queries_report_compile_subphase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql) in [
        (
            "user.age.in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age ASC, id ASC LIMIT 3",
        ),
        (
            "user.age.not_in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT IN (24, 31, 43) ORDER BY id ASC LIMIT 3",
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf =
            query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, 1).unwrap_or_else(|err| {
                panic!("membership scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} key={} lookup={} parse={} tokenize={} select={} expr={} predicate={} agg_check={} prepare={} lower={} bind={} cache_insert={} execute={} total={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.compile.cache_key_local_instructions,
            perf.attribution.compile.cache_lookup_local_instructions,
            perf.attribution.compile.parse_local_instructions,
            perf.attribution.compile.parse_tokenize_local_instructions,
            perf.attribution.compile.parse_select_local_instructions,
            perf.attribution.compile.parse_expr_local_instructions,
            perf.attribution.compile.parse_predicate_local_instructions,
            perf.attribution
                .compile
                .aggregate_lane_check_local_instructions,
            perf.attribution.compile.prepare_local_instructions,
            perf.attribution.compile.lower_local_instructions,
            perf.attribution.compile.bind_local_instructions,
            perf.attribution.compile.cache_insert_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
        );

        assert!(
            perf.attribution.compile_local_instructions > 0,
            "membership scenario '{scenario_key}' should report positive compile cost",
        );
        assert_eq!(
            perf.attribution.compile.parse_local_instructions,
            perf.attribution
                .compile
                .parse_tokenize_local_instructions
                .saturating_add(perf.attribution.compile.parse_select_local_instructions)
                .saturating_add(perf.attribution.compile.parse_expr_local_instructions)
                .saturating_add(perf.attribution.compile.parse_predicate_local_instructions),
            "membership scenario '{scenario_key}' should keep parse subphases exhaustive",
        );
        assert_eq!(
            perf.attribution.structural_work.membership_authored_members, 3,
            "membership scenario '{scenario_key}' should retain the exact authored list cardinality",
        );
        assert_eq!(
            perf.attribution
                .structural_work
                .membership_normalized_members,
            3,
            "membership scenario '{scenario_key}' should retain the exact normalized list cardinality",
        );
        assert_eq!(
            perf.attribution.structural_work.membership_distinct_members, 3,
            "membership scenario '{scenario_key}' should retain the exact distinct member count",
        );
        assert_eq!(
            perf.attribution.structural_work.membership_null_members, 0,
            "membership scenario '{scenario_key}' should report no authored null member",
        );
        assert!(
            perf.attribution
                .structural_work
                .membership_canonicalization_passes
                > 0,
            "membership scenario '{scenario_key}' should attribute canonicalization work",
        );
        assert!(
            perf.attribution
                .structural_work
                .membership_members_revisited
                >= perf.attribution.structural_work.membership_authored_members,
            "membership scenario '{scenario_key}' should attribute every revisited member",
        );
    }
}

#[test]
fn sql_perf_compound_range_reports_one_bounded_physical_child() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let perf = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        "SELECT id FROM PerfAuditToken \
         WHERE collection_id = '01KV5N439P0000000000000000' \
           AND stage >= 'Draft' \
           AND stage < 'Review' \
         ORDER BY stage ASC, id ASC \
         LIMIT 50",
        1,
    )
    .expect("compound-range attribution query should succeed");
    let structural = perf.attribution.structural_work;

    assert_eq!(structural.range_conjunctions_examined, 1);
    assert_eq!(structural.range_lower_bounds_extracted, 1);
    assert_eq!(structural.range_upper_bounds_extracted, 1);
    assert_eq!(
        structural.range_physical_children_emitted, 1,
        "compatible bounds should lower to one physical range child",
    );
    assert_eq!(
        structural.residual_predicate_evaluations, 0,
        "the fully extracted compound range should not retain a runtime residual",
    );
    assert_eq!(
        perf.attribution.index_store_range_scan_calls, 1,
        "one lowered range child should execute one physical range traversal for this fixture",
    );
}

#[test]
fn sql_perf_blob_metadata_query_stays_on_covering_index() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Blob,
        "EXPLAIN EXECUTION SELECT id, label, bucket \
         FROM PerfAuditBlob \
         WHERE bucket = 10 \
         ORDER BY bucket ASC, label ASC, id ASC \
         LIMIT 3",
        1,
    )
    .expect("blob scalar metadata EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("blob scalar metadata EXPLAIN EXECUTION should return explain output");
    };

    let perf = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Blob,
        "SELECT id, label, bucket \
         FROM PerfAuditBlob \
         WHERE bucket = 10 \
         ORDER BY bucket ASC, label ASC, id ASC \
         LIMIT 3",
        1,
    )
    .expect("blob scalar metadata query should succeed");

    assert_eq!(
        perf.attribution.store_get_calls, 0,
        "blob scalar metadata query should stay on the covering index and avoid row-store get() calls: {explain}",
    );
    assert!(
        perf.attribution.pure_covering.is_some(),
        "blob scalar metadata query should report the pure covering attribution lane",
    );
    assert_eq!(
        perf.attribution.output_blob.projected_bytes, 0,
        "blob scalar metadata query should not project blob payload bytes",
    );
}

#[test]
fn sql_perf_token_branch_set_page_is_bounded_and_page_only() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_PAGE_SQL}").as_str(),
        1,
    )
    .expect("token branch-set EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("token branch-set EXPLAIN EXECUTION should return explain output");
    };

    assert!(
        explain.contains("IndexBranchSet"),
        "token branch-set EXPLAIN should expose the branch-aware route: {explain}",
    );
    assert!(
        !explain.contains("OrderByMaterializedSort"),
        "token branch-set EXPLAIN must not materialize-sort the page route: {explain}",
    );

    for (scenario_key, min_store_gets, max_store_gets) in [
        (
            "token.collection_stage_id.branch_set.page_only.limit3",
            0,
            0,
        ),
        (
            "token.collection_stage_id.branch_set.noncovered_page_only.limit3",
            3,
            4,
        ),
    ] {
        let sample = sample_perf_scenario(&fixture, sql_perf_scenario_by_key(scenario_key));
        print_branch_set_perf_sample("page-only", &sample);

        assert_eq!(
            sample.outcome.result_kind, "projection",
            "token branch-set audit row '{scenario_key}' should remain a page/projection query, not count",
        );
        assert_eq!(
            sample.outcome.row_count, 3,
            "token branch-set audit row '{scenario_key}' should return the requested page size",
        );
        assert!(
            (min_store_gets..=max_store_gets).contains(&sample.data_store_get_calls),
            "token branch-set audit row '{scenario_key}' should keep row-store get() calls bounded, got {}: {explain}",
            sample.data_store_get_calls,
        );
        assert!(
            sample.index_store_entry_reads <= 8,
            "token branch-set audit row '{scenario_key}' should keep index traversal bounded by branch fetch, got {}: {explain}",
            sample.index_store_entry_reads,
        );
        assert_eq!(
            sample.grouped_count_row_materialization_local_instructions, 0,
            "token branch-set default page query '{scenario_key}' must not invoke grouped/count materialization",
        );
        assert_eq!(
            sample.grouped_count_group_lookup_local_instructions, 0,
            "token branch-set default page query '{scenario_key}' must not invoke grouped/count lookup work",
        );
    }
}

#[test]
fn sql_perf_token_branch_set_limit50_pressure_beats_overcap_fallback() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    assert_token_branch_set_limit50_explain_contract(&fixture);
    assert_token_branch_set_limit50_fallback_rows_match(&fixture);

    let branch = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.page_only.limit50"),
    );
    let wide_branch = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.wide_page_only.limit50"),
    );
    let fallback = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.overcap_fallback.page_only.limit50"),
    );
    let pruned = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.overcap_pruned.page_only.limit50"),
    );
    let large_in_fallback = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.large_in_fallback.page_only.limit50"),
    );
    let sparse_collection_in = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_id.sparse_in.page_only.limit50"),
    );
    print_branch_set_perf_sample("limit50 branch", &branch);
    print_branch_set_perf_sample("limit50 wide branch", &wide_branch);
    print_branch_set_perf_sample("limit50 overcap fallback", &fallback);
    print_branch_set_perf_sample("limit50 overcap pruned", &pruned);
    print_branch_set_perf_sample("limit50 large IN fallback", &large_in_fallback);
    print_branch_set_perf_sample("limit50 sparse collection IN", &sparse_collection_in);
    let execute_delta = i128::from(fallback.execute_local_instructions)
        - i128::from(branch.execute_local_instructions);
    let total_delta =
        i128::from(fallback.local_instructions) - i128::from(branch.local_instructions);
    println!("branch-set perf limit50 saved: execute={execute_delta} total={total_delta}");

    assert_token_branch_set_limit50_pressure_contract(
        &branch,
        &wide_branch,
        &fallback,
        &large_in_fallback,
        &sparse_collection_in,
    );
    assert!(
        branch.structural_work.prefix_branch_cap_admissions > 0,
        "bounded branch-set planning should report at least one branch-cap admission",
    );
    assert!(
        fallback.structural_work.prefix_branch_cap_rejections > 0,
        "over-cap fallback planning should report at least one branch-cap rejection",
    );
    assert!(
        pruned.structural_work.prefix_exclusions_tested > 0,
        "over-cap pruning should report the exclusions it tests",
    );
    assert!(
        pruned.structural_work.prefix_exclusions_pruned > 0,
        "over-cap pruning should report removed branches",
    );
    assert!(
        pruned.structural_work.prefix_branch_cap_admissions > 0,
        "post-pruning branch cardinality should pass the branch cap",
    );
}

fn assert_token_branch_set_limit50_pressure_contract(
    branch: &SqlPerfScenarioSample,
    wide_branch: &SqlPerfScenarioSample,
    fallback: &SqlPerfScenarioSample,
    large_in_fallback: &SqlPerfScenarioSample,
    sparse_collection_in: &SqlPerfScenarioSample,
) {
    assert_eq!(
        branch.outcome.row_count, 50,
        "branch-set LIMIT 50 pressure query should return the requested page size",
    );
    assert_eq!(
        fallback.outcome, branch.outcome,
        "over-cap fallback comparator should return the same page result as the branch route",
    );
    assert_eq!(
        large_in_fallback.outcome, branch.outcome,
        "large-IN fallback comparator should return the same page result as the branch route",
    );
    assert_eq!(
        wide_branch.outcome, branch.outcome,
        "wide branch-set comparator should return the same page result as the small branch route",
    );
    assert_eq!(
        branch.data_store_get_calls, 0,
        "covered branch-set LIMIT 50 pressure query should avoid row-store get() calls",
    );
    assert_eq!(
        wide_branch.data_store_get_calls, 0,
        "covered wide branch-set LIMIT 50 pressure query should avoid row-store get() calls",
    );
    assert!(
        branch.index_store_entry_reads <= 128,
        "branch-set LIMIT 50 should keep index traversal bounded by the merged page, got {}",
        branch.index_store_entry_reads,
    );
    assert!(
        branch.execute_local_instructions < fallback.execute_local_instructions,
        "branch-set LIMIT 50 should execute cheaper than the over-cap fallback; branch={} fallback={}",
        branch.execute_local_instructions,
        fallback.execute_local_instructions,
    );
    assert!(
        wide_branch.execute_local_instructions < fallback.execute_local_instructions,
        "wide branch-set LIMIT 50 should execute cheaper than the over-cap fallback; wide={} fallback={}",
        wide_branch.execute_local_instructions,
        fallback.execute_local_instructions,
    );
    assert_eq!(
        large_in_fallback.data_store_get_calls, 0,
        "covered large-IN fallback should avoid row-store get() calls",
    );
    assert!(
        large_in_fallback.index_store_entry_reads <= 320,
        "large-IN fallback should stay bounded to the fixed collection prefix, got {}",
        large_in_fallback.index_store_entry_reads,
    );
    assert_eq!(
        sparse_collection_in.outcome.row_count, 50,
        "sparse collection IN audit row should return the requested page size",
    );
    assert!(
        sparse_collection_in.index_store_range_scan_calls <= 16,
        "sparse collection IN should expand only bounded non-empty child prefixes, got {} range scans",
        sparse_collection_in.index_store_range_scan_calls,
    );
    assert!(
        sparse_collection_in.index_store_entry_reads <= 128,
        "sparse collection IN should read bounded child-prefix entries, got {}",
        sparse_collection_in.index_store_entry_reads,
    );
}

fn assert_branch_set_count_sample_uses_prefix_cardinality(
    label: &str,
    sample: &SqlPerfScenarioSample,
) {
    assert_eq!(
        sample.outcome.result_kind, "projection",
        "{label} branch-set COUNT audit row should return SQL projection output",
    );
    assert_eq!(
        sample.outcome.row_count, 1,
        "{label} branch-set COUNT audit row should return one aggregate row",
    );
    assert_eq!(
        sample.data_store_get_calls, 0,
        "{label} branch COUNT should avoid row-store get() calls",
    );
    assert_eq!(
        sample.index_store_entry_reads, 0,
        "{label} branch COUNT should use prefix-cardinality metadata without scanning index entries",
    );
}

#[test]
fn sql_perf_token_branch_set_changed_queries_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    assert_token_branch_set_index_residual_explain_contract(&fixture);

    let residual = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
        ),
    );
    print_branch_set_perf_sample("index residual covering", &residual);
    assert_eq!(
        residual.outcome.result_kind, "projection",
        "branch-set index-residual audit row should remain a projection page query",
    );
    assert_eq!(
        residual.outcome.row_count, 3,
        "branch-set index-residual audit row should return the requested page size",
    );
    assert_eq!(
        residual.data_store_get_calls, 0,
        "index-residual covered branch query should stay row-store-free",
    );
    assert!(
        residual.index_store_entry_reads <= 16,
        "index-residual branch query should keep index traversal bounded by lazy branch heads, got {}",
        residual.index_store_entry_reads,
    );
    assert_eq!(
        residual.grouped_count_row_materialization_local_instructions, 0,
        "index-residual page query must not invoke grouped/count materialization",
    );
    assert_eq!(
        residual.grouped_count_group_lookup_local_instructions, 0,
        "index-residual page query must not invoke grouped/count lookup work",
    );

    let count = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.count"),
    );
    print_branch_set_perf_sample("count", &count);
    assert_branch_set_count_sample_uses_prefix_cardinality("plain", &count);

    let duplicate_count = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.duplicate_count"),
    );
    print_branch_set_perf_sample("duplicate count", &duplicate_count);
    assert_branch_set_count_sample_uses_prefix_cardinality("duplicate", &duplicate_count);

    let duplicate_count_query = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL,
        1,
    )
    .expect("token duplicate-literal branch COUNT should succeed");
    let scalar_aggregate = duplicate_count_query
        .attribution
        .scalar_aggregate
        .expect("duplicate branch COUNT should report scalar aggregate attribution");
    let duplicate_membership = duplicate_count.structural_work;
    assert!(
        duplicate_membership.membership_authored_members
            > duplicate_membership.membership_distinct_members,
        "duplicate membership attribution should distinguish authored from distinct members",
    );
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "duplicate branch COUNT should attribute the metadata-backed terminal source",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "duplicate branch COUNT should not ingest rows through the buffered reducer",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "duplicate branch COUNT should report one scalar aggregate terminal",
    );
}

#[test]
fn sql_perf_token_hybrid_covering_hotspot_counters_are_attributed() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL}")
            .as_str(),
        1,
    )
    .expect("token hybrid over-cap EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("token hybrid over-cap EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        explain.contains("cov_read_kind=Text(\"hybrid_covering\")"),
        "hybrid over-cap EXPLAIN should expose the hybrid covering route kind: {explain}",
    );
    assert!(
        explain.contains("covering_kind=Text(\"hybrid_covering\")"),
        "hybrid over-cap EXPLAIN should expose the hybrid covering terminal: {explain}",
    );
    assert!(
        explain.contains("existing_row_mode=Text(\"row_check_required\")"),
        "hybrid over-cap route should keep the accepted-index row-presence check visible: {explain}",
    );

    let sample = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
        ),
    );
    print_branch_set_perf_sample("overcap hybrid covering", &sample);

    assert_eq!(
        sample.outcome.result_kind, "projection",
        "hybrid over-cap audit row should remain a projection page query",
    );
    assert_eq!(
        sample.outcome.row_count, 50,
        "hybrid over-cap audit row should return the requested page size",
    );
    assert_eq!(
        sample.hybrid_covering_path_hits, 1,
        "hybrid over-cap audit row should report the hybrid covering path",
    );
    assert_eq!(
        sample.hybrid_covering_row_field_accesses, 50,
        "hybrid over-cap audit row should read one row-backed field per returned row",
    );
    assert_eq!(
        sample.data_store_get_calls, 50,
        "hybrid over-cap audit row should hydrate only returned rows after filtering, sorting, and windowing",
    );
    assert!(
        sample.index_store_entry_reads > sample.data_store_get_calls,
        "hybrid over-cap audit row should still attribute the pre-window index scan separately",
    );
}

fn assert_token_branch_set_limit50_explain_contract(fixture: &StandaloneCanisterFixture) {
    let branch_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token branch-set LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: branch_explain,
        ..
    } = branch_explain.result
    else {
        panic!("token branch-set LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        branch_explain.contains("IndexBranchSet"),
        "token branch-set LIMIT 50 EXPLAIN should expose the branch-aware route: {branch_explain}",
    );
    assert!(
        !branch_explain.contains("OrderByMaterializedSort"),
        "token branch-set LIMIT 50 EXPLAIN must not materialize-sort the page route: {branch_explain}",
    );

    let wide_branch_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token wide branch-set LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: wide_branch_explain,
        ..
    } = wide_branch_explain.result
    else {
        panic!("token wide branch-set LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        wide_branch_explain.contains("IndexBranchSet"),
        "token wide branch-set LIMIT 50 EXPLAIN should expose the branch-aware route: {wide_branch_explain}",
    );
    assert!(
        !wide_branch_explain.contains("OrderByMaterializedSort"),
        "token wide branch-set LIMIT 50 EXPLAIN must not materialize-sort the page route: {wide_branch_explain}",
    );

    let fallback_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token over-cap fallback LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: fallback_explain,
        ..
    } = fallback_explain.result
    else {
        panic!("token over-cap fallback LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        !fallback_explain.contains("IndexBranchSet"),
        "token over-cap fallback LIMIT 50 should not be admitted as IndexBranchSet: {fallback_explain}",
    );
    assert!(
        fallback_explain.contains("OrderByMaterializedSort"),
        "token over-cap fallback LIMIT 50 should materialize-sort after rejecting the branch route: {fallback_explain}",
    );

    let large_in_fallback_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token large-IN fallback LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: large_in_fallback_explain,
        ..
    } = large_in_fallback_explain.result
    else {
        panic!("token large-IN fallback LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        !large_in_fallback_explain.contains("IndexBranchSet"),
        "token large-IN fallback LIMIT 50 should not be admitted as IndexBranchSet: {large_in_fallback_explain}",
    );
    assert!(
        large_in_fallback_explain.contains("OrderByMaterializedSort"),
        "token large-IN fallback LIMIT 50 should remain on the over-cap fallback route: {large_in_fallback_explain}",
    );
}

fn assert_token_branch_set_index_residual_explain_contract(fixture: &StandaloneCanisterFixture) {
    let residual_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL}").as_str(),
        1,
    )
    .expect("token branch-set index-residual EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = residual_explain.result else {
        panic!("token branch-set index-residual EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        explain.contains("IndexPrefix"),
        "token branch-set index-residual EXPLAIN should expose the pruned prefix route: {explain}",
    );
    assert!(
        !explain.contains("IndexBranchSet"),
        "token branch-set index-residual EXPLAIN should prune the rejected branch before route execution: {explain}",
    );
    assert!(
        !explain.contains("OrderByMaterializedSort"),
        "token branch-set index-residual EXPLAIN must not materialize-sort the page route: {explain}",
    );
    assert!(
        explain.contains("covering_scan=true"),
        "token branch-set index-residual EXPLAIN should stay on the covering scan lane: {explain}",
    );
}

fn assert_token_branch_set_limit50_fallback_rows_match(fixture: &StandaloneCanisterFixture) {
    let branch_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL,
            1,
        )
        .expect("token branch-set LIMIT 50 query should succeed")
        .result,
    );
    let fallback_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL,
            1,
        )
        .expect("token over-cap fallback LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        fallback_rows, branch_rows,
        "over-cap fallback comparator should return the same first page as the branch route",
    );
    let large_in_fallback_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL,
            1,
        )
        .expect("token large-IN fallback LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        large_in_fallback_rows, branch_rows,
        "large-IN fallback comparator should return the same first page as the branch route",
    );
    let wide_branch_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL,
            1,
        )
        .expect("token wide branch-set LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        wide_branch_rows, branch_rows,
        "wide branch-set comparator should return the same first page as the small branch route",
    );
}

#[test]
fn sql_perf_explain_queries_report_phase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql) in [
        (
            "user.explain.lower.order.limit1",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        (
            "user.explain_execution.lower.order.limit1",
            "EXPLAIN EXECUTION SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        (
            "user.explain_json.lower.order.limit1",
            "EXPLAIN JSON SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf =
            query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, 1).unwrap_or_else(|err| {
                panic!("explain scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} planner={} store={} executor={} execute={} total={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.execution.planner_local_instructions,
            perf.attribution.execution.store_local_instructions,
            perf.attribution.execution.executor_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
        );

        assert!(
            perf.attribution.total_local_instructions > 0,
            "explain scenario '{scenario_key}' should report positive total cost",
        );
    }
}

#[test]
// Prints the parser/compile subphase breakdown for the canonical shared-floor
// rows, so the long literal scenario table stays visible in one place.
#[expect(clippy::too_many_lines)]
fn sql_perf_shared_floor_queries_report_phase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql, query_loop_count) in [
        (
            "user.pk.key_only.asc.limit1",
            "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            1,
        ),
        (
            "user.pk.order_only.asc.limit1",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            1,
        ),
        (
            "user.pk.order_only.asc.limit2",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            1,
        ),
        (
            "user.name.lower.order_only.asc.limit3",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            1,
        ),
        (
            "user.grouped.age_count.limit10",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            1,
        ),
        (
            "user.age.in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age ASC, id ASC LIMIT 3",
            1,
        ),
        (
            "user.age.not_in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT IN (24, 31, 43) ORDER BY id ASC LIMIT 3",
            1,
        ),
        (
            "repeat.user.pk.order_only.asc.limit1.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            10,
        ),
        (
            "repeat.user.pk.order_only.asc.limit2.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        (
            "repeat.user.name.lower.order_only.asc.limit3.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            10,
        ),
        (
            "repeat.user.grouped.age_count.limit10.runs10",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf = query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, query_loop_count)
            .unwrap_or_else(|err| {
                panic!("shared floor scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} key={} lookup={} parse={} tokenize={} select={} expr={} predicate={} agg_check={} prepare={} lower={} bind={} planner={} store={} executor={} execute={} total={} pure={:?} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.compile.cache_key_local_instructions,
            perf.attribution.compile.cache_lookup_local_instructions,
            perf.attribution.compile.parse_local_instructions,
            perf.attribution.compile.parse_tokenize_local_instructions,
            perf.attribution.compile.parse_select_local_instructions,
            perf.attribution.compile.parse_expr_local_instructions,
            perf.attribution.compile.parse_predicate_local_instructions,
            perf.attribution
                .compile
                .aggregate_lane_check_local_instructions,
            perf.attribution.compile.prepare_local_instructions,
            perf.attribution.compile.lower_local_instructions,
            perf.attribution.compile.bind_local_instructions,
            perf.attribution.execution.planner_local_instructions,
            perf.attribution.execution.store_local_instructions,
            perf.attribution.execution.executor_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
            perf.attribution.pure_covering,
            perf.attribution.cache.sql_compiled_command_hits,
            perf.attribution.cache.sql_compiled_command_misses,
            perf.attribution.cache.shared_query_plan_hits,
            perf.attribution.cache.shared_query_plan_misses,
        );

        assert!(
            perf.attribution.total_local_instructions > 0,
            "shared floor scenario '{scenario_key}' should report positive total cost",
        );
        let parse_subphase_total = perf
            .attribution
            .compile
            .parse_tokenize_local_instructions
            .saturating_add(perf.attribution.compile.parse_select_local_instructions)
            .saturating_add(perf.attribution.compile.parse_expr_local_instructions)
            .saturating_add(perf.attribution.compile.parse_predicate_local_instructions);
        let parse_rounding_gap = perf
            .attribution
            .compile
            .parse_local_instructions
            .abs_diff(parse_subphase_total);
        assert!(
            parse_rounding_gap <= 2,
            "shared floor scenario '{scenario_key}' should keep parse subphases exhaustive apart from averaged rounding, got parse={} subphases={parse_subphase_total}",
            perf.attribution.compile.parse_local_instructions,
        );
    }
}
