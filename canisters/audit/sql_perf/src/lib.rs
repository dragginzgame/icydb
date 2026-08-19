//!
//! Dedicated SQL perf-audit canister used only for instruction-sampling and
//! access-shape coverage.
//!

#[cfg(feature = "sql")]
use candid::{CandidType, Deserialize};
#[cfg(feature = "sql")]
use ic_cdk::query;
#[cfg(feature = "sql")]
use ic_cdk::update;
#[cfg(feature = "sql")]
use icydb::types::{Blob, Timestamp, Ulid};
#[cfg(feature = "sql")]
use icydb::value::OutputValue;
#[cfg(feature = "sql")]
use icydb::{
    ErrorCode, ErrorOrigin,
    db::{
        DynamicQuery, EntitySchemaDescription, ExhaustiveQueryPageOutput, ExhaustiveReadError,
        GroupedCountAttribution, GroupedExecutionAttribution, IntegrityCheckError,
        IntegrityCheckResult, IntegrityJobOwner, LiveQueryPageOutput, MutationJobAdvanceReceipt,
        MutationJobAdvanceRequest, MutationJobError, MutationJobId, MutationJobIdempotencyKey,
        MutationJobPhase, MutationJobState, MutationJobStatus, ReadSetRevisionError,
        ReadSetRevisionProof, SqlCompileAttribution, SqlExecutionAttribution, SqlIntegrityError,
        SqlPureCoveringAttribution, SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
        SqlStructuralWorkAttribution, StructuralMutation, StructuralPatch, WriteCell,
        query::{FieldRef, asc},
        sql::SqlQueryResult,
    },
    value::InputValue,
};
#[cfg(feature = "sql")]
use icydb_testing_audit_sql_perf_fixtures::sql_perf::{
    PerfAuditAccount, PerfAuditBlob, PerfAuditHeapUser, PerfAuditJournaledUser,
    PerfAuditMutationScoringState, PerfAuditMutationToken, PerfAuditRelationSource,
    PerfAuditRelationTarget, PerfAuditStreamingCompoundRow, PerfAuditStreamingRow, PerfAuditToken,
    PerfAuditUser,
};

#[cfg(not(feature = "test-admin-api"))]
icydb::start!();

#[cfg(feature = "test-admin-api")]
icydb::start! {
    init() => application_startup_init;
    post_upgrade() => application_startup_post_upgrade;
}

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_load(handler = load_perf_fixtures);
}

// SqlQueryPerfResult
//
// Dedicated audit envelope that preserves the SQL result payload while
// attaching one compile/execute instruction sample for the measured query call
// or one average sample across a same-call loop.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct ReadTotalOnlyPerfResult {
    row_count: u32,
    instructions: u64,
}

/// Exact schema-application work observed inside one IC message.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct SchemaApplicationPerfResult {
    local_instructions: u64,
    reconcile_checks: u64,
    first_create: u64,
    exact_match: u64,
}

/// Pure startup-state work observed inside one IC query message.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct StartupObservationPerfResult {
    state: icydb::db::DatabaseStartupState,
    local_instructions: u64,
}

/// Accepted catalog read observed after startup has reopened schema authority.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct AcceptedSchemaReadInstructionResult {
    description: EntitySchemaDescription,
    local_instructions: u64,
}

/// Canonical instruction evidence recorded by the generated startup watchdog.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
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

/// Exact admitted debt and retryable pressure observed by the closeout fixture.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct ConvergenceCloseoutDebtFacts {
    admitted_batches: u32,
    first_id: i32,
    last_admitted_id: i32,
    rejected_id: i32,
    pressure: icydb::Error,
}

/// Closed producer-side evidence for the maximum-index-fanout fixture.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct JointFanoutFixtureFacts {
    rows: u32,
    secondary_indexes_per_row: u32,
    load_local_instructions: u64,
}

/// Isolated IC instruction evidence for the dormant 0.229 selector and
/// maximum accepted-index positioned-metadata lifecycle.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct ConvergenceCandidatePerfResult {
    effects: u32,
    stores: u32,
    selected_store: u8,
    remaining_effects: u32,
    checksum: u64,
    local_instructions: u64,
}

/// Application lifecycle entry that began the current readiness observation.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
enum ApplicationStartupHook {
    Init,
    PostUpgrade,
}

/// Application-owned readiness and restoration evidence.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct ApplicationStartupSnapshot {
    hook: ApplicationStartupHook,
    engine_registered_before_hook: bool,
    observations: u32,
    recovering_observations: u32,
    ready_observations: u32,
    restorations: u32,
    retry_scheduled: bool,
    failure: Option<icydb::db::StartupFailure>,
}

#[cfg(feature = "test-admin-api")]
struct ApplicationStartupState {
    hook: ApplicationStartupHook,
    engine_registered_before_hook: bool,
    observations: u32,
    recovering_observations: u32,
    ready_observations: u32,
    restorations: u32,
    failure: Option<icydb::db::StartupFailure>,
}

#[cfg(feature = "test-admin-api")]
impl ApplicationStartupState {
    const fn new(hook: ApplicationStartupHook, engine_registered_before_hook: bool) -> Self {
        Self {
            hook,
            engine_registered_before_hook,
            observations: 0,
            recovering_observations: 0,
            ready_observations: 0,
            restorations: 0,
            failure: None,
        }
    }
}

#[cfg(feature = "test-admin-api")]
std::thread_local! {
    static APPLICATION_STARTUP_STATE: std::cell::RefCell<ApplicationStartupState> =
        const { std::cell::RefCell::new(ApplicationStartupState::new(ApplicationStartupHook::Init, false)) };
    static APPLICATION_STARTUP_TIMER: std::cell::RefCell<Option<ic_timers::OnceRegistration>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-admin-api")]
pub(crate) fn application_startup_init() {
    begin_application_startup(ApplicationStartupHook::Init);
}

#[cfg(feature = "test-admin-api")]
pub(crate) fn application_startup_post_upgrade() {
    begin_application_startup(ApplicationStartupHook::PostUpgrade);
}

#[cfg(feature = "test-admin-api")]
fn begin_application_startup(hook: ApplicationStartupHook) {
    let engine_registered_before_hook = engine_startup_watchdog_armed();
    APPLICATION_STARTUP_STATE.with(|state| {
        state.replace(ApplicationStartupState::new(
            hook,
            engine_registered_before_hook,
        ));
    });
    apply_application_startup_observation(startup_state());
}

#[cfg(feature = "test-admin-api")]
fn apply_application_startup_observation(
    observation: Result<icydb::db::DatabaseStartupState, icydb::db::StartupFailure>,
) {
    if observe_application_startup(observation) {
        schedule_application_startup_poll();
    } else {
        clear_application_startup_poll();
    }
}

#[cfg(feature = "test-admin-api")]
fn observe_application_startup(
    observation: Result<icydb::db::DatabaseStartupState, icydb::db::StartupFailure>,
) -> bool {
    APPLICATION_STARTUP_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.observations = state.observations.saturating_add(1);
        match observation {
            Ok(icydb::db::DatabaseStartupState::Ready) => {
                state.ready_observations = state.ready_observations.saturating_add(1);
                if state.restorations == 0 && state.failure.is_none() {
                    state.restorations = 1;
                }
                false
            }
            Ok(icydb::db::DatabaseStartupState::Recovering) => {
                state.recovering_observations = state.recovering_observations.saturating_add(1);
                true
            }
            Err(failure) => {
                state.failure = Some(failure);
                false
            }
        }
    })
}

#[cfg(feature = "test-admin-api")]
fn schedule_application_startup_poll() {
    APPLICATION_STARTUP_TIMER.with(|timer| {
        let mut registration = timer.borrow_mut();
        if registration.is_none() {
            let Ok(created) = ic_timers::register_once(
                application_startup_timer_identity(),
                ic_timers::DeclarationLifetime::Retained,
                |_context| async { application_startup_poll_result() },
            ) else {
                ic_cdk::trap("application startup timer registration failed");
            };
            *registration = Some(created);
        }
        let Some(registration) = registration.as_ref() else {
            ic_cdk::trap("application startup timer registration is absent");
        };
        if registration
            .ensure_scheduled(ic_timers::TimerSchedule::After(
                std::time::Duration::from_secs(1),
            ))
            .is_err()
        {
            ic_cdk::trap("application startup timer scheduling failed");
        }
    });
}

#[cfg(feature = "test-admin-api")]
fn clear_application_startup_poll() {
    APPLICATION_STARTUP_TIMER.with(|timer| {
        if let Some(registration) = timer.borrow().as_ref()
            && registration.cancel().is_err()
        {
            ic_cdk::trap("application startup timer cancellation failed");
        }
    });
}

#[cfg(feature = "test-admin-api")]
fn application_startup_poll_result() -> ic_timers::TimerRunResult {
    let recovering = observe_application_startup(startup_state());
    let directive = if recovering {
        ic_timers::TimerDirective::RetryAfter(std::time::Duration::from_secs(1))
    } else {
        ic_timers::TimerDirective::Stop
    };
    ic_timers::TimerRunResult::new(ic_timers::TimerCompletion::no_work(), directive)
}

#[cfg(feature = "test-admin-api")]
fn application_startup_timer_identity() -> ic_timers::TimerIdentity {
    match ic_timers::TimerIdentity::try_new("icydb-audit", "startup", "application-poll") {
        Ok(identity) => identity,
        Err(_) => ic_cdk::trap("application startup timer identity is invalid"),
    }
}

#[cfg(feature = "test-admin-api")]
fn application_startup_poll_scheduled() -> bool {
    APPLICATION_STARTUP_TIMER.with(|timer| {
        let registration = timer.borrow();
        let Some(registration) = registration.as_ref() else {
            return false;
        };
        match registration.has_armed_wakeup() {
            Ok(has_wakeup) => has_wakeup,
            Err(_) => ic_cdk::trap("application startup timer observation failed"),
        }
    })
}

#[cfg(feature = "test-admin-api")]
fn engine_startup_watchdog_armed() -> bool {
    let Ok(identity) = ic_timers::TimerIdentity::try_new("icydb", "startup", "recovery") else {
        ic_cdk::trap("IcyDB startup watchdog identity is invalid");
    };
    match ic_timers::timer_snapshot(&identity) {
        Ok(Some(snapshot)) => snapshot.next_deadline_ns().is_some(),
        Ok(None) => false,
        Err(_) => ic_cdk::trap("IcyDB startup watchdog observation failed"),
    }
}

#[cfg(feature = "test-admin-api")]
fn engine_startup_watchdog_perf_snapshot() -> StartupWatchdogPerfSnapshot {
    let Ok(identity) = ic_timers::TimerIdentity::try_new("icydb", "startup", "recovery") else {
        ic_cdk::trap("IcyDB startup watchdog identity is invalid");
    };
    let snapshot = match ic_timers::timer_snapshot(&identity) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => ic_cdk::trap("IcyDB startup watchdog snapshot is absent"),
        Err(_) => ic_cdk::trap("IcyDB startup watchdog observation failed"),
    };
    let observability = snapshot.observability();
    let counters = observability.counters();
    let performance = observability.performance();
    let scheduler = performance.scheduler_instructions();
    let work = performance.work_instructions();

    StartupWatchdogPerfSnapshot {
        scheduler_samples: scheduler.samples(),
        scheduler_total_instructions: scheduler.total(),
        scheduler_maximum_instructions: scheduler.maximum(),
        work_samples: work.samples(),
        work_total_instructions: work.total(),
        work_latest_instructions: work.latest(),
        work_maximum_instructions: work.maximum(),
        work_started: counters.work_started(),
        work_completed: counters.work_completed(),
        succeeded: counters.succeeded(),
        retryable_failures: counters.retryable_failure(),
        invariant_failures: counters.invariant_failure(),
    }
}

#[cfg(feature = "test-admin-api")]
fn application_startup_snapshot() -> ApplicationStartupSnapshot {
    APPLICATION_STARTUP_STATE.with(|state| {
        let state = state.borrow();
        ApplicationStartupSnapshot {
            hook: state.hook,
            engine_registered_before_hook: state.engine_registered_before_hook,
            observations: state.observations,
            recovering_observations: state.recovering_observations,
            ready_observations: state.ready_observations,
            restorations: state.restorations,
            retry_scheduled: application_startup_poll_scheduled(),
            failure: state.failure.clone(),
        }
    })
}

///
/// ScalePayloadProfile
///
/// Exact blob-payload distribution loaded by one SQL scale fixture.
/// Owned by the audit canister and returned to the host as fixture evidence.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "sql")]
enum ScalePayloadProfile {
    /// The selected surface has no blob payload fields.
    #[serde(rename = "not_applicable")]
    NotApplicable,

    /// Thumbnail lengths cycle through 32/64/128/256 bytes and chunk lengths
    /// cycle through 256/512/1,024/2,048 bytes.
    #[serde(rename = "blob_cycle_v1")]
    BlobCycleV1,
}

///
/// ScaleFixtureFacts
///
/// Realized deterministic distribution facts for one loaded scale surface.
/// Owned by the audit canister and validated by the host before sampling.
///

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct ScaleFixtureFacts {
    /// Current hard-cut scale-fixture format version.
    profile_version: u32,

    /// Stable audit surface name loaded into the otherwise-empty canister.
    surface: String,

    /// Exact number of rows constructed and inserted for the surface.
    fixture_rows: u32,

    /// Rows matching the surface's declared impossible predicate.
    zero_match_rows: u32,

    /// Rows matching the surface's declared exact-key predicate.
    one_match_rows: u32,

    /// Rows matching the surface's declared quarter-selectivity predicate.
    quarter_match_rows: u32,

    /// Rows matching the surface's declared all-row predicate.
    all_match_rows: u32,

    /// Exact blob payload distribution, or typed non-applicability.
    payload_profile: ScalePayloadProfile,
}

/// Exact deterministic source facts for the streaming executor fixture.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct StreamingExecutionFixtureFacts {
    profile_version: u32,
    seed: u64,
    fixture_rows: u32,
    lane_a_zero_rows: u32,
    lane_b_zero_rows: u32,
    sparse_overlap_rows: u32,
    empty_overlap_rows: u32,
    group_count: u32,
    wide_payload_bytes: Vec<u32>,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct StorageWritePerfResult {
    first_insert_local_instructions: u64,
    steady_insert_avg_local_instructions: u64,
    steady_update_avg_local_instructions: u64,
    steady_delete_avg_local_instructions: u64,
    write_then_read_back_local_instructions: u64,
    read_back_rows: u32,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct ConstraintActivationPerfResult {
    no_check: StorageWritePerfResult,
    add_check_local_instructions: u64,
    add_check_rows_scanned: u64,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlWriteMaterializationPerfResult {
    local_instructions: [u64; 4],
    rows: [u32; 4],
}

/// Focused durable Forward instruction and replay evidence.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
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

/// Stable Verify completion, revision-drift restart, replay, and acknowledgement evidence.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationJobVerifyResult {
    first_verify_keys_scanned: u64,
    first_verify_local_instructions: u64,
    verify_replay_local_instructions: u64,
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

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationJobReplayEvidence {
    verify_matches: bool,
    terminal_matches: bool,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationJobAcknowledgementEvidence {
    stale_rejected: bool,
    terminal_acknowledged: bool,
}

#[cfg(feature = "sql")]
struct MutationJobCompletionEvidence {
    stable_verify_local_instructions: Vec<u64>,
    restarted_forward_rows_updated: u64,
    terminal_request: MutationJobAdvanceRequest,
    terminal_receipt: MutationJobAdvanceReceipt,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationJobStartPerfResult {
    state: MutationJobState,
    local_instructions: u64,
    target_rows_changed: u32,
}

/// Fixed application phase in the collection-scale durable fixture.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "sql")]
enum MutationScaleJob {
    Tier,
    Scoring,
}

#[cfg(feature = "sql")]
impl MutationScaleJob {
    const fn discriminator(self) -> u8 {
        match self {
            Self::Tier => 81,
            Self::Scoring => 82,
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Tier => {
                "UPDATE PerfAuditMutationToken SET tier = 'Default' WHERE collection_id = 7"
            }
            Self::Scoring => {
                "UPDATE PerfAuditMutationScoringState SET score_stale = true WHERE collection_id = 7"
            }
        }
    }
}

/// One bounded fixture-load page completed by one update message.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationScaleLoadEvidence {
    first_id: u32,
    last_id: u32,
    matching_rows_loaded: u32,
    unrelated_rows_loaded: u32,
}

/// One bounded count-only fact from the scale fixture.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "sql")]
enum MutationScaleFact {
    TokenCollection,
    TokenDefault,
    TokenOther,
    TokenOtherDefault,
    ScoringCollection,
    ScoringStale,
    ScoringOther,
    ScoringOtherStale,
}

#[cfg(feature = "sql")]
impl MutationScaleFact {
    const fn sql(self) -> &'static str {
        match self {
            Self::TokenCollection => {
                "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE collection_id = 7"
            }
            Self::TokenDefault => {
                "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE collection_id = 7 AND tier = 'Default'"
            }
            Self::TokenOther => {
                "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE collection_id = 8"
            }
            Self::TokenOtherDefault => {
                "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE collection_id = 8 AND tier = 'Default'"
            }
            Self::ScoringCollection => {
                "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE collection_id = 7"
            }
            Self::ScoringStale => {
                "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE collection_id = 7 AND score_stale = true"
            }
            Self::ScoringOther => {
                "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE collection_id = 8"
            }
            Self::ScoringOtherStale => {
                "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE collection_id = 8 AND score_stale = true"
            }
        }
    }
}

/// One bounded durable advance plus its canister-local instruction cost.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationScaleAdvancePerfResult {
    receipt: MutationJobAdvanceReceipt,
    local_instructions: u64,
}

/// Guarded target-store recovery measured separately from one bounded advance.
// The test-admin Candid documentation above is frozen. Patch 4 retains the
// envelope but makes the observation state-only.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct MutationScaleRecoveryEvidence {
    complete: bool,
    warmed_rows: u32,
    local_instructions: u64,
}

/// One public integrity result plus its canister-local execution cost.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct IntegritySqlPerfResult {
    result: IntegrityCheckResult,
    local_instructions: u64,
}

#[cfg(feature = "sql")]
const STORAGE_WRITE_MATRIX_RUNS: u32 = 10;
#[cfg(feature = "sql")]
const SQL_WRITE_MATERIALIZATION_ROWS: i32 = 32;
#[cfg(feature = "sql")]
const INTEGRITY_JOURNAL_TAIL_BATCHES: i32 = 6;
#[cfg(feature = "test-admin-api")]
const CONVERGENCE_CLOSEOUT_ADMITTED_BATCHES: u32 = 38;
#[cfg(feature = "sql")]
const JOURNALED_REENTRY_PROBE_ROWS: i32 = 32;
#[cfg(feature = "sql")]
const MUTATION_SCALE_FIXTURE_ROWS: i32 = 10_001;
#[cfg(feature = "sql")]
const MUTATION_SCALE_UNRELATED_ROWS: i32 = 17;
#[cfg(feature = "sql")]
const MUTATION_SCALE_LOAD_PAGE_ROWS: i32 = 1_024;
#[cfg(feature = "sql")]
const TOKEN_TARGET_COLLECTION: &str = "01KV5N439P0000000000000000";
#[cfg(feature = "sql")]
const TOKEN_OTHER_COLLECTION: &str = "01KV5N439P1111111111111111";
#[cfg(feature = "sql")]
const SCALE_FIXTURE_PROFILE_VERSION: u32 = 1;
#[cfg(feature = "sql")]
const SCALE_FIXTURE_ROW_CARDINALITIES: &[u32] = &[16, 256, 2_048];
#[cfg(feature = "test-admin-api")]
const JOINT_ZERO_INDEX_ADMITTED_ROWS: u32 = 4_096;
#[cfg(feature = "test-admin-api")]
const JOINT_THREE_INDEX_ADMITTED_ROWS: u32 = 2_048;
#[cfg(feature = "test-admin-api")]
const PATCH1_WIDE_ROW_PAYLOAD_BYTES: usize = (4 * 1024 * 1024) - 1024;
#[cfg(feature = "test-admin-api")]
const JOINT_FANOUT_ADMITTED_ROWS: u32 = 240;
#[cfg(feature = "test-admin-api")]
const JOINT_FANOUT_FIRST_REJECTED_ROWS: u32 = 241;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_PROFILE_VERSION: u32 = 1;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_SEED: u64 = 3;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_SEED_I32: i32 = 3;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_ROWS: i32 = 2_048;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_CONTINUATION_ROWS: i32 = 10_001;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_CONTINUATION_LOAD_BATCH_ROWS: i32 = 2_048;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES: &[usize] = &[300 * 1_024, 150 * 1_024, 40 * 1_024];

#[derive(CandidType, Debug, Deserialize)]
#[cfg(feature = "sql")]
enum StreamingExhaustivePageError {
    Database(icydb::Error),
    Revision(ReadSetRevisionError),
}

#[cfg(feature = "sql")]
impl From<ExhaustiveReadError> for StreamingExhaustivePageError {
    fn from(error: ExhaustiveReadError) -> Self {
        match error {
            ExhaustiveReadError::Database(error) => Self::Database(error),
            ExhaustiveReadError::Revision(error) => Self::Revision(error),
        }
    }
}

#[cfg(feature = "sql")]
trait StructuralFixtureRow {
    const ENTITY: &'static str;

    fn into_structural_patch(self) -> StructuralPatch;
}

#[cfg(feature = "sql")]
trait StorageWriteFixtureRow: StructuralFixtureRow {
    fn primary_key_input(&self) -> InputValue;
}

#[cfg(feature = "sql")]
fn authored(value: impl Into<InputValue>) -> WriteCell<InputValue> {
    WriteCell::Value(value.into())
}

#[cfg(feature = "sql")]
fn insert_fixture_rows<R>(rows: Vec<R>) -> Result<(), icydb::Error>
where
    R: StructuralFixtureRow,
{
    if rows.is_empty() {
        return Ok(());
    }
    let expected = u32::try_from(rows.len()).map_err(|_| query_validate_error())?;
    let patches = rows
        .into_iter()
        .map(StructuralFixtureRow::into_structural_patch)
        .collect();
    let result = db()?.execute_trusted_structural_insert_batch(R::ENTITY, patches)?;
    if result.affected_rows != expected {
        return Err(query_validate_error());
    }
    Ok(())
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditUser {
    const ENTITY: &'static str = "PerfAuditUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
            .field("age_nat", authored(self.age_nat))
            .field("rank", authored(self.rank))
            .field("active", authored(self.active))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditHeapUser {
    const ENTITY: &'static str = "PerfAuditHeapUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
    }
}

#[cfg(feature = "sql")]
impl StorageWriteFixtureRow for PerfAuditHeapUser {
    fn primary_key_input(&self) -> InputValue {
        self.id.into()
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditJournaledUser {
    const ENTITY: &'static str = "PerfAuditJournaledUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
    }
}

#[cfg(feature = "sql")]
impl StorageWriteFixtureRow for PerfAuditJournaledUser {
    fn primary_key_input(&self) -> InputValue {
        self.id.into()
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditMutationToken {
    const ENTITY: &'static str = "PerfAuditMutationToken";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("collection_id", authored(self.collection_id))
            .field("tier", authored(self.tier))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditMutationScoringState {
    const ENTITY: &'static str = "PerfAuditMutationScoringState";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("collection_id", authored(self.collection_id))
            .field("score_stale", authored(self.score_stale))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditRelationTarget {
    const ENTITY: &'static str = "PerfAuditRelationTarget";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new().field("id", authored(self.id))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditRelationSource {
    const ENTITY: &'static str = "PerfAuditRelationSource";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("target_id", authored(self.target_id))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditBlob {
    const ENTITY: &'static str = "PerfAuditBlob";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("label", authored(self.label))
            .field("bucket", authored(self.bucket))
            .field("thumbnail", authored(self.thumbnail))
            .field("chunk", authored(self.chunk))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditAccount {
    const ENTITY: &'static str = "PerfAuditAccount";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("handle", authored(self.handle))
            .field("tier", authored(self.tier))
            .field("active", authored(self.active))
            .field("score", authored(self.score))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditToken {
    const ENTITY: &'static str = "PerfAuditToken";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("collection_id", authored(self.collection_id))
            .field("stage", authored(self.stage))
            .field("title", authored(self.title))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditStreamingRow {
    const ENTITY: &'static str = "PerfAuditStreamingRow";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("lane_a", authored(self.lane_a))
            .field("lane_b", authored(self.lane_b))
            .field("group_key", authored(self.group_key))
            .field("sort_key", authored(self.sort_key))
            .field("label", authored(self.label))
            .field("payload", authored(self.payload))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditStreamingCompoundRow {
    const ENTITY: &'static str = "PerfAuditStreamingCompoundRow";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("lane_a", authored(self.lane_a))
            .field("lane_b", authored(self.lane_b))
            .field("group_key", authored(self.group_key))
            .field("sort_key", authored(self.sort_key))
            .field("label", authored(self.label))
            .field("payload", authored(self.payload))
    }
}

#[cfg(feature = "sql")]
const fn query_validate_error() -> icydb::Error {
    icydb::Error::from_error_code(ErrorCode::QUERY_VALIDATE, ErrorOrigin::Query)
}

#[cfg(feature = "sql")]
const fn invalid_perf_loop_runs_error() -> icydb::Error {
    query_validate_error()
}

#[cfg(feature = "sql")]
fn validate_scale_fixture_rows(row_count: u32) -> Result<i32, icydb::Error> {
    if !SCALE_FIXTURE_ROW_CARDINALITIES.contains(&row_count) {
        return Err(query_validate_error());
    }

    i32::try_from(row_count).map_err(|_| query_validate_error())
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedCountTotals {
    borrowed_hash_computations: u64,
    bucket_candidate_checks: u64,
    existing_group_hits: u64,
    new_group_inserts: u64,
    row_materialization_local_instructions: u64,
    group_lookup_local_instructions: u64,
    existing_group_update_local_instructions: u64,
    new_group_insert_local_instructions: u64,
}

#[cfg(feature = "sql")]
impl GroupedCountTotals {
    const fn record_grouped_count(&mut self, count: GroupedCountAttribution) {
        self.borrowed_hash_computations = self
            .borrowed_hash_computations
            .saturating_add(count.borrowed_hash_computations);
        self.bucket_candidate_checks = self
            .bucket_candidate_checks
            .saturating_add(count.bucket_candidate_checks);
        self.existing_group_hits = self
            .existing_group_hits
            .saturating_add(count.existing_group_hits);
        self.new_group_inserts = self
            .new_group_inserts
            .saturating_add(count.new_group_inserts);
        self.row_materialization_local_instructions = self
            .row_materialization_local_instructions
            .saturating_add(count.row_materialization_local_instructions);
        self.group_lookup_local_instructions = self
            .group_lookup_local_instructions
            .saturating_add(count.group_lookup_local_instructions);
        self.existing_group_update_local_instructions = self
            .existing_group_update_local_instructions
            .saturating_add(count.existing_group_update_local_instructions);
        self.new_group_insert_local_instructions = self
            .new_group_insert_local_instructions
            .saturating_add(count.new_group_insert_local_instructions);
    }
}

///
/// GroupedRuntimeTotals
///
/// Accumulates executor-owned grouped runtime facts across repeated perf runs.
/// Average work counters and maximum live-state peaks are projected into the
/// final sample without making the audit canister a second runtime authority.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedRuntimeTotals {
    rows_scanned: u64,
    groups_observed: u64,
    groups_finalized: u64,
    max_peak_live_groups: u64,
    max_peak_live_aggregate_states: u64,
    max_peak_live_distinct_values: u64,
    early_scan_stop_runs: u64,
}

#[cfg(feature = "sql")]
impl GroupedRuntimeTotals {
    fn record(&mut self, grouped: GroupedExecutionAttribution) {
        self.rows_scanned = self.rows_scanned.saturating_add(grouped.rows_scanned);
        self.groups_observed = self.groups_observed.saturating_add(grouped.groups_observed);
        self.groups_finalized = self
            .groups_finalized
            .saturating_add(grouped.groups_finalized);
        self.max_peak_live_groups = self.max_peak_live_groups.max(grouped.peak_live_groups);
        self.max_peak_live_aggregate_states = self
            .max_peak_live_aggregate_states
            .max(grouped.peak_live_aggregate_states);
        self.max_peak_live_distinct_values = self
            .max_peak_live_distinct_values
            .max(grouped.peak_live_distinct_values);
        self.early_scan_stop_runs = self
            .early_scan_stop_runs
            .saturating_add(u64::from(grouped.early_scan_stop));
    }

    const fn apply_average(
        self,
        attribution: &mut GroupedExecutionAttribution,
        repeated_run_count: u64,
    ) {
        attribution.rows_scanned = self.rows_scanned / repeated_run_count;
        attribution.groups_observed = self.groups_observed / repeated_run_count;
        attribution.groups_finalized = self.groups_finalized / repeated_run_count;
        attribution.peak_live_groups = self.max_peak_live_groups;
        attribution.peak_live_aggregate_states = self.max_peak_live_aggregate_states;
        attribution.peak_live_distinct_values = self.max_peak_live_distinct_values;
        attribution.early_scan_stop = self.early_scan_stop_runs == repeated_run_count;
    }
}

#[cfg(feature = "sql")]
const fn record_structural_work(
    total: &mut SqlStructuralWorkAttribution,
    current: SqlStructuralWorkAttribution,
) {
    total.range_conjunctions_examined = total
        .range_conjunctions_examined
        .saturating_add(current.range_conjunctions_examined);
    total.range_lower_bounds_extracted = total
        .range_lower_bounds_extracted
        .saturating_add(current.range_lower_bounds_extracted);
    total.range_upper_bounds_extracted = total
        .range_upper_bounds_extracted
        .saturating_add(current.range_upper_bounds_extracted);
    total.range_physical_children_emitted = total
        .range_physical_children_emitted
        .saturating_add(current.range_physical_children_emitted);
    total.residual_predicate_evaluations = total
        .residual_predicate_evaluations
        .saturating_add(current.residual_predicate_evaluations);
    total.membership_authored_members = total
        .membership_authored_members
        .saturating_add(current.membership_authored_members);
    total.membership_normalized_members = total
        .membership_normalized_members
        .saturating_add(current.membership_normalized_members);
    total.membership_distinct_members = total
        .membership_distinct_members
        .saturating_add(current.membership_distinct_members);
    total.membership_null_members = total
        .membership_null_members
        .saturating_add(current.membership_null_members);
    total.membership_canonicalization_passes = total
        .membership_canonicalization_passes
        .saturating_add(current.membership_canonicalization_passes);
    total.membership_members_revisited = total
        .membership_members_revisited
        .saturating_add(current.membership_members_revisited);
    total.prefix_branches_before_deduplication = total
        .prefix_branches_before_deduplication
        .saturating_add(current.prefix_branches_before_deduplication);
    total.prefix_branches_after_deduplication = total
        .prefix_branches_after_deduplication
        .saturating_add(current.prefix_branches_after_deduplication);
    total.prefix_exclusions_tested = total
        .prefix_exclusions_tested
        .saturating_add(current.prefix_exclusions_tested);
    total.prefix_exclusions_pruned = total
        .prefix_exclusions_pruned
        .saturating_add(current.prefix_exclusions_pruned);
    total.prefix_branch_cap_admissions = total
        .prefix_branch_cap_admissions
        .saturating_add(current.prefix_branch_cap_admissions);
    total.prefix_branch_cap_rejections = total
        .prefix_branch_cap_rejections
        .saturating_add(current.prefix_branch_cap_rejections);
}

#[cfg(feature = "sql")]
const fn average_structural_work(
    total: SqlStructuralWorkAttribution,
    divisor: u64,
) -> SqlStructuralWorkAttribution {
    SqlStructuralWorkAttribution {
        range_conjunctions_examined: total.range_conjunctions_examined / divisor,
        range_lower_bounds_extracted: total.range_lower_bounds_extracted / divisor,
        range_upper_bounds_extracted: total.range_upper_bounds_extracted / divisor,
        range_physical_children_emitted: total.range_physical_children_emitted / divisor,
        residual_predicate_evaluations: total.residual_predicate_evaluations / divisor,
        membership_authored_members: total.membership_authored_members / divisor,
        membership_normalized_members: total.membership_normalized_members / divisor,
        membership_distinct_members: total.membership_distinct_members / divisor,
        membership_null_members: total.membership_null_members / divisor,
        membership_canonicalization_passes: total.membership_canonicalization_passes / divisor,
        membership_members_revisited: total.membership_members_revisited / divisor,
        prefix_branches_before_deduplication: total.prefix_branches_before_deduplication / divisor,
        prefix_branches_after_deduplication: total.prefix_branches_after_deduplication / divisor,
        prefix_exclusions_tested: total.prefix_exclusions_tested / divisor,
        prefix_exclusions_pruned: total.prefix_exclusions_pruned / divisor,
        prefix_branch_cap_admissions: total.prefix_branch_cap_admissions / divisor,
        prefix_branch_cap_rejections: total.prefix_branch_cap_rejections / divisor,
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
#[expect(
    clippy::field_reassign_with_default,
    reason = "perf attribution DTOs intentionally use default-backed assignment so future diagnostics counters do not break audit initializers"
)]
fn average_attribution(
    total_compile_local_instructions: u64,
    total_compile_cache_key_local_instructions: u64,
    total_compile_cache_lookup_local_instructions: u64,
    total_compile_parse_local_instructions: u64,
    total_compile_parse_tokenize_local_instructions: u64,
    total_compile_parse_select_local_instructions: u64,
    total_compile_parse_expr_local_instructions: u64,
    total_compile_parse_predicate_local_instructions: u64,
    total_compile_aggregate_lane_check_local_instructions: u64,
    total_compile_prepare_local_instructions: u64,
    total_compile_lower_local_instructions: u64,
    total_compile_bind_local_instructions: u64,
    total_compile_cache_insert_local_instructions: u64,
    total_plan_lookup_local_instructions: u64,
    total_planner_local_instructions: u64,
    total_store_local_instructions: u64,
    total_executor_invocation_local_instructions: u64,
    total_executor_local_instructions: u64,
    total_response_finalization_local_instructions: u64,
    total_pure_covering_decode_local_instructions: u64,
    total_pure_covering_row_assembly_local_instructions: u64,
    total_grouped_stream_local_instructions: u64,
    total_grouped_fold_local_instructions: u64,
    total_grouped_finalize_local_instructions: u64,
    grouped_runtime_totals: GroupedRuntimeTotals,
    total_grouped_count_borrowed_hash_computations: u64,
    total_grouped_count_bucket_candidate_checks: u64,
    total_grouped_count_existing_group_hits: u64,
    total_grouped_count_new_group_inserts: u64,
    total_grouped_count_row_materialization_local_instructions: u64,
    total_grouped_count_group_lookup_local_instructions: u64,
    total_grouped_count_existing_group_update_local_instructions: u64,
    total_grouped_count_new_group_insert_local_instructions: u64,
    total_store_get_calls: u64,
    total_index_store_get_calls: u64,
    total_index_store_range_scan_calls: u64,
    total_index_store_entry_reads: u64,
    total_structural_work: SqlStructuralWorkAttribution,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_sql_compiled_command_cache_hits: u64,
    total_sql_compiled_command_cache_misses: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    total_shared_query_plan_cache_insertions: u64,
    total_shared_query_plan_cache_evictions: u64,
    total_shared_query_plan_cache_rejected_oversize: u64,
    saw_pure_covering: bool,
    saw_grouped: bool,
    runs: u32,
) -> SqlQueryExecutionAttribution {
    let divisor = u64::from(runs);

    let mut attribution = SqlQueryExecutionAttribution::default();
    attribution.compile_local_instructions = total_compile_local_instructions / divisor;
    attribution.compile = SqlCompileAttribution {
        cache_key_local_instructions: total_compile_cache_key_local_instructions / divisor,
        cache_lookup_local_instructions: total_compile_cache_lookup_local_instructions / divisor,
        parse_local_instructions: total_compile_parse_local_instructions / divisor,
        parse_tokenize_local_instructions: total_compile_parse_tokenize_local_instructions
            / divisor,
        parse_select_local_instructions: total_compile_parse_select_local_instructions / divisor,
        parse_expr_local_instructions: total_compile_parse_expr_local_instructions / divisor,
        parse_predicate_local_instructions: total_compile_parse_predicate_local_instructions
            / divisor,
        aggregate_lane_check_local_instructions:
            total_compile_aggregate_lane_check_local_instructions / divisor,
        prepare_local_instructions: total_compile_prepare_local_instructions / divisor,
        lower_local_instructions: total_compile_lower_local_instructions / divisor,
        bind_local_instructions: total_compile_bind_local_instructions / divisor,
        cache_insert_local_instructions: total_compile_cache_insert_local_instructions / divisor,
    };
    attribution.plan_lookup_local_instructions = total_plan_lookup_local_instructions / divisor;
    attribution.execution = SqlExecutionAttribution {
        planner_local_instructions: total_planner_local_instructions / divisor,
        planner_schema_info_local_instructions: 0,
        planner_prepare_local_instructions: 0,
        planner_cache_key_local_instructions: 0,
        planner_cache_lookup_local_instructions: 0,
        planner_plan_build_local_instructions: 0,
        planner_cache_insert_local_instructions: 0,
        store_local_instructions: total_store_local_instructions / divisor,
        executor_invocation_local_instructions: total_executor_invocation_local_instructions
            / divisor,
        executor_local_instructions: total_executor_local_instructions / divisor,
        response_finalization_local_instructions: total_response_finalization_local_instructions
            / divisor,
    };
    if saw_pure_covering {
        attribution.pure_covering = Some(SqlPureCoveringAttribution {
            decode_local_instructions: total_pure_covering_decode_local_instructions / divisor,
            row_assembly_local_instructions: total_pure_covering_row_assembly_local_instructions
                / divisor,
        });
    }
    if saw_grouped {
        let mut grouped = GroupedExecutionAttribution {
            stream_local_instructions: total_grouped_stream_local_instructions / divisor,
            fold_local_instructions: total_grouped_fold_local_instructions / divisor,
            finalize_local_instructions: total_grouped_finalize_local_instructions / divisor,
            count: GroupedCountAttribution {
                borrowed_hash_computations: total_grouped_count_borrowed_hash_computations
                    / divisor,
                bucket_candidate_checks: total_grouped_count_bucket_candidate_checks / divisor,
                existing_group_hits: total_grouped_count_existing_group_hits / divisor,
                new_group_inserts: total_grouped_count_new_group_inserts / divisor,
                row_materialization_local_instructions:
                    total_grouped_count_row_materialization_local_instructions / divisor,
                group_lookup_local_instructions: total_grouped_count_group_lookup_local_instructions
                    / divisor,
                existing_group_update_local_instructions:
                    total_grouped_count_existing_group_update_local_instructions / divisor,
                new_group_insert_local_instructions:
                    total_grouped_count_new_group_insert_local_instructions / divisor,
            },
            ..GroupedExecutionAttribution::default()
        };
        grouped_runtime_totals.apply_average(&mut grouped, divisor);
        attribution.grouped = Some(grouped);
    }
    attribution.store_get_calls = total_store_get_calls / divisor;
    attribution.index_store_get_calls = total_index_store_get_calls / divisor;
    attribution.index_store_range_scan_calls = total_index_store_range_scan_calls / divisor;
    attribution.index_store_entry_reads = total_index_store_entry_reads / divisor;
    attribution.structural_work = average_structural_work(total_structural_work, divisor);
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.cache = SqlQueryCacheAttribution {
        sql_compiled_command_hits: total_sql_compiled_command_cache_hits,
        sql_compiled_command_misses: total_sql_compiled_command_cache_misses,
        shared_query_plan_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_misses: total_shared_query_plan_cache_misses,
        shared_query_plan_insertions: total_shared_query_plan_cache_insertions,
        shared_query_plan_evictions: total_shared_query_plan_cache_evictions,
        shared_query_plan_rejected_oversize: total_shared_query_plan_cache_rejected_oversize,
    };

    attribution
}
#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_entity_with_perf_loop(sql: &str, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = icydb::db!()?;
    let mut first_result = None;
    let mut total_compile_local_instructions = 0_u64;
    let mut total_compile_cache_key_local_instructions = 0_u64;
    let mut total_compile_cache_lookup_local_instructions = 0_u64;
    let mut total_compile_parse_local_instructions = 0_u64;
    let mut total_compile_parse_tokenize_local_instructions = 0_u64;
    let mut total_compile_parse_select_local_instructions = 0_u64;
    let mut total_compile_parse_expr_local_instructions = 0_u64;
    let mut total_compile_parse_predicate_local_instructions = 0_u64;
    let mut total_compile_aggregate_lane_check_local_instructions = 0_u64;
    let mut total_compile_prepare_local_instructions = 0_u64;
    let mut total_compile_lower_local_instructions = 0_u64;
    let mut total_compile_bind_local_instructions = 0_u64;
    let mut total_compile_cache_insert_local_instructions = 0_u64;
    let mut total_plan_lookup_local_instructions = 0_u64;
    let mut total_planner_local_instructions = 0_u64;
    let mut total_store_local_instructions = 0_u64;
    let mut total_executor_invocation_local_instructions = 0_u64;
    let mut total_executor_local_instructions = 0_u64;
    let mut total_response_finalization_local_instructions = 0_u64;
    let mut total_pure_covering_decode_local_instructions = 0_u64;
    let mut total_pure_covering_row_assembly_local_instructions = 0_u64;
    let mut total_grouped_stream_local_instructions = 0_u64;
    let mut total_grouped_fold_local_instructions = 0_u64;
    let mut total_grouped_finalize_local_instructions = 0_u64;
    let mut grouped_runtime_totals = GroupedRuntimeTotals::default();
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_store_get_calls = 0_u64;
    let mut total_index_store_get_calls = 0_u64;
    let mut total_index_store_range_scan_calls = 0_u64;
    let mut total_index_store_entry_reads = 0_u64;
    let mut total_structural_work = SqlStructuralWorkAttribution::default();
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_sql_compiled_command_cache_hits = 0_u64;
    let mut total_sql_compiled_command_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_insertions = 0_u64;
    let mut total_shared_query_plan_cache_evictions = 0_u64;
    let mut total_shared_query_plan_cache_rejected_oversize = 0_u64;
    let mut saw_pure_covering = false;
    let mut saw_grouped = false;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_trusted_sql_query_with_attribution(sql)?;
        if first_result.is_none() {
            first_result = Some(result);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_compile_cache_key_local_instructions = total_compile_cache_key_local_instructions
            .saturating_add(attribution.compile.cache_key_local_instructions);
        total_compile_cache_lookup_local_instructions =
            total_compile_cache_lookup_local_instructions
                .saturating_add(attribution.compile.cache_lookup_local_instructions);
        total_compile_parse_local_instructions = total_compile_parse_local_instructions
            .saturating_add(attribution.compile.parse_local_instructions);
        total_compile_parse_tokenize_local_instructions =
            total_compile_parse_tokenize_local_instructions
                .saturating_add(attribution.compile.parse_tokenize_local_instructions);
        total_compile_parse_select_local_instructions =
            total_compile_parse_select_local_instructions
                .saturating_add(attribution.compile.parse_select_local_instructions);
        total_compile_parse_expr_local_instructions = total_compile_parse_expr_local_instructions
            .saturating_add(attribution.compile.parse_expr_local_instructions);
        total_compile_parse_predicate_local_instructions =
            total_compile_parse_predicate_local_instructions
                .saturating_add(attribution.compile.parse_predicate_local_instructions);
        total_compile_aggregate_lane_check_local_instructions =
            total_compile_aggregate_lane_check_local_instructions
                .saturating_add(attribution.compile.aggregate_lane_check_local_instructions);
        total_compile_prepare_local_instructions = total_compile_prepare_local_instructions
            .saturating_add(attribution.compile.prepare_local_instructions);
        total_compile_lower_local_instructions = total_compile_lower_local_instructions
            .saturating_add(attribution.compile.lower_local_instructions);
        total_compile_bind_local_instructions = total_compile_bind_local_instructions
            .saturating_add(attribution.compile.bind_local_instructions);
        total_compile_cache_insert_local_instructions =
            total_compile_cache_insert_local_instructions
                .saturating_add(attribution.compile.cache_insert_local_instructions);
        total_plan_lookup_local_instructions = total_plan_lookup_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions);
        total_planner_local_instructions = total_planner_local_instructions
            .saturating_add(attribution.execution.planner_local_instructions);
        total_store_local_instructions = total_store_local_instructions
            .saturating_add(attribution.execution.store_local_instructions);
        total_executor_invocation_local_instructions = total_executor_invocation_local_instructions
            .saturating_add(attribution.execution.executor_invocation_local_instructions);
        total_executor_local_instructions = total_executor_local_instructions
            .saturating_add(attribution.execution.executor_local_instructions);
        total_response_finalization_local_instructions =
            total_response_finalization_local_instructions.saturating_add(
                attribution
                    .execution
                    .response_finalization_local_instructions,
            );
        if let Some(pure_covering) = attribution.pure_covering {
            saw_pure_covering = true;
            total_pure_covering_decode_local_instructions =
                total_pure_covering_decode_local_instructions
                    .saturating_add(pure_covering.decode_local_instructions);
            total_pure_covering_row_assembly_local_instructions =
                total_pure_covering_row_assembly_local_instructions
                    .saturating_add(pure_covering.row_assembly_local_instructions);
        }
        if let Some(grouped) = attribution.grouped {
            saw_grouped = true;
            total_grouped_stream_local_instructions = total_grouped_stream_local_instructions
                .saturating_add(grouped.stream_local_instructions);
            total_grouped_fold_local_instructions = total_grouped_fold_local_instructions
                .saturating_add(grouped.fold_local_instructions);
            total_grouped_finalize_local_instructions = total_grouped_finalize_local_instructions
                .saturating_add(grouped.finalize_local_instructions);
            grouped_runtime_totals.record(grouped);
            grouped_count_totals.record_grouped_count(grouped.count);
        }
        total_store_get_calls = total_store_get_calls.saturating_add(attribution.store_get_calls);
        total_index_store_get_calls =
            total_index_store_get_calls.saturating_add(attribution.index_store_get_calls);
        total_index_store_range_scan_calls = total_index_store_range_scan_calls
            .saturating_add(attribution.index_store_range_scan_calls);
        total_index_store_entry_reads =
            total_index_store_entry_reads.saturating_add(attribution.index_store_entry_reads);
        record_structural_work(&mut total_structural_work, attribution.structural_work);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_sql_compiled_command_cache_hits = total_sql_compiled_command_cache_hits
            .saturating_add(attribution.cache.sql_compiled_command_hits);
        total_sql_compiled_command_cache_misses = total_sql_compiled_command_cache_misses
            .saturating_add(attribution.cache.sql_compiled_command_misses);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.cache.shared_query_plan_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.cache.shared_query_plan_misses);
        total_shared_query_plan_cache_insertions = total_shared_query_plan_cache_insertions
            .saturating_add(attribution.cache.shared_query_plan_insertions);
        total_shared_query_plan_cache_evictions = total_shared_query_plan_cache_evictions
            .saturating_add(attribution.cache.shared_query_plan_evictions);
        total_shared_query_plan_cache_rejected_oversize =
            total_shared_query_plan_cache_rejected_oversize
                .saturating_add(attribution.cache.shared_query_plan_rejected_oversize);
    }

    Ok(SqlQueryPerfResult {
        result: first_result.expect("perf loop with runs > 0 should record one result"),
        attribution: average_attribution(
            total_compile_local_instructions,
            total_compile_cache_key_local_instructions,
            total_compile_cache_lookup_local_instructions,
            total_compile_parse_local_instructions,
            total_compile_parse_tokenize_local_instructions,
            total_compile_parse_select_local_instructions,
            total_compile_parse_expr_local_instructions,
            total_compile_parse_predicate_local_instructions,
            total_compile_aggregate_lane_check_local_instructions,
            total_compile_prepare_local_instructions,
            total_compile_lower_local_instructions,
            total_compile_bind_local_instructions,
            total_compile_cache_insert_local_instructions,
            total_plan_lookup_local_instructions,
            total_planner_local_instructions,
            total_store_local_instructions,
            total_executor_invocation_local_instructions,
            total_executor_local_instructions,
            total_response_finalization_local_instructions,
            total_pure_covering_decode_local_instructions,
            total_pure_covering_row_assembly_local_instructions,
            total_grouped_stream_local_instructions,
            total_grouped_fold_local_instructions,
            total_grouped_finalize_local_instructions,
            grouped_runtime_totals,
            grouped_count_totals.borrowed_hash_computations,
            grouped_count_totals.bucket_candidate_checks,
            grouped_count_totals.existing_group_hits,
            grouped_count_totals.new_group_inserts,
            grouped_count_totals.row_materialization_local_instructions,
            grouped_count_totals.group_lookup_local_instructions,
            grouped_count_totals.existing_group_update_local_instructions,
            grouped_count_totals.new_group_insert_local_instructions,
            total_store_get_calls,
            total_index_store_get_calls,
            total_index_store_range_scan_calls,
            total_index_store_entry_reads,
            total_structural_work,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_sql_compiled_command_cache_hits,
            total_sql_compiled_command_cache_misses,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            total_shared_query_plan_cache_insertions,
            total_shared_query_plan_cache_evictions,
            total_shared_query_plan_cache_rejected_oversize,
            saw_pure_covering,
            saw_grouped,
            runs,
        ),
    })
}
/// Clear all dedicated perf fixture rows from this canister.
#[cfg(feature = "sql")]
fn reset_perf_fixtures() -> Result<(), icydb::Error> {
    let session = db()?;
    for entity in [
        "PerfAuditRelationSource",
        "PerfAuditAccount",
        "PerfAuditBlob",
        "PerfAuditHeapUser",
        "PerfAuditJournaledUser",
        "PerfAuditMaxFanout",
        "PerfAuditMutationScoringState",
        "PerfAuditMutationToken",
        "PerfAuditRelationTarget",
        "PerfAuditStreamingCompoundRow",
        "PerfAuditStreamingRow",
        "PerfAuditToken",
        "PerfAuditUser",
    ] {
        let _ = session.execute_trusted_sql_mutation(&format!("DELETE FROM {entity}"))?;
    }

    Ok(())
}

/// Load one deterministic fixture batch tuned for SQL perf audit queries.
#[cfg(feature = "test-admin-api")]
fn load_perf_fixtures() -> Result<(), icydb::Error> {
    insert_fixture_rows(perf_audit_users())?;
    insert_fixture_rows(perf_audit_heap_users())?;
    insert_fixture_rows(perf_audit_journaled_users())?;
    insert_fixture_rows(perf_audit_blobs())?;
    insert_fixture_rows(perf_audit_accounts())?;
    insert_fixture_rows(perf_audit_tokens())?;

    Ok(())
}

/// Measure a conservative isolated form of Patch-6 candidate overhead.
///
/// The measured body performs both publication and retirement for the full
/// 65,536-key accepted-index shape, while a real fold callback performs only
/// retirement. It also scans all 38 possible store heads.
#[cfg(feature = "test-admin-api")]
#[update]
fn measure_dormant_convergence_candidate() -> ConvergenceCandidatePerfResult {
    const EFFECTS: u32 = 65_536;
    const STORES: u32 = 38;

    let start = ic_cdk::api::performance_counter(0);
    let mut positions = std::collections::BTreeMap::new();
    for target in 0..EFFECTS {
        positions.insert(target, (100_u8, 1_u64));
    }
    let selected = (0..STORES)
        .map(|ordinal| {
            let allocation_ordinal =
                u8::try_from(ordinal).expect("the fixed store ordinal should fit u8");
            if ordinal + 1 == STORES {
                (1_u64, 100_u8 + allocation_ordinal, 1_u64)
            } else {
                (
                    1_000_u64 + u64::from(ordinal),
                    100_u8 + allocation_ordinal,
                    1_u64,
                )
            }
        })
        .min()
        .expect("the fixed 38-head candidate cannot be empty");
    let mut checksum = 0_u64;
    for target in 0..EFFECTS {
        let (_, sequence) = positions
            .remove(&target)
            .expect("each measured positioned target should retire exactly");
        checksum = checksum
            .wrapping_add(u64::from(target))
            .wrapping_add(sequence);
    }
    std::hint::black_box(checksum);
    let local_instructions = ic_cdk::api::performance_counter(0).saturating_sub(start);

    ConvergenceCandidatePerfResult {
        effects: EFFECTS,
        stores: STORES,
        selected_store: selected.1,
        remaining_effects: u32::try_from(positions.len())
            .expect("the measured effect count should fit u32"),
        checksum,
        local_instructions,
    }
}

/// Load the fixed key-stream/materialization baseline fixture.
#[cfg(feature = "sql")]
#[update]
fn load_streaming_execution_fixture() -> Result<StreamingExecutionFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let rows = perf_streaming_execution_rows();
        let facts = streaming_execution_fixture_facts(rows.as_slice())?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;
        insert_fixture_rows(perf_streaming_execution_compound_rows())?;

        Ok(facts)
    })
}

/// Load the frozen 10,001-row continuation fixture without attempting to
/// process it in the same message. The bounded insert batches are setup work;
/// live and exhaustive traversal happens through separate query calls below.
#[cfg(feature = "sql")]
#[update]
fn load_streaming_execution_continuation_fixture() -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        let mut first = 1;
        while first <= STREAMING_EXECUTION_CONTINUATION_ROWS {
            let last = first
                .saturating_add(STREAMING_EXECUTION_CONTINUATION_LOAD_BATCH_ROWS - 1)
                .min(STREAMING_EXECUTION_CONTINUATION_ROWS);
            insert_fixture_rows(perf_streaming_execution_rows_range(first, last))?;
            first = last.saturating_add(1);
        }

        u32::try_from(STREAMING_EXECUTION_CONTINUATION_ROWS).map_err(|_| query_validate_error())
    })
}

/// Execute one revision-tolerant page of the frozen 10,001-row fixture.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_live_page(
    continuation: Option<String>,
) -> Result<LiveQueryPageOutput, icydb::Error> {
    icydb::db::with_request_execution(|| {
        db()?.execute_trusted_live_page(
            &streaming_execution_continuation_query(),
            continuation.as_deref(),
        )
    })
}

/// Execute one revision-strict page of the frozen 10,001-row fixture.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_exhaustive_page(
    continuation: Option<String>,
    proof: Option<ReadSetRevisionProof>,
) -> Result<ExhaustiveQueryPageOutput, StreamingExhaustivePageError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(StreamingExhaustivePageError::Database)?;
        session
            .execute_trusted_exhaustive_page(
                &streaming_execution_continuation_query(),
                continuation.as_deref(),
                proof.as_ref(),
            )
            .map_err(Into::into)
    })
}

#[cfg(feature = "sql")]
fn streaming_execution_continuation_query() -> DynamicQuery {
    DynamicQuery::new("PerfAuditStreamingRow")
        .filter(FieldRef::new("lane_a").gte(0_i32))
        .order_by(asc("id"))
        .select(["id"])
}

/// Load only the deterministic user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_users(validated_rows);
        let facts = scale_fixture_facts(
            "user",
            row_count,
            rows.len(),
            rows.iter().filter(|row| row.name.starts_with('A')).count(),
            rows.iter().filter(|row| row.id == 1).count(),
            rows.iter()
                .filter(|row| row.age >= 24 && row.age < 40)
                .count(),
            ScalePayloadProfile::NotApplicable,
        )?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic account scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_account_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_accounts(validated_rows);
        let facts = scale_fixture_facts(
            "account",
            row_count,
            rows.len(),
            rows.iter()
                .filter(|row| row.handle.starts_with('a'))
                .count(),
            rows.iter().filter(|row| row.id == 1).count(),
            rows.iter()
                .filter(|row| row.tier == "gold" && row.active)
                .count(),
            ScalePayloadProfile::NotApplicable,
        )?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic blob scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_blob_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_blobs(validated_rows);
        let facts = scale_fixture_facts(
            "blob",
            row_count,
            rows.len(),
            rows.iter()
                .filter(|row| row.label.starts_with("blob-"))
                .count(),
            rows.iter().filter(|row| row.id == 1).count(),
            rows.iter().filter(|row| row.bucket == 10).count(),
            ScalePayloadProfile::BlobCycleV1,
        )?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load a journaled wide-row surface that crosses both resumable scan and
/// exact writer-staging page boundaries without approaching the row limit.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_journaled_user_mutation_byte_fixture(row_id: u32) -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        const ROWS: i32 = 20;
        const NAME_BYTES: usize = 900 * 1024;

        let id = i32::try_from(row_id).map_err(|_| query_validate_error())?;
        if !(1..=ROWS).contains(&id) {
            return Err(query_validate_error());
        }
        if id == 1 {
            reset_perf_fixtures()?;
        }
        let mut name = format!("wide-{id:02}-");
        name.push_str(&"x".repeat(NAME_BYTES.saturating_sub(name.len())));
        insert_fixture_rows(vec![build_perf_audit_journaled_user(id, &name, 1)])?;
        Ok(row_id)
    })
}

/// Load only the deterministic heap-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_heap_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_heap_users(validated_rows);
        let facts = scale_user_mirror_fixture_facts("heap_user", row_count, &rows)?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic journaled-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_journaled_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_journaled_users(validated_rows);
        let facts = scale_journaled_user_fixture_facts(row_count, &rows)?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load the closed zero-index joint-admission boundary.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_joint_zero_index_boundary_fixture() -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let rows = perf_scale_journaled_users(
            i32::try_from(JOINT_ZERO_INDEX_ADMITTED_ROWS).map_err(|_| query_validate_error())?,
        );
        let facts = scale_journaled_user_fixture_facts(JOINT_ZERO_INDEX_ADMITTED_ROWS, &rows)?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load the closed three-index joint-admission boundary.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_joint_three_index_boundary_fixture() -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let requested_rows = JOINT_THREE_INDEX_ADMITTED_ROWS;
        let rows =
            perf_scale_users(i32::try_from(requested_rows).map_err(|_| query_validate_error())?);
        let facts = scale_fixture_facts(
            "user",
            requested_rows,
            rows.len(),
            rows.iter().filter(|row| row.name.starts_with('A')).count(),
            rows.iter().filter(|row| row.id == 1).count(),
            rows.iter()
                .filter(|row| row.age >= 24 && row.age < 40)
                .count(),
            ScalePayloadProfile::NotApplicable,
        )?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Fill the cumulative batch ceiling with one three-index row per commit.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_convergence_closeout_debt(
    first_id: i32,
) -> Result<ConvergenceCloseoutDebtFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let last_admitted_id = convergence_closeout_id(
            first_id,
            CONVERGENCE_CLOSEOUT_ADMITTED_BATCHES.saturating_sub(1),
        )?;
        let rejected_id = convergence_closeout_id(first_id, CONVERGENCE_CLOSEOUT_ADMITTED_BATCHES)?;

        for offset in 0..CONVERGENCE_CLOSEOUT_ADMITTED_BATCHES {
            let id = convergence_closeout_id(first_id, offset)?;
            insert_fixture_rows(vec![convergence_closeout_user(id)])?;
        }

        let pressure = match insert_fixture_rows(vec![convergence_closeout_user(rejected_id)]) {
            Ok(()) => return Err(query_validate_error()),
            Err(error)
                if error.code() == ErrorCode::RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE =>
            {
                error
            }
            Err(error) => return Err(error),
        };

        Ok(ConvergenceCloseoutDebtFacts {
            admitted_batches: CONVERGENCE_CLOSEOUT_ADMITTED_BATCHES,
            first_id,
            last_admitted_id,
            rejected_id,
            pressure,
        })
    })
}

/// Retry the row rejected by the populated convergence closeout fixture.
#[cfg(feature = "test-admin-api")]
#[update]
fn retry_convergence_closeout_row(id: i32) -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| insert_fixture_rows(vec![convergence_closeout_user(id)]))
}

/// Load one near-maximum row with maintained derived-index fanout.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_patch1_wide_row_recovery_fixture() -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        insert_fixture_rows(vec![PerfAuditBlob {
            id: 1,
            label: "patch1-wide-row".to_string(),
            bucket: 1,
            thumbnail: Vec::new().into(),
            chunk: vec![0xA5; PATCH1_WIDE_ROW_PAYLOAD_BYTES].into(),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        }])?;

        u32::try_from(PATCH1_WIDE_ROW_PAYLOAD_BYTES).map_err(|_| query_validate_error())
    })
}

#[cfg(feature = "test-admin-api")]
fn joint_fanout_patches(row_count: u32) -> Result<Vec<StructuralPatch>, icydb::Error> {
    (0..row_count)
        .map(|ordinal| {
            let value = i32::try_from(ordinal).map_err(|_| query_validate_error())?;
            Ok(StructuralPatch::new()
                .field("id", authored(value))
                .field("a", authored(value))
                .field("b", authored(value.saturating_add(1)))
                .field("c", authored(value.saturating_add(2)))
                .field("d", authored(value.saturating_add(3)))
                .field("e", authored(value.saturating_add(4)))
                .field("f", authored(value.saturating_add(5)))
                .field("g", authored(value.saturating_add(6)))
                .field("h", authored(value.saturating_add(7)))
                .field("i", authored(value.saturating_add(8))))
        })
        .collect::<Result<Vec<_>, icydb::Error>>()
}

/// Exercise the first rejected 64-secondary-index batch through maintained admission.
#[cfg(feature = "test-admin-api")]
#[update]
fn reject_joint_fanout_over_boundary_fixture() -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        let inserted = db()?.execute_trusted_structural_insert_batch(
            "PerfAuditMaxFanout",
            joint_fanout_patches(JOINT_FANOUT_FIRST_REJECTED_ROWS)?,
        )?;

        Ok(inserted.affected_rows)
    })
}

/// Load one closed batch at the admitted 64-secondary-index fanout boundary.
#[cfg(feature = "test-admin-api")]
#[update]
fn load_joint_fanout_boundary_fixture() -> Result<JointFanoutFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        let start = ic_cdk::api::performance_counter(1);
        let inserted = db()?.execute_trusted_structural_insert_batch(
            "PerfAuditMaxFanout",
            joint_fanout_patches(JOINT_FANOUT_ADMITTED_ROWS)?,
        )?;
        if inserted.affected_rows != JOINT_FANOUT_ADMITTED_ROWS {
            return Err(query_validate_error());
        }
        let load_local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(JointFanoutFixtureFacts {
            rows: inserted.affected_rows,
            secondary_indexes_per_row: 64,
            load_local_instructions,
        })
    })
}

/// Load only the deterministic token scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_token_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_tokens(validated_rows);
        let first_id = Ulid::from_bytes(20_001_u128.to_be_bytes());
        let facts = scale_fixture_facts(
            "token",
            row_count,
            rows.len(),
            rows.iter()
                .filter(|row| row.collection_id == "missing-collection")
                .count(),
            rows.iter().filter(|row| row.id == first_id).count(),
            rows.iter()
                .filter(|row| row.collection_id == TOKEN_TARGET_COLLECTION)
                .count(),
            ScalePayloadProfile::NotApplicable,
        )?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Return accepted runtime schema descriptions in stable audit-surface order.
#[cfg(feature = "sql")]
#[query]
fn accepted_schema_descriptions() -> Result<Vec<EntitySchemaDescription>, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let session = db()?;

        Ok(vec![
            session.try_describe_entity_by_name("PerfAuditAccount")?,
            session.try_describe_entity_by_name("PerfAuditBlob")?,
            session.try_describe_entity_by_name("PerfAuditHeapUser")?,
            session.try_describe_entity_by_name("PerfAuditJournaledUser")?,
            session.try_describe_entity_by_name("PerfAuditMutationScoringState")?,
            session.try_describe_entity_by_name("PerfAuditMutationToken")?,
            session.try_describe_entity_by_name("PerfAuditRelationSource")?,
            session.try_describe_entity_by_name("PerfAuditRelationTarget")?,
            session.try_describe_entity_by_name("PerfAuditToken")?,
            session.try_describe_entity_by_name("PerfAuditUser")?,
        ])
    })
}

#[cfg(feature = "test-admin-api")]
fn measure_schema_application() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    let session = icydb::db::DbSession::new(crate::__icydb_generated::core_db()?);
    let target = session.schema_application_target()?;
    let (first_create, exact_match) = match target.accepted_head() {
        icydb::db::ExpectedAcceptedHead::Empty => (1, 0),
        icydb::db::ExpectedAcceptedHead::Exact { .. } => (0, 1),
    };
    let start = ic_cdk::api::performance_counter(1);
    session.apply_generated_schema_fragment(
        crate::__icydb_generated::ICYDB_SCHEMA_FRAGMENT,
        crate::__icydb_generated::ICYDB_SCHEMA_MIGRATION_PLAN,
        crate::__icydb_generated::ICYDB_SCHEMA_SUBMISSION_KEY,
        crate::__icydb_generated::ICYDB_SCHEMA_ENTITY_STORES,
    )?;
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SchemaApplicationPerfResult {
        local_instructions,
        reconcile_checks: 1,
        first_create,
        exact_match,
    })
}

/// Measure schema application inside a rollback-scoped query message.
#[cfg(feature = "test-admin-api")]
#[query]
fn measure_schema_application_query() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(measure_schema_application)
}

/// Measure and persist schema application through an update message.
#[cfg(feature = "test-admin-api")]
#[update]
fn measure_schema_application_update() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(measure_schema_application)
}

/// Measure the generated pure startup observer without opening a session.
#[cfg(feature = "test-admin-api")]
#[query]
fn measure_startup_observation() -> Result<StartupObservationPerfResult, icydb::db::StartupFailure>
{
    let start = ic_cdk::api::performance_counter(1);
    let state = startup_state()?;
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(StartupObservationPerfResult {
        state,
        local_instructions,
    })
}

/// Measure one accepted entity-schema read through reopened catalog authority.
#[cfg(feature = "test-admin-api")]
#[query]
fn measure_accepted_schema_read_instructions(
    entity: String,
) -> Result<AcceptedSchemaReadInstructionResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let description = db()?.try_describe_entity_by_name(entity.as_str())?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(AcceptedSchemaReadInstructionResult {
            description,
            local_instructions,
        })
    })
}

/// Read instruction evidence from the real generated startup watchdog.
#[cfg(feature = "test-admin-api")]
#[query]
fn startup_watchdog_perf_snapshot() -> StartupWatchdogPerfSnapshot {
    engine_startup_watchdog_perf_snapshot()
}

/// Report whether the generated production watchdog retains a scheduled wake-up.
#[cfg(feature = "test-admin-api")]
#[query]
fn startup_watchdog_armed() -> bool {
    engine_startup_watchdog_armed()
}

/// Expose application-owned readiness evidence only in the local audit actor.
#[cfg(feature = "test-admin-api")]
#[query]
fn application_startup_contract() -> ApplicationStartupSnapshot {
    application_startup_snapshot()
}

/// Feed the application policy one typed observation without changing IcyDB state.
#[cfg(feature = "test-admin-api")]
#[update]
fn observe_application_startup_for_tests(
    observation: Result<icydb::db::DatabaseStartupState, icydb::db::StartupFailure>,
) {
    apply_application_startup_observation(observation);
}

/// Use the predecessor admission path solely to prepare Patch 2 readiness evidence.
// The test-admin Candid documentation above is frozen. The maintained body now
// exercises state-only ordinary admission and never initializes recovery.
#[cfg(feature = "test-admin-api")]
#[update]
fn initialize_startup_observation_fixture() -> Result<(), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result: Result<(), icydb::Error> = icydb::db::with_request_execution(|| {
        let _session = crate::__icydb_generated::db()?;
        Ok(())
    });
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    if result.as_ref().is_err_and(|error| {
        error.code() == icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
    }) {
        ic_cdk::println!(
            "icydb_0225_pending_admission local_instructions={local_instructions} ceiling=30000000"
        );
    }
    result
}

/// Trap after one canonical startup-driver attempt reaches required `Ready`.
///
/// Optional cardinality work may keep the shared watchdog active after that
/// boundary and is deliberately outside this journal-rollback probe.
#[cfg(feature = "test-admin-api")]
#[update]
fn trap_after_complete_startup_recovery() -> Result<(), icydb::Error> {
    let _optional_work_complete = icydb::db::with_request_execution(
        crate::__icydb_generated::__icydb_startup_driver_attempt_for_tests,
    )?;
    match startup_state() {
        Ok(icydb::db::DatabaseStartupState::Ready) => {
            ic_cdk::trap("intentional complete startup-recovery rollback probe")
        }
        Ok(icydb::db::DatabaseStartupState::Recovering) => {
            Err(icydb::db::__startup_recovery_pending())
        }
        Err(failure) => Err(failure.error().clone()),
    }
}

/// Load a small journaled-only fixture for same-WASM upgrade/reentry
/// instruction probes. The full SQL perf corpus intentionally remains larger
/// than this audit budget.
#[cfg(feature = "sql")]
#[update]
fn load_journaled_reentry_probe_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        insert_fixture_rows(perf_audit_journaled_reentry_probe_users())?;

        Ok(())
    })
}

/// Load one row per commit so Deep integrity must resume within a live journal
/// tail rather than merely observe an empty or single-batch tail.
#[cfg(feature = "sql")]
#[update]
fn load_journal_tail_integrity_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        for id in 1..=INTEGRITY_JOURNAL_TAIL_BATCHES {
            insert_fixture_rows(vec![build_perf_audit_journaled_user(
                id,
                &format!("integrity-journal-tail-{id:04}"),
                18 + id,
            )])?;
        }

        Ok(())
    })
}

/// Load the deterministic relation pair used by bounded integrity evidence.
#[cfg(feature = "sql")]
#[update]
fn load_relation_integrity_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        insert_fixture_rows(perf_audit_relation_targets())?;
        insert_fixture_rows(perf_audit_relation_sources())?;

        Ok(())
    })
}

/// Execute one PerfAuditUser-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_user(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditUser-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            icydb::db!()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the fully attributed path
/// while measuring the same outer canister-local boundary as the total-only
/// calibration endpoint.
#[cfg(feature = "sql")]
#[query]
fn query_user_attributed_total_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let (result, _attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the normal non-attributed
/// path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditUser-only SQL query repeatedly inside one canister
/// query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one fixed streaming-fixture query with full attribution.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Warm one fixed streaming-fixture query under update instructions.
#[cfg(feature = "sql")]
#[update]
fn warm_streaming_execution_query_with_perf(
    sql: String,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one fixed streaming-fixture query repeatedly in one request.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

#[cfg(feature = "sql")]
const fn unexpected_write_perf_count_error(
    _label: &str,
    _expected: u32,
    _actual: u32,
) -> icydb::Error {
    query_validate_error()
}

#[cfg(feature = "sql")]
const fn sql_write_result_row_count(result: &SqlQueryResult) -> Option<u32> {
    match result {
        SqlQueryResult::Count { row_count, .. } => Some(*row_count),
        SqlQueryResult::Projection(rows) => Some(rows.row_count),
        _ => None,
    }
}

#[cfg(feature = "sql")]
const fn ensure_sql_write_row_count(
    label: &str,
    result: &SqlQueryResult,
    expected: u32,
) -> Result<u32, icydb::Error> {
    let Some(actual) = sql_write_result_row_count(result) else {
        return Err(query_validate_error());
    };
    if actual != expected {
        return Err(unexpected_write_perf_count_error(label, expected, actual));
    }

    Ok(actual)
}

#[cfg(feature = "sql")]
fn measure_storage_write_matrix<E, B>(
    storage_label: &str,
    base_id: i32,
    build: B,
) -> Result<StorageWritePerfResult, icydb::Error>
where
    E: StorageWriteFixtureRow,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let session = db()?;
    let first_row = build(base_id, "first-insert", 41);
    let start = ic_cdk::api::performance_counter(1);
    session.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: E::ENTITY.to_string(),
        patch: first_row.into_structural_patch(),
    })?;
    let first_insert_local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let mut steady_insert_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let row = build(
            id,
            "steady-insert",
            42 + i32::try_from(offset % 7).unwrap_or(0),
        );
        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: E::ENTITY.to_string(),
            patch: row.into_structural_patch(),
        })?;
        steady_insert_total =
            steady_insert_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
    }

    let mut steady_update_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let row = build(
            id,
            "steady-update",
            51 + i32::try_from(offset % 7).unwrap_or(0),
        );
        let key = row.primary_key_input();
        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Update {
            entity: E::ENTITY.to_string(),
            key,
            patch: row.into_structural_patch(),
        })?;
        steady_update_total =
            steady_update_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
    }

    let mut steady_delete_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let start = ic_cdk::api::performance_counter(1);
        let deleted = session
            .execute_trusted_structural_mutation(StructuralMutation::Delete {
                entity: E::ENTITY.to_string(),
                key: id.into(),
            })?
            .affected_rows;
        steady_delete_total =
            steady_delete_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
        if deleted != 1 {
            return Err(unexpected_write_perf_count_error(storage_label, 1, deleted));
        }
    }

    let read_back_id = base_id + 10_000;
    let read_back_row = build(read_back_id, "write-read-back", 73);
    let start = ic_cdk::api::performance_counter(1);
    session.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: E::ENTITY.to_string(),
        patch: read_back_row.into_structural_patch(),
    })?;
    let response = session.execute_trusted_sql_query(&format!(
        "SELECT id FROM {} WHERE id = {read_back_id} LIMIT 1",
        E::ENTITY
    ))?;
    let write_then_read_back_local_instructions =
        ic_cdk::api::performance_counter(1).saturating_sub(start);
    let read_back_rows = sql_write_result_row_count(&response).ok_or_else(query_validate_error)?;
    if read_back_rows != 1 {
        return Err(unexpected_write_perf_count_error(
            storage_label,
            1,
            read_back_rows,
        ));
    }

    Ok(StorageWritePerfResult {
        first_insert_local_instructions,
        steady_insert_avg_local_instructions: steady_insert_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        steady_update_avg_local_instructions: steady_update_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        steady_delete_avg_local_instructions: steady_delete_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        write_then_read_back_local_instructions,
        read_back_rows,
    })
}

#[cfg(feature = "sql")]
fn sql_write_window_rows<E, B>(start_id: i32, label: &str, age: i32, build: B) -> Vec<E>
where
    B: Fn(i32, &str, i32) -> E + Copy,
{
    (0..SQL_WRITE_MATERIALIZATION_ROWS)
        .map(|offset| {
            build(
                start_id + offset,
                &format!("{label}-{offset:03}"),
                age + (offset % 7),
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn measure_sql_write_statement(
    label: &str,
    sql: &str,
    expected_rows: u32,
) -> Result<(u64, u32), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_mutation(sql)?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let row_count = ensure_sql_write_row_count(label, &result, expected_rows)?;

    Ok((instructions, row_count))
}

#[cfg(feature = "sql")]
fn measure_sql_exact_update_statement(
    label: &str,
    sql: &str,
    expected_rows: u32,
) -> Result<(u64, u32), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_exact_update(sql, expected_rows)?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let row_count = ensure_sql_write_row_count(label, &result, expected_rows)?;

    Ok((instructions, row_count))
}

#[cfg(feature = "sql")]
fn measure_sql_write_materialization_matrix<E, B>(
    entity_name: &str,
    base_id: i32,
    build: B,
) -> Result<SqlWriteMaterializationPerfResult, icydb::Error>
where
    E: StructuralFixtureRow,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let expected_rows = u32::try_from(SQL_WRITE_MATERIALIZATION_ROWS).unwrap_or(u32::MAX);
    let update_count_start = base_id + 2_000;
    let update_returning_start = base_id + 3_000;
    let delete_count_start = base_id + 4_000;
    let delete_returning_start = base_id + 5_000;

    insert_fixture_rows(sql_write_window_rows(
        update_count_start,
        "update-count",
        41,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        update_returning_start,
        "update-returning",
        51,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        delete_count_start,
        "delete-count",
        61,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        delete_returning_start,
        "delete-returning",
        71,
        build,
    ))?;

    let update_count_end = update_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let update_returning_end = update_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_count_end = delete_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_returning_end = delete_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;

    let update_count = measure_sql_exact_update_statement(
        "SQL write materialization UPDATE count",
        &format!(
            "UPDATE {entity_name} SET age = 77 \
             WHERE id >= {update_count_start} AND id < {update_count_end}"
        ),
        expected_rows,
    )?;
    let update_returning = measure_sql_exact_update_statement(
        "SQL write materialization UPDATE RETURNING",
        &format!(
            "UPDATE {entity_name} SET age = 78 \
             WHERE id >= {update_returning_start} AND id < {update_returning_end} \
             RETURNING id"
        ),
        expected_rows,
    )?;
    let delete_count = measure_sql_write_statement(
        "SQL write materialization DELETE count",
        &format!(
            "DELETE FROM {entity_name} \
             WHERE id >= {delete_count_start} AND id < {delete_count_end}"
        ),
        expected_rows,
    )?;
    let delete_returning = measure_sql_write_statement(
        "SQL write materialization DELETE RETURNING",
        &format!(
            "DELETE FROM {entity_name} \
             WHERE id >= {delete_returning_start} AND id < {delete_returning_end} \
             RETURNING id"
        ),
        expected_rows,
    )?;

    Ok(SqlWriteMaterializationPerfResult {
        local_instructions: [
            update_count.0,
            update_returning.0,
            delete_count.0,
            delete_returning.0,
        ],
        rows: [
            update_count.1,
            update_returning.1,
            delete_count.1,
            delete_returning.1,
        ],
    })
}

/// Measure the heap typed write path.
#[cfg(feature = "sql")]
#[update]
fn measure_heap_user_write_matrix_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditHeapUser, _>(
            "heap write matrix",
            30_000,
            build_perf_audit_heap_user,
        )
    })
}

/// Measure the journaled typed write path.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_write_matrix_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled write matrix",
            40_000,
            build_perf_audit_journaled_user,
        )
    })
}

/// Measure the matched journaled typed-write path before and after one simple
/// accepted check, including the exact bounded publication scan.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_constraint_write_perf()
-> Result<ConstraintActivationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let no_check = measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled no-check write matrix",
            70_000,
            build_perf_audit_journaled_user,
        )?;

        let start = ic_cdk::api::performance_counter(1);
        let add_result = db()?.execute_admin_sql_ddl(
            "ALTER TABLE PerfAuditJournaledUser ADD CONSTRAINT \
         perf_audit_age_nonnegative CHECK (age >= 0) NOT VALID \
         EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
        )?;
        let add_check_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        let SqlQueryResult::Ddl {
            rows_scanned: add_check_rows_scanned,
            ..
        } = add_result
        else {
            return Err(query_validate_error());
        };

        Ok(ConstraintActivationPerfResult {
            no_check,
            add_check_local_instructions,
            add_check_rows_scanned,
        })
    })
}

/// Measure the journaled typed-write path after the preceding audit call has
/// published its simple accepted check.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_checked_write_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled checked write matrix",
            90_000,
            build_perf_audit_journaled_user,
        )
    })
}

/// Advance the audit-only journaled check activation to accepted authority.
#[cfg(feature = "sql")]
#[update]
fn validate_journaled_user_perf_check() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        const MAX_VALIDATION_STEPS: usize = 4;

        for _ in 0..MAX_VALIDATION_STEPS {
            let result = db()?.execute_admin_sql_ddl(
                "ALTER TABLE PerfAuditJournaledUser \
             VALIDATE CONSTRAINT perf_audit_age_nonnegative",
            )?;
            if matches!(
                result,
                SqlQueryResult::Ddl {
                    constraint_validation: Some(ref validation),
                    ..
                } if validation.complete
            ) {
                return Ok(());
            }
        }

        Err(query_validate_error())
    })
}

/// Measure broad SQL write materialization shapes against heap storage.
#[cfg(feature = "sql")]
#[update]
fn measure_heap_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_sql_write_materialization_matrix::<PerfAuditHeapUser, _>(
            "PerfAuditHeapUser",
            50_000,
            build_perf_audit_heap_user,
        )
    })
}

/// Measure broad SQL write materialization shapes against journaled storage.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_sql_write_materialization_matrix::<PerfAuditJournaledUser, _>(
            "PerfAuditJournaledUser",
            60_000,
            build_perf_audit_journaled_user,
        )
    })
}

#[cfg(feature = "sql")]
fn mutation_scale_job_id(job: MutationScaleJob) -> Result<MutationJobId, MutationJobError> {
    let mut bytes = [0; 32];
    bytes[31] = job.discriminator();
    MutationJobId::try_from_bytes(bytes)
}

#[cfg(feature = "sql")]
fn mutation_scale_count(sql: &str) -> Result<u32, icydb::Error> {
    let SqlQueryResult::Projection(projection) = db()?.execute_trusted_sql_query(sql)? else {
        return Err(query_validate_error());
    };
    let [row] = projection.rows.as_slice() else {
        return Err(query_validate_error());
    };
    let [OutputValue::Nat64(count)] = row.as_slice() else {
        return Err(query_validate_error());
    };

    u32::try_from(*count).map_err(|_| query_validate_error())
}

/// Load one bounded page of the fixed 10,001-row tier and scoring fixture.
#[cfg(feature = "sql")]
#[update]
fn load_collection_mutation_scale_page(
    first_id: u32,
    row_count: u32,
) -> Result<MutationScaleLoadEvidence, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let first_id = i32::try_from(first_id).map_err(|_| query_validate_error())?;
        let row_count = i32::try_from(row_count).map_err(|_| query_validate_error())?;
        if first_id < 1 || !(1..=MUTATION_SCALE_LOAD_PAGE_ROWS).contains(&row_count) {
            return Err(query_validate_error());
        }
        let last_id = first_id
            .checked_add(row_count - 1)
            .ok_or_else(query_validate_error)?;
        if last_id > MUTATION_SCALE_FIXTURE_ROWS {
            return Err(query_validate_error());
        }
        if first_id == 1 {
            reset_perf_fixtures()?;
        }

        insert_fixture_rows(perf_audit_mutation_tokens(first_id, last_id))?;
        insert_fixture_rows(perf_audit_mutation_scoring_states(first_id, last_id))?;
        let unrelated_rows_loaded = if first_id == 1 {
            insert_fixture_rows(perf_audit_unrelated_mutation_tokens())?;
            insert_fixture_rows(perf_audit_unrelated_mutation_scoring_states())?;
            u32::try_from(MUTATION_SCALE_UNRELATED_ROWS).map_err(|_| query_validate_error())?
        } else {
            0
        };

        Ok(MutationScaleLoadEvidence {
            first_id: u32::try_from(first_id).map_err(|_| query_validate_error())?,
            last_id: u32::try_from(last_id).map_err(|_| query_validate_error())?,
            matching_rows_loaded: u32::try_from(row_count).map_err(|_| query_validate_error())?,
            unrelated_rows_loaded,
        })
    })
}

/// Return one count-only fact without resetting the aggregate request budget.
#[cfg(feature = "sql")]
#[query]
fn collection_mutation_scale_fact(fact: MutationScaleFact) -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| mutation_scale_count(fact.sql()))
}

/// Advance one engine-owned startup-recovery page after upgrade without
/// attributing that work to a bounded mutation-job page.
// The test-admin Candid documentation above is frozen. This method now only
// observes lifecycle-driven progress; it owns no recovery page.
#[cfg(feature = "sql")]
#[update]
fn recover_collection_mutation_scale_store() -> Result<MutationScaleRecoveryEvidence, icydb::Error>
{
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let ready = match startup_state() {
            Ok(icydb::db::DatabaseStartupState::Ready) => true,
            Ok(icydb::db::DatabaseStartupState::Recovering) => false,
            Err(failure) => return Err(failure.error().clone()),
        };
        let warmed_rows = if ready {
            let session = crate::__icydb_generated::db()?;
            let result = session.execute_trusted_sql_query(
                "SELECT id FROM PerfAuditMutationToken WHERE id = 1 LIMIT 1",
            )?;
            sql_write_result_row_count(&result).ok_or_else(query_validate_error)?
        } else {
            0
        };
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(MutationScaleRecoveryEvidence {
            complete: ready,
            warmed_rows,
            local_instructions,
        })
    })
}

/// Prove that the one-shot 10,001-row assertion is rejected before mutation.
#[cfg(feature = "sql")]
#[update]
fn try_collection_eager_tier_reset() -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        db()?.execute_trusted_sql_exact_update(
            MutationScaleJob::Tier.sql(),
            u32::try_from(MUTATION_SCALE_FIXTURE_ROWS).map_err(|_| query_validate_error())?,
        )
    })
}

/// Start or exactly replay one fixed collection-scale application phase.
#[cfg(feature = "sql")]
#[update]
fn start_collection_mutation_scale_job(
    job: MutationScaleJob,
) -> Result<MutationJobState, MutationJobError> {
    icydb::db::with_request_execution(|| {
        db().map_err(|_| MutationJobError::Internal)?
            .start_trusted_sql_mutation_job(mutation_scale_job_id(job)?, job.sql())
    })
}

/// Advance exactly one bounded page of one collection-scale application phase.
#[cfg(feature = "sql")]
#[update]
fn advance_collection_mutation_scale_job(
    job: MutationScaleJob,
    expected_sequence: u64,
    idempotency_key: String,
) -> Result<MutationScaleAdvancePerfResult, MutationJobError> {
    icydb::db::with_request_execution(|| {
        // Match the Patch 1 measurement authority: session establishment is
        // request setup, while this sample owns exactly one engine advance.
        let session = db().map_err(|_| MutationJobError::Internal)?;
        let request = MutationJobAdvanceRequest::new(
            mutation_scale_job_id(job)?,
            expected_sequence,
            MutationJobIdempotencyKey::new(idempotency_key)?,
        );
        let start = ic_cdk::api::performance_counter(1);
        let receipt = session.advance_trusted_mutation_job(&request)?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(MutationScaleAdvancePerfResult {
            receipt,
            local_instructions,
        })
    })
}

/// Load current count-only public state for one collection-scale application phase.
/// This audit surface is an update so a post-upgrade call retains the IC's
/// update headroom while the current guarded-reentry contract recovers a large
/// journal tail.
#[cfg(feature = "sql")]
#[update]
fn collection_mutation_scale_job_state(
    job: MutationScaleJob,
) -> Result<MutationJobState, MutationJobError> {
    icydb::db::with_request_execution(|| {
        db().map_err(|_| MutationJobError::Internal)?
            .mutation_job_state(mutation_scale_job_id(job)?)
    })
}

/// Acknowledge one terminal scale job; repeating after response loss is safe.
#[cfg(feature = "sql")]
#[update]
fn acknowledge_collection_mutation_scale_job(
    job: MutationScaleJob,
    expected_sequence: u64,
) -> Result<(), MutationJobError> {
    icydb::db::with_request_execution(|| {
        db().map_err(|_| MutationJobError::Internal)?
            .acknowledge_mutation_job(mutation_scale_job_id(job)?, expected_sequence)
    })
}

/// Measure durable Forward convergence without exposing private intent or continuation bytes.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_mutation_forward_perf()
-> Result<MutationJobForwardPerfResult, MutationJobError> {
    icydb::db::with_request_execution(|| {
        const MAX_STEPS: usize = 16;

        let session = db().map_err(|_| MutationJobError::Internal)?;
        let sql = "UPDATE PerfAuditJournaledUser SET name = 'resumable-measured' WHERE age >= 0";
        let mut job_bytes = [0; 32];
        job_bytes[31] = 73;
        let job_id = MutationJobId::try_from_bytes(job_bytes)?;
        let start = ic_cdk::api::performance_counter(1);
        let state = session.start_trusted_sql_mutation_job(job_id, sql)?;
        let start_local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let mut sequence = state.sequence;
        let mut forward_local_instructions = Vec::new();
        let mut forward_keys_scanned = 0_u64;
        let mut rows_updated = 0_u64;
        let mut forward_keys_scanned_per_step = Vec::new();
        let mut rows_updated_per_step = Vec::new();

        for _ in 0..MAX_STEPS {
            let request = MutationJobAdvanceRequest::new(
                job_id,
                sequence,
                MutationJobIdempotencyKey::new(format!("forward-{sequence}"))?,
            );
            let start = ic_cdk::api::performance_counter(1);
            let receipt = session.advance_trusted_mutation_job(&request)?;
            let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
            forward_local_instructions.push(instructions);
            forward_keys_scanned = forward_keys_scanned.saturating_add(receipt.keys_scanned);
            rows_updated = rows_updated.saturating_add(receipt.rows_updated);
            forward_keys_scanned_per_step.push(receipt.keys_scanned);
            rows_updated_per_step.push(receipt.rows_updated);
            sequence = receipt.committed_sequence;
            if receipt.phase == MutationJobPhase::Verify {
                let start = ic_cdk::api::performance_counter(1);
                let replay = session.advance_trusted_mutation_job(&request)?;
                let replay_local_instructions =
                    ic_cdk::api::performance_counter(1).saturating_sub(start);
                let timestamp_groups = match session
                    .execute_trusted_sql_query(
                        "SELECT updated_at, COUNT(*) FROM PerfAuditJournaledUser \
                         WHERE name = 'resumable-measured' \
                         GROUP BY updated_at ORDER BY updated_at ASC LIMIT 2",
                    )
                    .map_err(|_| MutationJobError::Internal)?
                {
                    SqlQueryResult::Grouped(groups) => groups.row_count,
                    _ => return Err(MutationJobError::Internal),
                };

                let mut zero_job_bytes = [0; 32];
                zero_job_bytes[31] = 74;
                let zero_job_id = MutationJobId::try_from_bytes(zero_job_bytes)?;
                session.start_trusted_sql_mutation_job(zero_job_id, sql)?;
                let zero_request = MutationJobAdvanceRequest::new(
                    zero_job_id,
                    0,
                    MutationJobIdempotencyKey::new("zero-candidate-0")?,
                );
                let zero_receipt = session.advance_trusted_mutation_job(&zero_request)?;
                let stale_request = MutationJobAdvanceRequest::new(
                    zero_job_id,
                    0,
                    MutationJobIdempotencyKey::new("stale-after-zero-candidate")?,
                );
                let stale_rejected = matches!(
                    session.advance_trusted_mutation_job(&stale_request),
                    Err(MutationJobError::StaleSequence {
                        expected: 0,
                        actual: 1,
                    })
                );
                let stale_request_preserved_sequence = stale_rejected
                    && session.mutation_job_state(zero_job_id)?.sequence
                        == zero_receipt.committed_sequence;
                return Ok(MutationJobForwardPerfResult {
                    start_local_instructions,
                    forward_local_instructions,
                    replay_local_instructions,
                    forward_keys_scanned,
                    rows_updated,
                    forward_keys_scanned_per_step,
                    rows_updated_per_step,
                    committed_sequence: sequence,
                    replay_matches: receipt == replay,
                    zero_candidate_keys_scanned: zero_receipt.keys_scanned,
                    zero_candidate_rows_updated: zero_receipt.rows_updated,
                    zero_candidate_sequence: zero_receipt.committed_sequence,
                    stale_request_preserved_sequence,
                    operation_timestamp_groups: timestamp_groups,
                });
            }
        }

        Err(MutationJobError::Internal)
    })
}

#[cfg(feature = "sql")]
fn advance_audit_mutation_job_to_verify(
    session: &icydb::db::DbSession<crate::__icydb_generated::__IcydbGeneratedCanister>,
    job_id: MutationJobId,
    mut sequence: u64,
) -> Result<u64, MutationJobError> {
    const MAX_FORWARD_STEPS: usize = 16;

    for _ in 0..MAX_FORWARD_STEPS {
        let request = MutationJobAdvanceRequest::new(
            job_id,
            sequence,
            MutationJobIdempotencyKey::new(format!("verify-forward-{sequence}"))?,
        );
        let receipt = session.advance_trusted_mutation_job(&request)?;
        sequence = receipt.committed_sequence;
        if receipt.phase == MutationJobPhase::Verify {
            return Ok(sequence);
        }
    }

    Err(MutationJobError::Internal)
}

#[cfg(feature = "sql")]
fn complete_audit_mutation_job_after_restart(
    session: &icydb::db::DbSession<crate::__icydb_generated::__IcydbGeneratedCanister>,
    job_id: MutationJobId,
    mut sequence: u64,
) -> Result<MutationJobCompletionEvidence, MutationJobError> {
    const MAX_RESTART_STEPS: usize = 8;

    let mut restarted_forward_rows_updated = 0_u64;
    let mut phase = MutationJobPhase::Forward;
    let mut stable_verify_local_instructions = Vec::new();
    for _ in 0..MAX_RESTART_STEPS {
        let request = MutationJobAdvanceRequest::new(
            job_id,
            sequence,
            MutationJobIdempotencyKey::new(format!("verify-resume-{sequence}"))?,
        );
        let start = ic_cdk::api::performance_counter(1);
        let receipt = session.advance_trusted_mutation_job(&request)?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        if phase == MutationJobPhase::Verify {
            stable_verify_local_instructions.push(local_instructions);
        }
        restarted_forward_rows_updated =
            restarted_forward_rows_updated.saturating_add(receipt.rows_updated);
        sequence = receipt.committed_sequence;
        phase = receipt.phase;
        if receipt.status == MutationJobStatus::Completed {
            return Ok(MutationJobCompletionEvidence {
                stable_verify_local_instructions,
                restarted_forward_rows_updated,
                terminal_request: request,
                terminal_receipt: receipt,
            });
        }
    }

    Err(MutationJobError::Internal)
}

#[cfg(feature = "sql")]
fn inject_audit_mutation_revision_drift(
    session: &icydb::db::DbSession<crate::__icydb_generated::__IcydbGeneratedCanister>,
) -> Result<(), MutationJobError> {
    session
        .execute_trusted_sql_exact_update(
            "UPDATE PerfAuditJournaledUser SET name = 'verify-drift' WHERE id = 1",
            1,
        )
        .map(|_| ())
        .map_err(|_| MutationJobError::TargetMutationFailed)
}

/// Exercise stable Verify, an intervening target write, replay, and terminal acknowledgement.
#[cfg(feature = "sql")]
#[update]
fn verify_journaled_user_mutation_job_lifecycle()
-> Result<MutationJobVerifyResult, MutationJobError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(|_| MutationJobError::Internal)?;
        let sql = "UPDATE PerfAuditJournaledUser SET name = 'verify-measured' WHERE age >= 0";
        let mut job_bytes = [0; 32];
        job_bytes[31] = 75;
        let job_id = MutationJobId::try_from_bytes(job_bytes)?;
        let sequence = session
            .start_trusted_sql_mutation_job(job_id, sql)?
            .sequence;
        let mut sequence = advance_audit_mutation_job_to_verify(&session, job_id, sequence)?;

        let first_verify_request = MutationJobAdvanceRequest::new(
            job_id,
            sequence,
            MutationJobIdempotencyKey::new(format!("verify-page-{sequence}"))?,
        );
        let start = ic_cdk::api::performance_counter(1);
        let first_verify = session.advance_trusted_mutation_job(&first_verify_request)?;
        let first_verify_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        if first_verify.phase != MutationJobPhase::Verify
            || first_verify.status != MutationJobStatus::Active
        {
            return Err(MutationJobError::Internal);
        }
        let start = ic_cdk::api::performance_counter(1);
        let first_verify_replay = session.advance_trusted_mutation_job(&first_verify_request)?;
        let verify_replay_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        sequence = first_verify.committed_sequence;

        inject_audit_mutation_revision_drift(&session)?;

        let drift_request = MutationJobAdvanceRequest::new(
            job_id,
            sequence,
            MutationJobIdempotencyKey::new(format!("verify-drift-{sequence}"))?,
        );
        let start = ic_cdk::api::performance_counter(1);
        let drift_restart = session.advance_trusted_mutation_job(&drift_request)?;
        let drift_restart_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        if drift_restart.phase != MutationJobPhase::Forward
            || drift_restart.status != MutationJobStatus::Active
            || drift_restart.verify_restarts_total != 1
        {
            return Err(MutationJobError::Internal);
        }
        sequence = drift_restart.committed_sequence;
        let MutationJobCompletionEvidence {
            stable_verify_local_instructions,
            restarted_forward_rows_updated,
            terminal_request,
            terminal_receipt,
        } = complete_audit_mutation_job_after_restart(&session, job_id, sequence)?;
        sequence = terminal_receipt.committed_sequence;
        let start = ic_cdk::api::performance_counter(1);
        let terminal_state = session.mutation_job_state(job_id)?;
        let state_local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        if terminal_state.status != MutationJobStatus::Completed {
            return Err(MutationJobError::Internal);
        }
        let start = ic_cdk::api::performance_counter(1);
        let terminal_replay = session.advance_trusted_mutation_job(&terminal_request)?;
        let terminal_replay_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        let stale_acknowledgement_rejected = matches!(
            session.acknowledge_mutation_job(job_id, sequence.saturating_sub(1)),
            Err(MutationJobError::StaleSequence { .. })
        );
        let start = ic_cdk::api::performance_counter(1);
        session.acknowledge_mutation_job(job_id, sequence)?;
        let acknowledgement_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        session.acknowledge_mutation_job(job_id, sequence)?;
        let terminal_acknowledged = matches!(
            session.mutation_job_state(job_id),
            Err(MutationJobError::NotFound)
        );

        Ok(MutationJobVerifyResult {
            first_verify_keys_scanned: first_verify.keys_scanned,
            first_verify_local_instructions,
            verify_replay_local_instructions,
            drift_restart_keys_scanned: drift_restart.keys_scanned,
            drift_restart_local_instructions,
            stable_verify_local_instructions,
            verify_restarts_total: terminal_receipt.verify_restarts_total,
            restarted_forward_rows_updated,
            completed_sequence: terminal_receipt.committed_sequence,
            state_local_instructions,
            terminal_replay_local_instructions,
            acknowledgement_local_instructions,
            replay: MutationJobReplayEvidence {
                verify_matches: first_verify == first_verify_replay,
                terminal_matches: terminal_receipt == terminal_replay,
            },
            acknowledgement: MutationJobAcknowledgementEvidence {
                stale_rejected: stale_acknowledgement_rejected,
                terminal_acknowledged,
            },
        })
    })
}

/// Start or replay one fixed audit mutation intent without advancing it.
#[cfg(feature = "sql")]
#[update]
fn start_journaled_user_mutation_job(
    job_discriminator: u8,
    intent_discriminator: u8,
) -> Result<MutationJobStartPerfResult, MutationJobError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(|_| MutationJobError::Internal)?;
        let mut job_bytes = [0; 32];
        job_bytes[31] = job_discriminator;
        let job_id = MutationJobId::try_from_bytes(job_bytes)?;
        let sql = match intent_discriminator {
            0 => "UPDATE PerfAuditJournaledUser SET name = 'durable-start' WHERE age >= 0",
            1 => "UPDATE PerfAuditJournaledUser SET name = 'different-start' WHERE age >= 0",
            2 => "UPDATE PerfAuditHeapUser SET name = 'heap-start' WHERE age >= 0",
            3 => "  update PerfAuditJournaledUser set name='durable-start' where age >= 0  ",
            4 => "UPDATE PerfAuditJournaledUser SET age = 2 WHERE id < 0",
            5 => "UPDATE PerfAuditJournaledUser SET age = 2 WHERE id > 0",
            6 => "UPDATE PerfAuditMaxFanout SET a = 1001 WHERE id = 1",
            _ => return Err(MutationJobError::IneligibleIntent),
        };
        let start = ic_cdk::api::performance_counter(1);
        let state = session.start_trusted_sql_mutation_job(job_id, sql)?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let target_rows_changed = sql_write_result_row_count(&session
            .execute_trusted_sql_query(
                "SELECT id FROM PerfAuditJournaledUser WHERE name = 'durable-start' ORDER BY id LIMIT 1",
            )
            .map_err(|_| MutationJobError::Internal)?)
        .ok_or(MutationJobError::Internal)?;
        Ok(MutationJobStartPerfResult {
            state,
            local_instructions,
            target_rows_changed,
        })
    })
}

/// Apply one later ordinary write used by the managed-time mutation-job proof.
#[cfg(feature = "test-admin-api")]
#[update]
fn update_journaled_user_after_mutation_job_start() -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let result = db()?.execute_trusted_sql_exact_update(
            "UPDATE PerfAuditJournaledUser SET name = 'later-managed-write' WHERE id = 1",
            1,
        )?;
        sql_write_result_row_count(&result).ok_or_else(query_validate_error)
    })
}

/// Advance one managed-time proof job in exactly one canister message.
#[cfg(feature = "test-admin-api")]
#[update]
fn advance_journaled_user_mutation_job(
    job_discriminator: u8,
    expected_sequence: u64,
    idempotency_key: String,
) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
    icydb::db::with_request_execution(|| {
        let mut job_bytes = [0; 32];
        job_bytes[31] = job_discriminator;
        let request = MutationJobAdvanceRequest::new(
            MutationJobId::try_from_bytes(job_bytes)?,
            expected_sequence,
            MutationJobIdempotencyKey::new(idempotency_key)?,
        );
        db().map_err(|_| MutationJobError::Internal)?
            .advance_trusted_mutation_job(&request)
    })
}

/// Measure one canonical administrative integrity SQL operation.
#[cfg(feature = "sql")]
#[update]
// This audit endpoint deliberately exposes the canonical typed integrity
// error. Boxing it would change the generated Candid response contract.
#[allow(clippy::result_large_err)]
fn measure_integrity_sql_perf(sql: String) -> Result<IntegritySqlPerfResult, SqlIntegrityError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(SqlIntegrityError::Sql)?;
        let owner = IntegrityJobOwner::new("audit::sql-perf")
            .map_err(IntegrityCheckError::Job)
            .map_err(SqlIntegrityError::Integrity)?;
        let start = ic_cdk::api::performance_counter(1);
        let result = session.execute_admin_integrity_sql(sql.as_str(), owner)?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(IntegritySqlPerfResult {
            result,
            local_instructions,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_heap_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditHeapUser-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditJournaledUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditJournaledUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_total_only_perf(
    sql: String,
) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute the journaled LIMIT 1 shape through an update call. After a
/// same-WASM upgrade this gives the integration harness one normal guarded
/// reentry probe that includes any required recovery/rebuild work.
// Patch 4 moves recovery into the watchdog envelope. The retained test-admin
// method measures only the post-ready query path.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_reentry_perf() -> Result<ReadTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let response = db()?.execute_trusted_sql_query(
            "SELECT id FROM PerfAuditJournaledUser ORDER BY id LIMIT 1",
        )?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let row_count = sql_write_result_row_count(&response).ok_or_else(query_validate_error)?;

        Ok(ReadTotalOnlyPerfResult {
            row_count,
            instructions,
        })
    })
}

/// Execute one PerfAuditJournaledUser-only SQL query through the update surface
/// so the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_journaled_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditJournaledUser-only SQL query repeatedly inside
/// one canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditAccount-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_account(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditAccount-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditAccount-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditAccount-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditBlob-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_blob(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditBlob-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditBlob-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_blob_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditBlob-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditToken-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_token(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditToken-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditToken-only SQL query through the update surface so the
/// canister can persist warmed query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_token_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditToken-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

#[cfg(feature = "sql")]
fn scale_fixture_facts(
    surface: &str,
    requested_rows: u32,
    actual_rows: usize,
    zero_match_rows: usize,
    one_match_rows: usize,
    quarter_match_rows: usize,
    payload_profile: ScalePayloadProfile,
) -> Result<ScaleFixtureFacts, icydb::Error> {
    let actual_rows = u32::try_from(actual_rows).map_err(|_| query_validate_error())?;
    let zero_match_rows = u32::try_from(zero_match_rows).map_err(|_| query_validate_error())?;
    let one_match_rows = u32::try_from(one_match_rows).map_err(|_| query_validate_error())?;
    let quarter_match_rows =
        u32::try_from(quarter_match_rows).map_err(|_| query_validate_error())?;
    if actual_rows != requested_rows
        || zero_match_rows != 0
        || one_match_rows != 1
        || quarter_match_rows != requested_rows / 4
    {
        return Err(query_validate_error());
    }

    Ok(ScaleFixtureFacts {
        profile_version: SCALE_FIXTURE_PROFILE_VERSION,
        surface: surface.to_string(),
        fixture_rows: actual_rows,
        zero_match_rows,
        one_match_rows,
        quarter_match_rows,
        all_match_rows: actual_rows,
        payload_profile,
    })
}

#[cfg(feature = "sql")]
fn scale_user_mirror_fixture_facts(
    surface: &str,
    requested_rows: u32,
    rows: &[PerfAuditHeapUser],
) -> Result<ScaleFixtureFacts, icydb::Error> {
    scale_fixture_facts(
        surface,
        requested_rows,
        rows.len(),
        rows.iter().filter(|row| row.name.starts_with('A')).count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.age >= 24 && row.age < 40)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )
}

#[cfg(feature = "sql")]
fn scale_journaled_user_fixture_facts(
    requested_rows: u32,
    rows: &[PerfAuditJournaledUser],
) -> Result<ScaleFixtureFacts, icydb::Error> {
    scale_fixture_facts(
        "journaled_user",
        requested_rows,
        rows.len(),
        rows.iter().filter(|row| row.name.starts_with('A')).count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.age >= 24 && row.age < 40)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )
}

#[cfg(feature = "test-admin-api")]
fn convergence_closeout_id(first_id: i32, offset: u32) -> Result<i32, icydb::Error> {
    if first_id <= 0 {
        return Err(query_validate_error());
    }
    let offset = i32::try_from(offset).map_err(|_| query_validate_error())?;
    first_id
        .checked_add(offset)
        .ok_or_else(query_validate_error)
}

#[cfg(feature = "test-admin-api")]
fn convergence_closeout_user(id: i32) -> PerfAuditUser {
    PerfAuditUser {
        id,
        name: format!("convergence-closeout-{id}"),
        age: 31,
        age_nat: 31,
        rank: 29,
        active: true,
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

#[cfg(feature = "sql")]
fn perf_scale_users(row_count: i32) -> Vec<PerfAuditUser> {
    const MANY_GROUP_COUNT: i32 = 100;

    let quarter_rows = row_count / 4;
    let grouped_age_rows = quarter_rows / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            let age = if id <= grouped_age_rows {
                31
            } else if id <= grouped_age_rows * 2 {
                32
            } else if id <= grouped_age_rows * 3 {
                33
            } else if quarter_match {
                34
            } else {
                43
            };
            PerfAuditUser {
                id,
                name: format!("scale-group-{:03}", ((id - 1) % MANY_GROUP_COUNT) + 1),
                age,
                age_nat: if quarter_match { 31 } else { 43 },
                rank: age - 2,
                active: quarter_match,
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_accounts(row_count: i32) -> Vec<PerfAuditAccount> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            PerfAuditAccount {
                id,
                handle: format!("scale-account-{id:04}"),
                tier: if quarter_match { "gold" } else { "bronze" }.to_string(),
                active: quarter_match,
                score: 40 + (id % 60),
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_blobs(row_count: i32) -> Vec<PerfAuditBlob> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let (thumbnail_len, chunk_len) = match id % 4 {
                0 => (32, 256),
                1 => (64, 512),
                2 => (128, 1_024),
                _ => (256, 2_048),
            };
            // The low byte deliberately repeats a deterministic payload-byte
            // seed without affecting the separately declared length profile.
            PerfAuditBlob {
                id,
                label: format!("scale-payload-{id:04}"),
                bucket: if id <= quarter_rows { 10 } else { 20 },
                thumbnail: perf_blob(id.to_le_bytes()[0], thumbnail_len),
                chunk: perf_blob(id.wrapping_add(31).to_le_bytes()[0], chunk_len),
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_streaming_execution_rows() -> Vec<PerfAuditStreamingRow> {
    perf_streaming_execution_rows_range(1, STREAMING_EXECUTION_FIXTURE_ROWS)
}

#[cfg(feature = "sql")]
fn perf_streaming_execution_rows_range(first: i32, last: i32) -> Vec<PerfAuditStreamingRow> {
    (first..=last)
        .map(|id| PerfAuditStreamingRow {
            id,
            lane_a: streaming_lane_a(id),
            lane_b: streaming_lane_b(id),
            group_key: streaming_group_key(id),
            sort_key: streaming_sort_key(id),
            label: streaming_label(id).to_string(),
            payload: perf_blob(id.to_le_bytes()[0], streaming_payload_len(id)),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_streaming_execution_compound_rows() -> Vec<PerfAuditStreamingCompoundRow> {
    (1..=STREAMING_EXECUTION_FIXTURE_ROWS)
        .map(|id| PerfAuditStreamingCompoundRow {
            id,
            lane_a: streaming_lane_a(id),
            lane_b: streaming_lane_b(id),
            group_key: streaming_group_key(id),
            sort_key: streaming_sort_key(id),
            label: streaming_label(id).to_string(),
            payload: perf_blob(id.to_le_bytes()[0], 32),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
const fn streaming_lane_a(id: i32) -> i32 {
    (id * 17 + STREAMING_EXECUTION_FIXTURE_SEED_I32) % 97
}

#[cfg(feature = "sql")]
const fn streaming_lane_b(id: i32) -> i32 {
    (id * 29 + STREAMING_EXECUTION_FIXTURE_SEED_I32 + 2) % 101
}

#[cfg(feature = "sql")]
const fn streaming_group_key(id: i32) -> i32 {
    (id - 1) % 17
}

#[cfg(feature = "sql")]
const fn streaming_sort_key(id: i32) -> i32 {
    (id * 37 + STREAMING_EXECUTION_FIXTURE_SEED_I32) % STREAMING_EXECUTION_FIXTURE_ROWS
}

#[cfg(feature = "sql")]
const fn streaming_label(id: i32) -> &'static str {
    match id {
        1 => "early-wide",
        STREAMING_EXECUTION_FIXTURE_ROWS => "late-match",
        _ => "ordinary",
    }
}

#[cfg(feature = "sql")]
const fn streaming_payload_len(id: i32) -> usize {
    match id {
        1 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[0],
        2 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[1],
        3 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[2],
        _ => 32,
    }
}

#[cfg(feature = "sql")]
fn streaming_execution_fixture_facts(
    rows: &[PerfAuditStreamingRow],
) -> Result<StreamingExecutionFixtureFacts, icydb::Error> {
    let fixture_rows = u32::try_from(rows.len()).map_err(|_| query_validate_error())?;
    let first_lane_matches = u32::try_from(rows.iter().filter(|row| row.lane_a == 0).count())
        .map_err(|_| query_validate_error())?;
    let second_lane_matches = u32::try_from(rows.iter().filter(|row| row.lane_b == 0).count())
        .map_err(|_| query_validate_error())?;
    let sparse_overlap_rows = u32::try_from(
        rows.iter()
            .filter(|row| row.lane_a == 0 && row.lane_b == 0)
            .count(),
    )
    .map_err(|_| query_validate_error())?;
    let empty_overlap_rows = u32::try_from(
        rows.iter()
            .filter(|row| row.lane_a == 0 && row.lane_b == 1)
            .count(),
    )
    .map_err(|_| query_validate_error())?;
    let wide_payload_bytes = STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES
        .iter()
        .copied()
        .map(u32::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| query_validate_error())?;

    Ok(StreamingExecutionFixtureFacts {
        profile_version: STREAMING_EXECUTION_FIXTURE_PROFILE_VERSION,
        seed: STREAMING_EXECUTION_FIXTURE_SEED,
        fixture_rows,
        lane_a_zero_rows: first_lane_matches,
        lane_b_zero_rows: second_lane_matches,
        sparse_overlap_rows,
        empty_overlap_rows,
        group_count: 17,
        wide_payload_bytes,
    })
}

#[cfg(feature = "sql")]
fn perf_scale_heap_users(row_count: i32) -> Vec<PerfAuditHeapUser> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            build_perf_audit_heap_user(
                id,
                &format!("scale-heap-user-{id:04}"),
                if id <= quarter_rows { 31 } else { 43 },
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_journaled_users(row_count: i32) -> Vec<PerfAuditJournaledUser> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            build_perf_audit_journaled_user(
                id,
                &format!("scale-journaled-user-{id:04}"),
                if id <= quarter_rows { 31 } else { 43 },
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_tokens(row_count: i32) -> Vec<PerfAuditToken> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            let stage = if id % 2 == 0 { "Draft" } else { "Review" };
            perf_audit_token(
                20_000 + u128::from(id.unsigned_abs()),
                if quarter_match {
                    TOKEN_TARGET_COLLECTION
                } else {
                    TOKEN_OTHER_COLLECTION
                },
                stage,
                &format!("scale-token-{id:04}"),
            )
        })
        .collect()
}

/// Build the deterministic user fixture batch used by the perf audit.
#[cfg(feature = "test-admin-api")]
fn perf_audit_users() -> Vec<PerfAuditUser> {
    vec![
        PerfAuditUser {
            id: 1,
            name: "Alice".to_string(),
            age: 31,
            age_nat: 31,
            rank: 28,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 2,
            name: "bob".to_string(),
            age: 24,
            age_nat: 24,
            rank: 25,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 3,
            name: "Charlie".to_string(),
            age: 43,
            age_nat: 43,
            rank: 43,
            active: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 4,
            name: "amber".to_string(),
            age: 27,
            age_nat: 26,
            rank: 29,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 5,
            name: "Andrew".to_string(),
            age: 31,
            age_nat: 30,
            rank: 30,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 6,
            name: "Zelda".to_string(),
            age: 19,
            age_nat: 19,
            rank: 17,
            active: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

#[cfg(feature = "sql")]
fn build_perf_audit_heap_user(id: i32, name: &str, age: i32) -> PerfAuditHeapUser {
    PerfAuditHeapUser {
        id,
        name: name.to_string(),
        age,
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build a larger deterministic heap fixture window used by the bounded-query
/// instruction regression guard.
#[cfg(feature = "test-admin-api")]
fn perf_audit_heap_users() -> Vec<PerfAuditHeapUser> {
    (1..=512)
        .map(|id| build_perf_audit_heap_user(id, &format!("heap-user-{id:04}"), 18 + (id % 47)))
        .collect()
}

#[cfg(feature = "sql")]
fn build_perf_audit_journaled_user(id: i32, name: &str, age: i32) -> PerfAuditJournaledUser {
    PerfAuditJournaledUser {
        id,
        name: name.to_string(),
        age,
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build a larger deterministic journaled fixture window used by the
/// bounded-query instruction regression guard.
#[cfg(feature = "test-admin-api")]
fn perf_audit_journaled_users() -> Vec<PerfAuditJournaledUser> {
    (1..=512)
        .map(|id| {
            build_perf_audit_journaled_user(id, &format!("journaled-user-{id:04}"), 18 + (id % 47))
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_journaled_reentry_probe_users() -> Vec<PerfAuditJournaledUser> {
    (1..=JOURNALED_REENTRY_PROBE_ROWS)
        .map(|id| {
            build_perf_audit_journaled_user(
                id,
                &format!("journaled-reentry-{id:04}"),
                18 + (id % 13),
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_mutation_tokens(first_id: i32, last_id: i32) -> Vec<PerfAuditMutationToken> {
    (first_id..=last_id)
        .map(|id| PerfAuditMutationToken {
            id,
            collection_id: 7,
            tier: "Legacy".to_string(),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_mutation_scoring_states(
    first_id: i32,
    last_id: i32,
) -> Vec<PerfAuditMutationScoringState> {
    (first_id..=last_id)
        .map(|id| PerfAuditMutationScoringState {
            id,
            collection_id: 7,
            score_stale: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_unrelated_mutation_tokens() -> Vec<PerfAuditMutationToken> {
    (1..=MUTATION_SCALE_UNRELATED_ROWS)
        .map(|offset| PerfAuditMutationToken {
            id: 20_000 + offset,
            collection_id: 8,
            tier: "Legacy".to_string(),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_unrelated_mutation_scoring_states() -> Vec<PerfAuditMutationScoringState> {
    (1..=MUTATION_SCALE_UNRELATED_ROWS)
        .map(|offset| PerfAuditMutationScoringState {
            id: 20_000 + offset,
            collection_id: 8,
            score_stale: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_relation_targets() -> Vec<PerfAuditRelationTarget> {
    (1..=16)
        .map(|id| PerfAuditRelationTarget {
            id,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_relation_sources() -> Vec<PerfAuditRelationSource> {
    (1..=16)
        .map(|id| PerfAuditRelationSource {
            id,
            target_id: ((id - 1) % 8) + 1,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

/// Build one deterministic blob payload for perf fixture rows.
#[cfg(feature = "sql")]
fn perf_blob(seed: u8, len: usize) -> Blob {
    Blob::from(
        (0u8..=250)
            .cycle()
            .take(len)
            .map(|offset| seed.wrapping_add(offset))
            .collect::<Vec<_>>(),
    )
}

/// Build the deterministic blob fixture batch used by SQL perf audit queries.
#[cfg(feature = "test-admin-api")]
fn perf_audit_blobs() -> Vec<PerfAuditBlob> {
    vec![
        PerfAuditBlob {
            id: 1,
            label: "avatar-a".to_string(),
            bucket: 10,
            thumbnail: perf_blob(11, 1_024),
            chunk: perf_blob(31, 16_384),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 2,
            label: "avatar-b".to_string(),
            bucket: 10,
            thumbnail: perf_blob(12, 2_048),
            chunk: perf_blob(32, 32_768),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 3,
            label: "avatar-c".to_string(),
            bucket: 10,
            thumbnail: perf_blob(13, 4_096),
            chunk: perf_blob(33, 65_536),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 4,
            label: "archive-a".to_string(),
            bucket: 20,
            thumbnail: perf_blob(14, 1_024),
            chunk: perf_blob(34, 16_384),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 5,
            label: "archive-b".to_string(),
            bucket: 20,
            thumbnail: perf_blob(15, 2_048),
            chunk: perf_blob(35, 32_768),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 6,
            label: "archive-c".to_string(),
            bucket: 30,
            thumbnail: perf_blob(16, 4_096),
            chunk: perf_blob(36, 65_536),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

/// Build the deterministic account fixture batch used by the perf audit.
#[cfg(feature = "test-admin-api")]
fn perf_audit_accounts() -> Vec<PerfAuditAccount> {
    vec![
        PerfAuditAccount {
            id: 1,
            handle: "Bravo".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 91,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 2,
            handle: "alpha".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 75,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 3,
            handle: "bravo".to_string(),
            tier: "silver".to_string(),
            active: true,
            score: 78,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 4,
            handle: "Delta".to_string(),
            tier: "silver".to_string(),
            active: false,
            score: 66,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 5,
            handle: "brick".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 88,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 6,
            handle: "azure".to_string(),
            tier: "bronze".to_string(),
            active: true,
            score: 63,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

#[cfg(feature = "sql")]
fn perf_audit_token(id: u128, collection_id: &str, stage: &str, title: &str) -> PerfAuditToken {
    PerfAuditToken {
        id: Ulid::from_bytes(id.to_be_bytes()),
        collection_id: collection_id.to_string(),
        stage: stage.to_string(),
        title: title.to_string(),
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build the deterministic token fixture batch used by the branch-set perf
/// audit query.
#[cfg(feature = "test-admin-api")]
fn perf_audit_tokens() -> Vec<PerfAuditToken> {
    let mut tokens = vec![
        perf_audit_token(9_090, TOKEN_TARGET_COLLECTION, "Draft", "draft-090"),
        perf_audit_token(9_095, TOKEN_TARGET_COLLECTION, "Review", "review-095"),
        perf_audit_token(9_100, TOKEN_TARGET_COLLECTION, "Review", "review-100"),
        perf_audit_token(9_105, TOKEN_TARGET_COLLECTION, "Draft", "draft-105"),
        perf_audit_token(9_110, TOKEN_TARGET_COLLECTION, "Published", "published-110"),
        perf_audit_token(9_115, TOKEN_OTHER_COLLECTION, "Draft", "other-draft-115"),
        perf_audit_token(9_120, TOKEN_TARGET_COLLECTION, "Draft", "draft-120"),
        perf_audit_token(9_125, TOKEN_TARGET_COLLECTION, "Review", "review-125"),
        perf_audit_token(9_130, TOKEN_TARGET_COLLECTION, "Draft", "draft-130"),
        perf_audit_token(9_135, TOKEN_TARGET_COLLECTION, "Review", "review-135"),
        perf_audit_token(9_140, TOKEN_TARGET_COLLECTION, "Queued", "queued-140"),
        perf_audit_token(9_145, TOKEN_OTHER_COLLECTION, "Review", "other-review-145"),
        perf_audit_token(9_150, TOKEN_TARGET_COLLECTION, "Draft", "draft-150"),
        perf_audit_token(9_155, TOKEN_TARGET_COLLECTION, "Review", "review-155"),
        perf_audit_token(9_160, TOKEN_TARGET_COLLECTION, "Archived", "archived-160"),
        perf_audit_token(9_165, TOKEN_OTHER_COLLECTION, "Draft", "other-draft-165"),
        perf_audit_token(9_170, TOKEN_TARGET_COLLECTION, "Draft", "draft-170"),
        perf_audit_token(9_175, TOKEN_TARGET_COLLECTION, "Review", "review-175"),
        perf_audit_token(9_180, TOKEN_TARGET_COLLECTION, "Rejected", "rejected-180"),
        perf_audit_token(9_185, TOKEN_OTHER_COLLECTION, "Review", "other-review-185"),
    ];

    for offset in 0..240u128 {
        let stage = match offset % 4 {
            0 => "Draft",
            1 => "Queued",
            2 => "Review",
            _ => "Published",
        };
        let title = format!("{}-pressure-{offset:03}", stage.to_ascii_lowercase());
        tokens.push(perf_audit_token(
            10_000 + offset,
            TOKEN_TARGET_COLLECTION,
            stage,
            title.as_str(),
        ));
    }

    tokens
}

/// Closed producer and recovery facts for one maximum accepted-index publication.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "test-admin-api")]
struct PromotionIndexPublicationFacts {
    rows_scanned: u64,
    index_keys_written: u64,
    local_instructions: u64,
}

/// Publish or retire the temporary accepted index used by 0.230 closeout evidence.
#[cfg(feature = "test-admin-api")]
#[update]
fn mutate_cardinality_closeout_index(
    present: bool,
    expected_version: u64,
    next_version: u64,
) -> Result<PromotionIndexPublicationFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        if expected_version.checked_add(1) != Some(next_version) {
            return Err(query_validate_error());
        }
        let sql = if present {
            format!(
                "CREATE INDEX perf_cardinality_active_idx ON PerfAuditUser (active) \
                 EXPECT SCHEMA VERSION {expected_version} SET SCHEMA VERSION {next_version}"
            )
        } else {
            format!(
                "DROP INDEX perf_cardinality_active_idx ON PerfAuditUser \
                 EXPECT SCHEMA VERSION {expected_version} SET SCHEMA VERSION {next_version}"
            )
        };
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_admin_sql_ddl(sql.as_str())?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let SqlQueryResult::Ddl {
            rows_scanned,
            index_keys_written,
            status,
            ..
        } = result
        else {
            return Err(query_validate_error());
        };
        if status != "published" {
            return Err(query_validate_error());
        }

        Ok(PromotionIndexPublicationFacts {
            rows_scanned,
            index_keys_written,
            local_instructions,
        })
    })
}

#[cfg(feature = "test-admin-api")]
const PROMOTION_INDEX_FIXTURE_ROWS: u32 = 65_536;
#[cfg(feature = "test-admin-api")]
const PROMOTION_INDEX_LOAD_PAGE_ROWS: u32 = 4_096;

/// Append one admitted page of the maximum accepted-index promotion fixture.
#[cfg(feature = "test-admin-api")]
#[update]
fn append_promotion_index_fixture_page(first_id: u32, row_count: u32) -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        if first_id == 0 || row_count == 0 || row_count > PROMOTION_INDEX_LOAD_PAGE_ROWS {
            return Err(query_validate_error());
        }
        let last_id = first_id
            .checked_add(row_count.saturating_sub(1))
            .ok_or_else(query_validate_error)?;
        if last_id > PROMOTION_INDEX_FIXTURE_ROWS {
            return Err(query_validate_error());
        }
        let rows = (first_id..=last_id)
            .map(|id| {
                let id = i32::try_from(id).map_err(|_| query_validate_error())?;
                Ok(build_perf_audit_journaled_user(
                    id,
                    &format!("promotion-index-user-{id:05}"),
                    31,
                ))
            })
            .collect::<Result<Vec<_>, icydb::Error>>()?;
        insert_fixture_rows(rows)?;
        Ok(row_count)
    })
}

/// Publish the largest maintained accepted-index key set through ordinary DDL.
#[cfg(feature = "test-admin-api")]
#[update]
fn publish_promotion_index_fixture() -> Result<PromotionIndexPublicationFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_admin_sql_ddl(
            "CREATE INDEX perf_promotion_name_idx ON PerfAuditJournaledUser (name) \
             EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
        )?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let SqlQueryResult::Ddl {
            rows_scanned,
            index_keys_written,
            status,
            ..
        } = result
        else {
            return Err(query_validate_error());
        };
        if status != "published"
            || rows_scanned != u64::from(PROMOTION_INDEX_FIXTURE_ROWS)
            || index_keys_written != u64::from(PROMOTION_INDEX_FIXTURE_ROWS)
        {
            return Err(query_validate_error());
        }

        Ok(PromotionIndexPublicationFacts {
            rows_scanned,
            index_keys_written,
            local_instructions,
        })
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
