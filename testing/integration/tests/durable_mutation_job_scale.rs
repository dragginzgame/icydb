//! Collection-scale durable mutation-job composition and upgrade evidence.

use candid::CandidType;
use icydb::{
    Error,
    db::{
        EntitySchemaDescription, MutationJobAdvanceReceipt, MutationJobError, MutationJobPhase,
        MutationJobState, MutationJobStatus, sql::SqlQueryResult,
    },
    diagnostic::{DiagnosticDetail, SqlWriteBoundaryCode},
    value::OutputValue,
};
use icydb_testing_integration::{
    durable_mutation_job_contract::{
        DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING, DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING,
        DURABLE_MUTATION_JOB_FIXTURE_ROWS, DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT,
        DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT, DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING,
        minimum_forward_advances, minimum_verify_advances,
    },
    install_fixture_canister, upgrade_fixture_canister,
};
use serde::Deserialize;
use std::time::Duration;

const LOAD_PAGE_ROWS: u32 = 1_024;
const UNRELATED_ROWS: u32 = 17;
const MAX_JOB_ADVANCES: usize = 256;

const fn minimum_physical_scan_advances(rows: u32) -> u32 {
    rows.saturating_add(DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT - 1)
        / DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum MutationScaleJob {
    Tier,
    Scoring,
}

impl MutationScaleJob {
    const fn label(self) -> &'static str {
        match self {
            Self::Tier => "tier",
            Self::Scoring => "scoring",
        }
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationScaleLoadEvidence {
    first_id: u32,
    last_id: u32,
    matching_rows_loaded: u32,
    unrelated_rows_loaded: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationScaleFixtureFacts {
    token_collection: u32,
    token_default: u32,
    token_other: u32,
    token_other_default: u32,
    scoring_collection: u32,
    scoring_stale: u32,
    scoring_other: u32,
    scoring_other_stale: u32,
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
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

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationScaleAdvancePerfResult {
    receipt: MutationJobAdvanceReceipt,
    local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct MutationScaleRecoveryEvidence {
    complete: bool,
    warmed_rows: u32,
    local_instructions: u64,
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum ApplicationStartupHook {
    Init,
    PostUpgrade,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

struct MutationScaleStartupEvidence {
    observation_instructions: Vec<u64>,
    stable_bytes_before_upgrade: u64,
    stable_bytes_after_recovery: u64,
    application_recovering_observations: u32,
}

struct MutationScaleRunEvidence {
    phase: MutationJobPhase,
    sequence: u64,
    status: MutationJobStatus,
    forward_calls: u32,
    verify_calls: u32,
    forward_keys_scanned: u64,
    verify_keys_scanned: u64,
    rows_updated: u64,
    max_forward_instructions: u64,
    recovery_reentry_instructions: Option<u64>,
    max_recovered_forward_instructions: u64,
    max_recovered_verify_instructions: u64,
    max_verify_instructions: u64,
    max_replay_instructions: u64,
    forward_replayed: bool,
    verify_replayed: bool,
    completion_replayed: bool,
}

impl MutationScaleRunEvidence {
    const fn new(state: &MutationJobState) -> Self {
        Self {
            phase: state.phase,
            sequence: state.sequence,
            status: state.status,
            forward_calls: 0,
            verify_calls: 0,
            forward_keys_scanned: 0,
            verify_keys_scanned: 0,
            rows_updated: 0,
            max_forward_instructions: 0,
            recovery_reentry_instructions: None,
            max_recovered_forward_instructions: 0,
            max_recovered_verify_instructions: 0,
            max_verify_instructions: 0,
            max_replay_instructions: 0,
            forward_replayed: false,
            verify_replayed: false,
            completion_replayed: false,
        }
    }

    fn record_committed(&mut self, job: MutationScaleJob, result: &MutationScaleAdvancePerfResult) {
        self.record_committed_with_performance_gates(job, result, true);
    }

    fn record_recovered(&mut self, job: MutationScaleJob, result: &MutationScaleAdvancePerfResult) {
        self.record_committed_with_performance_gates(job, result, false);
    }

    fn record_recovery_reentry(
        &mut self,
        job: MutationScaleJob,
        result: &MutationScaleAdvancePerfResult,
    ) {
        assert!(self.recovery_reentry_instructions.is_none());
        assert_eq!(self.phase, MutationJobPhase::Forward);
        self.recovery_reentry_instructions = Some(result.local_instructions);
        self.record_recovered(job, result);
    }

    fn record_committed_with_performance_gates(
        &mut self,
        job: MutationScaleJob,
        result: &MutationScaleAdvancePerfResult,
        enforce_performance_gates: bool,
    ) {
        assert!(result.receipt.keys_scanned <= u64::from(DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT));
        assert!(result.receipt.rows_updated <= u64::from(DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT));
        match self.phase {
            MutationJobPhase::Forward => {
                self.forward_calls = self.forward_calls.saturating_add(1);
                self.forward_keys_scanned = self
                    .forward_keys_scanned
                    .saturating_add(result.receipt.keys_scanned);
                self.rows_updated = self
                    .rows_updated
                    .saturating_add(result.receipt.rows_updated);
                if enforce_performance_gates {
                    self.max_forward_instructions =
                        self.max_forward_instructions.max(result.local_instructions);
                    assert!(
                        result.local_instructions < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING,
                        "{} Forward step {} exceeded its frozen ceiling: {} >= {}",
                        job.label(),
                        self.forward_calls,
                        result.local_instructions,
                        DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING,
                    );
                } else {
                    self.max_recovered_forward_instructions = self
                        .max_recovered_forward_instructions
                        .max(result.local_instructions);
                }
            }
            MutationJobPhase::Verify => {
                self.verify_calls = self.verify_calls.saturating_add(1);
                self.verify_keys_scanned = self
                    .verify_keys_scanned
                    .saturating_add(result.receipt.keys_scanned);
                assert_eq!(result.receipt.rows_updated, 0);
                if enforce_performance_gates {
                    self.max_verify_instructions =
                        self.max_verify_instructions.max(result.local_instructions);
                    assert!(
                        result.local_instructions < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING,
                        "{} Verify step {} exceeded its frozen ceiling: {} >= {}",
                        job.label(),
                        self.verify_calls,
                        result.local_instructions,
                        DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING,
                    );
                } else {
                    self.max_recovered_verify_instructions = self
                        .max_recovered_verify_instructions
                        .max(result.local_instructions);
                }
            }
        }
        self.phase = result.receipt.phase;
        self.sequence = result.receipt.committed_sequence;
        self.status = result.receipt.status;
    }
}

fn load_scale_fixture(fixture: &ic_testkit::pic::StandaloneCanisterFixture) {
    let mut first_id = 1_u32;
    let mut loaded = 0_u32;
    while first_id <= DURABLE_MUTATION_JOB_FIXTURE_ROWS {
        let row_count = LOAD_PAGE_ROWS.min(DURABLE_MUTATION_JOB_FIXTURE_ROWS - first_id + 1);
        let evidence: Result<MutationScaleLoadEvidence, Error> = fixture
            .update_candid("load_collection_mutation_scale_page", (first_id, row_count))
            .expect("scale load page should decode");
        let evidence = evidence.expect("scale load page should succeed");
        assert_eq!(evidence.first_id, first_id);
        assert_eq!(evidence.last_id, first_id + row_count - 1);
        assert_eq!(evidence.matching_rows_loaded, row_count);
        assert_eq!(
            evidence.unrelated_rows_loaded,
            if first_id == 1 { UNRELATED_ROWS } else { 0 },
        );
        // The endpoint publishes two journaled fixture families. Deliver one
        // callback between pages so their retained records remain within the
        // production cumulative bound while preserving debt for upgrade.
        advance_startup_timers(fixture);
        loaded = loaded.saturating_add(row_count);
        first_id = first_id.saturating_add(row_count);
    }
    assert_eq!(loaded, DURABLE_MUTATION_JOB_FIXTURE_ROWS);
}

fn scale_facts(fixture: &ic_testkit::pic::StandaloneCanisterFixture) -> MutationScaleFixtureFacts {
    let count = |fact| {
        let count: Result<u32, Error> = fixture
            .query_candid("collection_mutation_scale_fact", (fact,))
            .expect("scale fact should decode");
        count.expect("scale fact should load")
    };
    MutationScaleFixtureFacts {
        token_collection: count(MutationScaleFact::TokenCollection),
        token_default: count(MutationScaleFact::TokenDefault),
        token_other: count(MutationScaleFact::TokenOther),
        token_other_default: count(MutationScaleFact::TokenOtherDefault),
        scoring_collection: count(MutationScaleFact::ScoringCollection),
        scoring_stale: count(MutationScaleFact::ScoringStale),
        scoring_other: count(MutationScaleFact::ScoringOther),
        scoring_other_stale: count(MutationScaleFact::ScoringOtherStale),
    }
}

fn application_startup_contract(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> ApplicationStartupSnapshot {
    fixture
        .query_candid("application_startup_contract", ())
        .expect("application startup contract should decode")
}

fn canister_stable_memory_bytes(fixture: &ic_testkit::pic::StandaloneCanisterFixture) -> u64 {
    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .expect("scale canister status should be available");
    match status
        .memory_metrics
        .stable_memory_size
        .0
        .to_u64_digits()
        .as_slice()
    {
        [] => 0,
        [bytes] => *bytes,
        _ => panic!("scale canister stable memory should fit u64"),
    }
}

fn accepted_schema_descriptions(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> Vec<EntitySchemaDescription> {
    let descriptions: Result<Vec<EntitySchemaDescription>, Error> = fixture
        .query_candid("accepted_schema_descriptions", ())
        .expect("accepted schema descriptions should decode");
    descriptions.expect("accepted schema descriptions should remain available")
}

fn query_count(fixture: &ic_testkit::pic::StandaloneCanisterFixture, sql: &str) -> u64 {
    let response: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_user", (sql.to_string(),))
        .expect("scale SQL count should decode");
    let SqlQueryResult::Projection(projection) = response.expect("scale SQL count should succeed")
    else {
        panic!("scale SQL count should return one projection");
    };
    let [row] = projection.rows.as_slice() else {
        panic!("scale SQL count should return one row");
    };
    let [OutputValue::Nat64(count)] = row.as_slice() else {
        panic!("scale SQL count should return one Nat64 cell");
    };
    *count
}

fn assert_populated_authority(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    accepted_schema_before_upgrade: &[u8],
) {
    let descriptions = accepted_schema_descriptions(fixture);
    let accepted_schema_after_upgrade =
        candid::encode_one(descriptions.clone()).expect("accepted schema should encode");
    assert_eq!(
        accepted_schema_after_upgrade,
        accepted_schema_before_upgrade
    );
    assert_eq!(descriptions.len(), 10);
    for entity_name in ["PerfAuditMutationScoringState", "PerfAuditMutationToken"] {
        let description = descriptions
            .iter()
            .find(|description| description.entity_name() == entity_name)
            .unwrap_or_else(|| panic!("accepted {entity_name} schema should remain present"));
        assert_eq!(description.primary_key_fields(), &["id"]);
        assert!(
            description
                .indexes()
                .iter()
                .any(|index| { !index.unique() && index.fields() == ["collection_id"] })
        );
    }

    for sql in [
        "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE id = 1 AND collection_id = 7 AND tier = 'Default'",
        "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE id = 10001 AND collection_id = 7 AND tier = 'Default'",
        "SELECT COUNT(*) FROM PerfAuditMutationToken WHERE id = 20001 AND collection_id = 8 AND tier = 'Legacy'",
        "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE id = 1 AND collection_id = 7 AND score_stale = true",
        "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE id = 10001 AND collection_id = 7 AND score_stale = true",
        "SELECT COUNT(*) FROM PerfAuditMutationScoringState WHERE id = 20001 AND collection_id = 8 AND score_stale = false",
    ] {
        assert_eq!(
            query_count(fixture, sql),
            1,
            "canonical row must survive: {sql}"
        );
    }
}

fn assert_application_deferred(snapshot: &ApplicationStartupSnapshot) {
    assert_eq!(snapshot.hook, ApplicationStartupHook::PostUpgrade);
    assert!(snapshot.engine_registered_before_hook);
    assert!(snapshot.observations > 0);
    assert!(snapshot.recovering_observations > 0);
    assert_eq!(snapshot.ready_observations, 0);
    assert_eq!(snapshot.restorations, 0);
    assert!(snapshot.retry_scheduled);
    assert!(snapshot.failure.is_none());
}

fn start_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
) -> MutationJobState {
    let state: Result<MutationJobState, MutationJobError> = fixture
        .update_candid("start_collection_mutation_scale_job", (job,))
        .expect("scale start should decode");
    state.expect("scale start should succeed")
}

fn scale_job_state(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
) -> Result<MutationJobState, MutationJobError> {
    fixture
        .update_candid("collection_mutation_scale_job_state", (job,))
        .expect("scale job state should decode")
}

fn advance_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
    sequence: u64,
) -> Result<MutationScaleAdvancePerfResult, MutationJobError> {
    fixture
        .update_candid(
            "advance_collection_mutation_scale_job",
            (job, sequence, format!("{}-{sequence}", job.label())),
        )
        .expect("scale advance should decode")
}

fn replay_scale_advance(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
    sequence: u64,
    expected: &MutationScaleAdvancePerfResult,
) -> u64 {
    let replay = advance_scale_job(fixture, job, sequence).expect("scale advance should replay");
    assert_eq!(replay.receipt, expected.receipt);
    assert!(
        replay.local_instructions < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING,
        "replay exceeded its frozen ceiling: {} >= {}",
        replay.local_instructions,
        DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING,
    );
    replay.local_instructions
}

fn run_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
    mut evidence: MutationScaleRunEvidence,
    expected_forward_calls: u32,
    expected_rows_updated: u64,
    enforce_performance_gates: bool,
) -> MutationScaleRunEvidence {
    for _ in 0..MAX_JOB_ADVANCES {
        if evidence.status == MutationJobStatus::Completed {
            break;
        }
        let request_sequence = evidence.sequence;
        let prior_phase = evidence.phase;
        let result = advance_scale_job(fixture, job, request_sequence)
            .expect("bounded scale advance should succeed");
        let replay_forward = prior_phase == MutationJobPhase::Forward && !evidence.forward_replayed;
        let replay_verify = prior_phase == MutationJobPhase::Verify && !evidence.verify_replayed;
        let replay_completion = result.receipt.status == MutationJobStatus::Completed;
        if replay_forward || replay_verify || replay_completion {
            evidence.max_replay_instructions = evidence.max_replay_instructions.max(
                replay_scale_advance(fixture, job, request_sequence, &result),
            );
            evidence.forward_replayed |= replay_forward;
            evidence.verify_replayed |= replay_verify;
            evidence.completion_replayed |= replay_completion;
        }
        if enforce_performance_gates {
            evidence.record_committed(job, &result);
        } else {
            evidence.record_recovered(job, &result);
        }
        // Model separate user calls with the replicated watchdog running
        // between them; each callback owns at most one complete batch.
        advance_startup_timers(fixture);
    }

    assert_eq!(evidence.status, MutationJobStatus::Completed);
    assert_eq!(evidence.forward_calls, expected_forward_calls);
    assert_eq!(
        evidence.verify_calls,
        minimum_verify_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
    );
    assert_eq!(
        evidence.forward_keys_scanned,
        u64::from(DURABLE_MUTATION_JOB_FIXTURE_ROWS + UNRELATED_ROWS),
    );
    assert_eq!(
        evidence.verify_keys_scanned,
        u64::from(DURABLE_MUTATION_JOB_FIXTURE_ROWS + UNRELATED_ROWS),
    );
    assert_eq!(evidence.rows_updated, expected_rows_updated);
    assert!(evidence.forward_replayed);
    assert!(evidence.verify_replayed);
    assert!(evidence.completion_replayed);
    evidence
}

fn acknowledge_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
    sequence: u64,
) {
    for _ in 0..2 {
        let result: Result<(), MutationJobError> = fixture
            .update_candid("acknowledge_collection_mutation_scale_job", (job, sequence))
            .expect("scale acknowledgement should decode");
        result.expect("terminal acknowledgement should be replay-safe");
    }
    assert!(matches!(
        scale_job_state(fixture, job),
        Err(MutationJobError::NotFound)
    ));
}

fn assert_initial_scale_facts(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> MutationScaleFixtureFacts {
    let initial = scale_facts(fixture);
    assert_eq!(initial.token_collection, DURABLE_MUTATION_JOB_FIXTURE_ROWS);
    assert_eq!(initial.token_default, 0);
    assert_eq!(initial.token_other, UNRELATED_ROWS);
    assert_eq!(initial.token_other_default, 0);
    assert_eq!(
        initial.scoring_collection,
        DURABLE_MUTATION_JOB_FIXTURE_ROWS,
    );
    assert_eq!(initial.scoring_stale, 0);
    assert_eq!(initial.scoring_other, UNRELATED_ROWS);
    assert_eq!(initial.scoring_other_stale, 0);
    initial
}

fn prove_eager_control_is_bounded(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    initial: &MutationScaleFixtureFacts,
) {
    let eager: Result<SqlQueryResult, Error> = fixture
        .update_candid("try_collection_eager_tier_reset", ())
        .expect("eager control should decode");
    let eager_error = eager.expect_err("10,001 rows must exceed one-shot exact admission");
    assert!(matches!(
        eager_error.diagnostic().detail(),
        Some(DiagnosticDetail::SqlWriteBoundary {
            boundary: SqlWriteBoundaryCode::ExactUpdateAssertionTooHigh,
        })
    ));
    assert_eq!(&scale_facts(fixture), initial);
}

fn start_composed_scale_jobs(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> (MutationJobState, MutationJobState) {
    // Persist both application phases before the first phase advances.
    let tier_state = start_scale_job(fixture, MutationScaleJob::Tier);
    let scoring_state = start_scale_job(fixture, MutationScaleJob::Scoring);
    assert_eq!(start_scale_job(fixture, MutationScaleJob::Tier), tier_state);
    assert_eq!(
        start_scale_job(fixture, MutationScaleJob::Scoring),
        scoring_state,
    );
    assert_eq!(tier_state.sequence, 0);
    assert_eq!(scoring_state.sequence, 0);

    let premature = advance_scale_job(fixture, MutationScaleJob::Tier, 1);
    assert!(matches!(
        premature,
        Err(MutationJobError::StaleSequence { .. })
    ));
    assert_eq!(
        scale_job_state(fixture, MutationScaleJob::Scoring)
            .expect("later phase must survive an earlier phase failure"),
        scoring_state,
    );
    (tier_state, scoring_state)
}

fn run_warm_scale_jobs(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    tier_state: &MutationJobState,
    scoring_state: &MutationJobState,
) -> (
    MutationScaleRunEvidence,
    MutationScaleRunEvidence,
    MutationScaleFixtureFacts,
) {
    // Commit and replay one real page, then converge both application phases.
    let first_tier = advance_scale_job(fixture, MutationScaleJob::Tier, 0)
        .expect("first tier page should commit");
    let mut tier_evidence = MutationScaleRunEvidence::new(tier_state);
    tier_evidence.max_replay_instructions =
        replay_scale_advance(fixture, MutationScaleJob::Tier, 0, &first_tier);
    tier_evidence.forward_replayed = true;
    tier_evidence.record_committed(MutationScaleJob::Tier, &first_tier);
    tier_evidence = run_scale_job(
        fixture,
        MutationScaleJob::Tier,
        tier_evidence,
        minimum_forward_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
        u64::from(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
        true,
    );
    let after_tier = scale_facts(fixture);
    assert_eq!(after_tier.token_default, DURABLE_MUTATION_JOB_FIXTURE_ROWS,);
    assert_eq!(after_tier.scoring_stale, 0);
    assert_eq!(
        scale_job_state(fixture, MutationScaleJob::Scoring)
            .expect("persisted scoring phase must remain runnable")
            .sequence,
        0,
    );

    let scoring_evidence = run_scale_job(
        fixture,
        MutationScaleJob::Scoring,
        MutationScaleRunEvidence::new(scoring_state),
        minimum_forward_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
        u64::from(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
        true,
    );
    let completed = scale_facts(fixture);
    assert_eq!(completed.token_default, DURABLE_MUTATION_JOB_FIXTURE_ROWS,);
    assert_eq!(completed.scoring_stale, DURABLE_MUTATION_JOB_FIXTURE_ROWS,);
    assert_eq!(completed.token_other, UNRELATED_ROWS);
    assert_eq!(completed.token_other_default, 0);
    assert_eq!(completed.scoring_other, UNRELATED_ROWS);
    assert_eq!(completed.scoring_other_stale, 0);
    (tier_evidence, scoring_evidence, completed)
}

fn advance_startup_timers(fixture: &ic_testkit::pic::StandaloneCanisterFixture) {
    fixture.pocket_ic().advance_time(Duration::from_secs(1));
    fixture.pocket_ic().tick();
    fixture.pocket_ic().tick();
}

fn wait_for_application_restoration(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> ApplicationStartupSnapshot {
    let mut snapshot = application_startup_contract(fixture);
    for _ in 0..4 {
        if snapshot.restorations == 1 {
            break;
        }
        advance_startup_timers(fixture);
        snapshot = application_startup_contract(fixture);
    }
    assert_eq!(snapshot.hook, ApplicationStartupHook::PostUpgrade);
    assert!(snapshot.engine_registered_before_hook);
    assert!(snapshot.recovering_observations > 0);
    assert_eq!(snapshot.ready_observations, 1);
    assert_eq!(snapshot.restorations, 1);
    assert!(!snapshot.retry_scheduled);
    assert!(snapshot.failure.is_none());
    snapshot
}

fn drive_populated_startup_recovery(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> (Vec<u64>, u32) {
    assert_application_deferred(&application_startup_contract(fixture));
    let mut recovery_ticks = 0_u32;
    let mut observation_instructions = Vec::new();
    let mut mid_recovery_upgrade_complete = false;
    let mut application_recovering_observations = 0_u32;
    loop {
        advance_startup_timers(fixture);
        let observation: Result<MutationScaleRecoveryEvidence, Error> = fixture
            .update_candid("recover_collection_mutation_scale_store", ())
            .expect("scale recovery observation should decode");
        let observation =
            observation.expect("scale target store observation should remain available");
        recovery_ticks = recovery_ticks.saturating_add(1);
        observation_instructions.push(observation.local_instructions);
        assert!(observation.local_instructions > 0);
        if observation.complete {
            assert_eq!(observation.warmed_rows, 1);
            break;
        }
        assert_eq!(observation.warmed_rows, 0);
        if !mid_recovery_upgrade_complete {
            let before_upgrade = application_startup_contract(fixture);
            assert_application_deferred(&before_upgrade);
            application_recovering_observations = before_upgrade.recovering_observations;
            let stable_bytes_before_upgrade = canister_stable_memory_bytes(fixture);
            upgrade_fixture_canister(fixture, "sql_perf");
            assert!(
                canister_stable_memory_bytes(fixture) >= stable_bytes_before_upgrade,
                "mid-recovery upgrade may fold pending batches but must not shrink stable memory",
            );
            assert_application_deferred(&application_startup_contract(fixture));
            mid_recovery_upgrade_complete = true;
        }
        assert!(
            recovery_ticks < 32,
            "startup recovery must make bounded progress"
        );
    }
    assert!(recovery_ticks > 1, "the scale fixture must resume");
    assert!(
        mid_recovery_upgrade_complete,
        "the populated fixture must upgrade while recovery is incomplete",
    );
    let application_ready = wait_for_application_restoration(fixture);
    application_recovering_observations = application_recovering_observations
        .saturating_add(application_ready.recovering_observations);
    (
        observation_instructions,
        application_recovering_observations,
    )
}

fn recover_active_tier_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    tier_sequence: u64,
    completed: &MutationScaleFixtureFacts,
) -> (MutationScaleRunEvidence, MutationScaleStartupEvidence) {
    // Reuse the converged tier intent to prove that an active private
    // continuation survives upgrade without conflating recovered stable-store
    // cost with the frozen warm Forward fixture.
    acknowledge_scale_job(fixture, MutationScaleJob::Tier, tier_sequence);
    let recovery_state = start_scale_job(fixture, MutationScaleJob::Tier);
    let scoring_scheduler_state = scale_job_state(fixture, MutationScaleJob::Scoring)
        .expect("completed scoring phase should remain durably scheduled");
    let accepted_schema_before_upgrade = candid::encode_one(accepted_schema_descriptions(fixture))
        .expect("accepted schema should encode before upgrade");
    let stable_bytes_before_upgrade = canister_stable_memory_bytes(fixture);
    upgrade_fixture_canister(fixture, "sql_perf");
    assert!(
        canister_stable_memory_bytes(fixture) >= stable_bytes_before_upgrade,
        "same-schema upgrade may advance recovery but must not shrink stable memory",
    );
    let (observation_instructions, application_recovering_observations) =
        drive_populated_startup_recovery(fixture);

    assert_eq!(
        start_scale_job(fixture, MutationScaleJob::Tier),
        recovery_state,
        "same-intent start replay must retain recovered progress",
    );
    assert_eq!(
        scale_job_state(fixture, MutationScaleJob::Tier)
            .expect("active tier job should survive upgrade"),
        recovery_state,
    );
    assert_eq!(
        scale_job_state(fixture, MutationScaleJob::Scoring)
            .expect("completed scoring scheduler row should survive upgrade"),
        scoring_scheduler_state,
    );
    assert_populated_authority(fixture, &accepted_schema_before_upgrade);

    let first_recovered = advance_scale_job(fixture, MutationScaleJob::Tier, 0)
        .expect("first recovered tier page should commit");
    let mut recovery_evidence = MutationScaleRunEvidence::new(&recovery_state);
    recovery_evidence.max_replay_instructions =
        replay_scale_advance(fixture, MutationScaleJob::Tier, 0, &first_recovered);
    recovery_evidence.forward_replayed = true;
    recovery_evidence.record_recovery_reentry(MutationScaleJob::Tier, &first_recovered);
    recovery_evidence = run_scale_job(
        fixture,
        MutationScaleJob::Tier,
        recovery_evidence,
        minimum_physical_scan_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS + UNRELATED_ROWS),
        0,
        false,
    );
    assert_eq!(&scale_facts(fixture), completed);
    let stable_bytes_after_recovery = canister_stable_memory_bytes(fixture);
    assert!(stable_bytes_after_recovery >= stable_bytes_before_upgrade);
    (
        recovery_evidence,
        MutationScaleStartupEvidence {
            observation_instructions,
            stable_bytes_before_upgrade,
            stable_bytes_after_recovery,
            application_recovering_observations,
        },
    )
}

fn print_scale_evidence(
    tier: &MutationScaleRunEvidence,
    scoring: &MutationScaleRunEvidence,
    recovered: &MutationScaleRunEvidence,
    startup: &MutationScaleStartupEvidence,
) {
    let recovery_observation_instructions = startup.observation_instructions.as_slice();
    let recovery_ticks = recovery_observation_instructions.len();
    let recovery_observation_instructions_total = recovery_observation_instructions
        .iter()
        .copied()
        .sum::<u64>();
    let first_recovery_observation = recovery_observation_instructions
        .first()
        .copied()
        .unwrap_or(0);
    let representative_recovery_observation = recovery_observation_instructions
        .get(recovery_ticks / 2)
        .copied()
        .unwrap_or(0);
    let terminal_recovery_observation = recovery_observation_instructions
        .last()
        .copied()
        .unwrap_or(0);
    println!(
        "icydb_collection_scale rows={} recovery_ticks={} recovery_observation_instructions={} tier_forward_calls={} tier_verify_calls={} \
         tier_forward_max={} tier_recovery_reentry={} tier_verify_max={} tier_replay_max={} \
         scoring_forward_calls={} scoring_verify_calls={} scoring_forward_max={} \
         scoring_verify_max={} scoring_replay_max={} recovered_forward_max={} recovered_verify_max={} \
         recovery_observation_first={} recovery_observation_representative={} recovery_observation_terminal={} \
         mid_recovery_upgrade=true application_recovering_observations={} stable_bytes_before_upgrade={} stable_bytes_after_recovery={}",
        DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        recovery_ticks,
        recovery_observation_instructions_total,
        tier.forward_calls,
        tier.verify_calls,
        tier.max_forward_instructions,
        recovered
            .recovery_reentry_instructions
            .expect("tier recovery re-entry should be measured"),
        tier.max_verify_instructions,
        tier.max_replay_instructions,
        scoring.forward_calls,
        scoring.verify_calls,
        scoring.max_forward_instructions,
        scoring.max_verify_instructions,
        scoring.max_replay_instructions,
        recovered.max_recovered_forward_instructions,
        recovered.max_recovered_verify_instructions,
        first_recovery_observation,
        representative_recovery_observation,
        terminal_recovery_observation,
        startup.application_recovering_observations,
        startup.stable_bytes_before_upgrade,
        startup.stable_bytes_after_recovery,
    );
}

#[test]
fn collection_scale_jobs_finish_across_calls_and_upgrade() {
    let fixture = install_fixture_canister("sql_perf");
    load_scale_fixture(&fixture);
    let initial = assert_initial_scale_facts(&fixture);
    prove_eager_control_is_bounded(&fixture, &initial);
    let (tier_state, scoring_state) = start_composed_scale_jobs(&fixture);
    let (tier, scoring, completed) = run_warm_scale_jobs(&fixture, &tier_state, &scoring_state);
    let (recovered, startup) = recover_active_tier_job(&fixture, tier.sequence, &completed);
    print_scale_evidence(&tier, &scoring, &recovered, &startup);

    acknowledge_scale_job(&fixture, MutationScaleJob::Tier, recovered.sequence);
    acknowledge_scale_job(&fixture, MutationScaleJob::Scoring, scoring.sequence);
}
