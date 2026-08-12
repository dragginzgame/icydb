//! Toko-shaped 0.223 durable mutation-job scale and composition evidence.

use candid::CandidType;
use icydb::{
    Error,
    db::{
        MutationJobAdvanceReceipt, MutationJobError, MutationJobPhase, MutationJobState,
        MutationJobStatus, sql::SqlQueryResult,
    },
    diagnostic::{DiagnosticDetail, SqlWriteBoundaryCode},
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
            .update_candid("load_toko_mutation_scale_page", (first_id, row_count))
            .expect("scale load page should decode");
        let evidence = evidence.expect("scale load page should succeed");
        assert_eq!(evidence.first_id, first_id);
        assert_eq!(evidence.last_id, first_id + row_count - 1);
        assert_eq!(evidence.matching_rows_loaded, row_count);
        assert_eq!(
            evidence.unrelated_rows_loaded,
            if first_id == 1 { UNRELATED_ROWS } else { 0 },
        );
        loaded = loaded.saturating_add(row_count);
        first_id = first_id.saturating_add(row_count);
    }
    assert_eq!(loaded, DURABLE_MUTATION_JOB_FIXTURE_ROWS);
}

fn scale_facts(fixture: &ic_testkit::pic::StandaloneCanisterFixture) -> MutationScaleFixtureFacts {
    let count = |fact| {
        let count: Result<u32, Error> = fixture
            .query_candid("toko_mutation_scale_fact", (fact,))
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

fn start_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
) -> MutationJobState {
    let state: Result<MutationJobState, MutationJobError> = fixture
        .update_candid("start_toko_mutation_scale_job", (job,))
        .expect("scale start should decode");
    state.expect("scale start should succeed")
}

fn scale_job_state(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
) -> Result<MutationJobState, MutationJobError> {
    fixture
        .update_candid("toko_mutation_scale_job_state", (job,))
        .expect("scale job state should decode")
}

fn advance_scale_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    job: MutationScaleJob,
    sequence: u64,
) -> Result<MutationScaleAdvancePerfResult, MutationJobError> {
    fixture
        .update_candid(
            "advance_toko_mutation_scale_job",
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
            .update_candid("acknowledge_toko_mutation_scale_job", (job, sequence))
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
        .update_candid("try_toko_eager_tier_reset", ())
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

fn recover_active_tier_job(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    tier_sequence: u64,
    completed: &MutationScaleFixtureFacts,
) -> (MutationScaleRunEvidence, Vec<u64>) {
    // Reuse the converged tier intent to prove that an active private
    // continuation survives upgrade without conflating recovered stable-store
    // cost with the frozen warm Forward fixture.
    acknowledge_scale_job(fixture, MutationScaleJob::Tier, tier_sequence);
    let recovery_state = start_scale_job(fixture, MutationScaleJob::Tier);
    upgrade_fixture_canister(fixture, "sql_perf");
    let mut recovery_pages = 0_u32;
    let mut recovery_instructions = 0_u64;
    let mut recovery_page_instructions = Vec::new();
    loop {
        let recovery: Result<MutationScaleRecoveryEvidence, Error> = fixture
            .update_candid("recover_toko_mutation_scale_store", ())
            .expect("scale recovery evidence should decode");
        let recovery = recovery.expect("scale target store should recover after upgrade");
        recovery_pages = recovery_pages.saturating_add(1);
        recovery_instructions = recovery_instructions.saturating_add(recovery.local_instructions);
        recovery_page_instructions.push(recovery.local_instructions);
        assert!(recovery.local_instructions > 0);
        assert!(recovery.local_instructions < 40_000_000_000);
        if recovery.complete {
            assert_eq!(recovery.warmed_rows, 1);
            break;
        }
        assert_eq!(recovery.warmed_rows, 0);
        assert!(
            recovery_pages < 32,
            "startup recovery must make bounded progress"
        );
    }
    assert!(
        recovery_pages > 1,
        "the scale fixture must exercise resumption"
    );
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
    assert_eq!(
        recovery_instructions,
        recovery_page_instructions.iter().copied().sum::<u64>(),
    );
    (recovery_evidence, recovery_page_instructions)
}

fn print_scale_evidence(
    tier: &MutationScaleRunEvidence,
    scoring: &MutationScaleRunEvidence,
    recovered: &MutationScaleRunEvidence,
    recovery_page_instructions: &[u64],
) {
    let recovery_pages = recovery_page_instructions.len();
    let recovery_instructions = recovery_page_instructions.iter().copied().sum::<u64>();
    let first_recovery_page = recovery_page_instructions.first().copied().unwrap_or(0);
    let representative_recovery_page = recovery_page_instructions
        .get(recovery_pages / 2)
        .copied()
        .unwrap_or(0);
    let terminal_recovery_page = recovery_page_instructions.last().copied().unwrap_or(0);
    println!(
        "icydb_0223_toko_scale rows={} recovery_pages={} recovery_instructions={} tier_forward_calls={} tier_verify_calls={} \
         tier_forward_max={} tier_recovery_reentry={} tier_verify_max={} tier_replay_max={} \
         scoring_forward_calls={} scoring_verify_calls={} scoring_forward_max={} \
         scoring_verify_max={} scoring_replay_max={} recovered_forward_max={} recovered_verify_max={} \
         recovery_first={} recovery_representative={} recovery_terminal={}",
        DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        recovery_pages,
        recovery_instructions,
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
        first_recovery_page,
        representative_recovery_page,
        terminal_recovery_page,
    );
}

#[test]
fn toko_shaped_jobs_finish_across_calls_and_upgrade() {
    let fixture = install_fixture_canister("sql_perf");
    load_scale_fixture(&fixture);
    let initial = assert_initial_scale_facts(&fixture);
    prove_eager_control_is_bounded(&fixture, &initial);
    let (tier_state, scoring_state) = start_composed_scale_jobs(&fixture);
    let (tier, scoring, completed) = run_warm_scale_jobs(&fixture, &tier_state, &scoring_state);
    let (recovered, recovery_page_instructions) =
        recover_active_tier_job(&fixture, tier.sequence, &completed);
    print_scale_evidence(&tier, &scoring, &recovered, &recovery_page_instructions);

    acknowledge_scale_job(&fixture, MutationScaleJob::Tier, recovered.sequence);
    acknowledge_scale_job(&fixture, MutationScaleJob::Scoring, scoring.sequence);
}
