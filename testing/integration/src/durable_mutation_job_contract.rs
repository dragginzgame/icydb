//! Stable 0.223 durable mutation-job contract and audit authority.
//!
//! This module owns declarations only. It does not execute mutations, publish
//! progress, or select recovery behavior. Later 0.223 patches must use these
//! identities and limits rather than substituting friendlier fixtures.

/// Current hard-cut fixture declaration format.
pub const DURABLE_MUTATION_JOB_CONTRACT_VERSION: u32 = 1;

/// Published predecessor used for every 0.223 comparison.
pub const DURABLE_MUTATION_JOB_BASELINE_TAG: &str = "v0.222.4";

/// Exact published predecessor commit.
pub const DURABLE_MUTATION_JOB_BASELINE_COMMIT: &str = "2a18f0aa3aade9cd209e8cec9e040d732e35b7af";

/// Exact published predecessor tree.
pub const DURABLE_MUTATION_JOB_BASELINE_TREE: &str = "09dc2a58d260932adac03c40c4cbdfbb2f11c1f2";

/// Matching rows in each collection-scale tier and scoring fixture.
pub const DURABLE_MUTATION_JOB_FIXTURE_ROWS: u32 = 10_001;

/// Current engine-owned maximum authoritative keys examined by one step.
pub const DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT: u32 = 4_096;

/// Current engine-owned maximum fixed updates staged by one Forward step.
pub const DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT: u32 = 240;

/// Current engine-owned maximum authoritative keys examined by one Verify step.
pub const DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT: u32 = 4_096;

/// Existing exact one-shot update admission ceiling used by the incident control.
pub const DURABLE_MUTATION_JOB_EAGER_UPDATE_ROW_LIMIT: u32 = 4_096;

/// Current maximum engine continuation retained inside one job.
pub const DURABLE_MUTATION_JOB_CONTINUATION_BYTES: u32 = 2 * 1_024;

/// Current maximum canonical accepted mutation intent.
pub const DURABLE_MUTATION_JOB_INTENT_BYTES: u32 = 16 * 1_024;

/// Current maximum retained replay receipt.
pub const DURABLE_MUTATION_JOB_RECEIPT_BYTES: u32 = 8 * 1_024;

/// Current maximum complete encoded mutation-job record.
pub const DURABLE_MUTATION_JOB_RECORD_BYTES: u32 = 64 * 1_024;

/// Shared current progress-store job capacity across all retained job families.
pub const DURABLE_MUTATION_JOB_GLOBAL_CAPACITY: u32 = 64;

/// Maximum shared occupancy admitted for non-integrity job families.
pub const DURABLE_PROGRESS_NON_INTEGRITY_CAPACITY: u32 = 56;

/// Exact shared slots reserved for Deep integrity work.
pub const DURABLE_PROGRESS_INTEGRITY_RESERVATION: u32 = 8;

/// Existing idempotency-key byte limit retained by the new job family.
pub const DURABLE_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES: u32 = 256;

/// Exact current sequence-zero record bytes for the small custody fixture.
pub const CURRENT_MUTATION_JOB_INITIAL_RECORD_BYTES: u32 = 97;

/// Exact current active-record bytes for the small replay fixture.
pub const CURRENT_MUTATION_JOB_REPLAY_RECORD_BYTES: u32 = 167;

/// Exact current completed-record bytes for the small custody fixture.
pub const CURRENT_MUTATION_JOB_COMPLETED_RECORD_BYTES: u32 = 165;

/// Exact current restart-required record bytes for the small custody fixture.
pub const CURRENT_MUTATION_JOB_RESTART_RECORD_BYTES: u32 = 166;

/// Exact current active record bytes with every bounded component maximized.
pub const CURRENT_MUTATION_JOB_MAX_ACTIVE_RECORD_BYTES: u32 = 18_842;

/// Exact retained replay-receipt bytes with a maximum idempotency key.
pub const CURRENT_MUTATION_JOB_MAX_REPLAY_RECEIPT_BYTES: u32 = 319;

/// Sole current marker envelope after atomic mutation-progress custody lands.
pub const CURRENT_MUTATION_PROGRESS_MARKER_VERSION: u8 = 1;

/// Exact maximum current mutation-progress contribution to one marker payload.
pub const CURRENT_MUTATION_PROGRESS_MAX_MARKER_PAYLOAD_BYTES: u32 = 37_797;

/// Excluded-allocation bytes at retained counts 55, 56, 63, and 64.
pub const CURRENT_PROGRESS_STABLE_BYTES_AT_RESERVATION_BOUNDARIES: [u64; 4] =
    [38_993_920, 38_993_920, 43_319_296, 43_319_296];

const _: () = {
    assert!(DURABLE_MUTATION_JOB_FIXTURE_ROWS > 10_000);
    assert!(DURABLE_MUTATION_JOB_FIXTURE_ROWS > DURABLE_MUTATION_JOB_EAGER_UPDATE_ROW_LIMIT);
    assert!(CURRENT_MUTATION_JOB_MAX_ACTIVE_RECORD_BYTES < DURABLE_MUTATION_JOB_RECORD_BYTES);
    assert!(CURRENT_MUTATION_JOB_MAX_REPLAY_RECEIPT_BYTES < DURABLE_MUTATION_JOB_RECEIPT_BYTES);
    assert!(
        DURABLE_PROGRESS_NON_INTEGRITY_CAPACITY + DURABLE_PROGRESS_INTEGRITY_RESERVATION
            == DURABLE_MUTATION_JOB_GLOBAL_CAPACITY
    );
    assert!(CURRENT_PROGRESS_STABLE_BYTES_AT_RESERVATION_BOUNDARIES.len() == 4);
    assert!(CURRENT_DURABLE_START_INSTRUCTIONS < DURABLE_START_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_START_REPLAY_INSTRUCTIONS < DURABLE_START_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_FORWARD_INSTRUCTIONS[0] < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_FORWARD_INSTRUCTIONS[1] < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_FORWARD_INSTRUCTIONS[2] < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING);
    assert!(
        CURRENT_DURABLE_MAX_FANOUT_FORWARD_INSTRUCTIONS
            < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING
    );
    assert!(CURRENT_DURABLE_VERIFY_INSTRUCTIONS[0] < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_VERIFY_INSTRUCTIONS[1] < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING);
    assert!(CURRENT_DURABLE_VERIFY_INSTRUCTIONS[2] < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING);
    assert!(
        CURRENT_DURABLE_VERIFY_REPLAY_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(
        CURRENT_DURABLE_VERIFY_DRIFT_RESTART_INSTRUCTIONS
            < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(CURRENT_DURABLE_STATE_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(
        CURRENT_DURABLE_COMPLETION_REPLAY_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(
        CURRENT_DURABLE_ACKNOWLEDGEMENT_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(CURRENT_DURABLE_CANCELLATION_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING);
    assert!(
        CURRENT_DURABLE_ABSENT_CANCELLATION_INSTRUCTIONS
            < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(
        CURRENT_DURABLE_INVENTORY_ONE_INSTRUCTIONS < DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING
    );
    assert!(
        CURRENT_DURABLE_INVENTORY_FULL_INSTRUCTIONS < DURABLE_INVENTORY_INSTRUCTION_REVIEW_CEILING
    );
};

/// Frozen existing current-control preparation measurement.
pub const BASELINE_PREPARE_INSTRUCTIONS: u64 = 430_996;

/// Frozen existing current-control Forward measurements over 512 matching rows.
pub const BASELINE_FORWARD_INSTRUCTIONS: &[u64] = &[
    25_553_863, 25_761_479, 26_037_553, 26_174_501, 26_396_047, 26_686_886, 26_826_814, 27_190_572,
];

/// Frozen existing current-control Verify measurements over 512 matching rows.
pub const BASELINE_VERIFY_INSTRUCTIONS: &[u64] = &[6_431_179, 6_440_484];

/// Maximum reviewed instruction cost for one durable start.
pub const DURABLE_START_INSTRUCTION_REVIEW_CEILING: u64 = 5_000_000;

/// Current first sequence-zero durable-start instruction sample.
pub const CURRENT_DURABLE_START_INSTRUCTIONS: u64 = 1_326_868;

/// Current canonically equivalent retained-start replay instruction sample.
pub const CURRENT_DURABLE_START_REPLAY_INSTRUCTIONS: u64 = 1_436_652;

/// Current durable 240-update Forward samples over the fixed 512-row fixture.
pub const CURRENT_DURABLE_FORWARD_INSTRUCTIONS: &[u64] = &[115_056_676, 122_858_524, 21_252_080];

/// Current 240-row Forward page with all 64 accepted secondary indexes.
pub const CURRENT_DURABLE_MAX_FANOUT_FORWARD_INSTRUCTIONS: u64 = 4_948_825_024;

/// Current exact replay sample for the final retained Forward receipt.
pub const CURRENT_DURABLE_FORWARD_REPLAY_INSTRUCTIONS: u64 = 123_012;

/// Current durable Verify samples over the fixed lifecycle fixture.
pub const CURRENT_DURABLE_VERIFY_INSTRUCTIONS: &[u64] = &[110_338_161, 107_492_238, 1_666_377];

/// Current exact replay sample for one retained nonterminal Verify receipt.
pub const CURRENT_DURABLE_VERIFY_REPLAY_INSTRUCTIONS: u64 = 124_872;

/// Current pre-scan revision-drift restart sample.
pub const CURRENT_DURABLE_VERIFY_DRIFT_RESTART_INSTRUCTIONS: u64 = 1_523_295;

/// Current terminal mutation-job state-load sample.
pub const CURRENT_DURABLE_STATE_INSTRUCTIONS: u64 = 104_993;

/// Current exact replay sample for the retained completion receipt.
pub const CURRENT_DURABLE_COMPLETION_REPLAY_INSTRUCTIONS: u64 = 106_131;

/// Current sequence-checked terminal acknowledgement sample.
pub const CURRENT_DURABLE_ACKNOWLEDGEMENT_INSTRUCTIONS: u64 = 117_863;

/// Current exact sequence-zero cancellation sample.
pub const CURRENT_DURABLE_CANCELLATION_INSTRUCTIONS: u64 = 144_089;

/// Current absent-record cancellation retry sample.
pub const CURRENT_DURABLE_ABSENT_CANCELLATION_INSTRUCTIONS: u64 = 37_249;

/// Current complete inventory sample with one retained mutation job.
pub const CURRENT_DURABLE_INVENTORY_ONE_INSTRUCTIONS: u64 = 116_690;

/// Current PocketIC 16 complete inventory sample at all 64 retained slots.
pub const CURRENT_DURABLE_INVENTORY_FULL_INSTRUCTIONS: u64 = 7_393_208;

/// Maximum reviewed instruction cost for one byte- and count-packed 240-update Forward step.
pub const DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING: u64 = 7_500_000_000;

/// Maximum reviewed instruction cost for one 4,096-key Verify step.
pub const DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING: u64 = 500_000_000;

/// Maximum reviewed instruction cost for state, replay, or acknowledgement.
pub const DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING: u64 = 2_000_000;

/// Maximum reviewed instruction cost for a complete 64-record inventory.
pub const DURABLE_INVENTORY_INSTRUCTION_REVIEW_CEILING: u64 = 7_500_000;

/// Per-patch raw-Wasm movement that requires explicit attribution.
pub const DURABLE_MUTATION_JOB_PATCH_WASM_REVIEW_BYTES: u64 = 64 * 1_024;

/// Complete-line raw-Wasm growth that blocks closeout without redesign.
pub const DURABLE_MUTATION_JOB_LINE_WASM_MAX_GROWTH_BYTES: u64 = 256 * 1_024;

/// Published dynamic-query raw final Wasm baseline.
pub const BASELINE_DYNAMIC_QUERY_RAW_WASM_BYTES: u64 = 2_607_381;

/// Published typed-query raw final Wasm baseline.
pub const BASELINE_TYPED_QUERY_RAW_WASM_BYTES: u64 = 1_792_657;

/// How one current owner participates in the durable mutation-job design.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MutationJobAuthorityDisposition {
    /// Reuse this authority without weakening its maintained contract.
    Reuse,
    /// Extend this authority in one current hard-cut format.
    Extend,
    /// Keep this authority separate because its semantics intentionally differ.
    KeepSeparate,
}

/// One source-anchored current owner that later patches must reconcile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationJobAuthorityEntry {
    /// Stable audit identity.
    pub id: &'static str,
    /// Workspace-relative current owner.
    pub owner_file: &'static str,
    /// Source marker proving the owner still exists.
    pub owner_symbol: &'static str,
    /// Planned patch that changes or consumes this authority.
    pub action_patch: u8,
    /// Required relationship to the durable mutation-job path.
    pub disposition: MutationJobAuthorityDisposition,
}

/// Current owner-family inventory reconciled from the published 0.222.4 boundary.
pub const MUTATION_JOB_AUTHORITY_INVENTORY: &[MutationJobAuthorityEntry] = &[
    MutationJobAuthorityEntry {
        id: "durable_mutation_custody",
        owner_file: "crates/icydb-core/src/db/session/mutation_job.rs",
        owner_symbol: "pub fn advance_trusted_mutation_job",
        action_patch: 5,
        disposition: MutationJobAuthorityDisposition::Reuse,
    },
    MutationJobAuthorityEntry {
        id: "bounded_forward_convergence",
        owner_file: "crates/icydb-core/src/db/session/sql/resumable_update.rs",
        owner_symbol: "fn advance_mutation_job_forward",
        action_patch: 5,
        disposition: MutationJobAuthorityDisposition::Reuse,
    },
    MutationJobAuthorityEntry {
        id: "stable_revision_verify",
        owner_file: "crates/icydb-core/src/db/session/sql/resumable_update.rs",
        owner_symbol: "fn scan_mutation_job_verify",
        action_patch: 6,
        disposition: MutationJobAuthorityDisposition::Reuse,
    },
    MutationJobAuthorityEntry {
        id: "generic_revision_strict_job_record",
        owner_file: "crates/icydb-core/src/db/resumable_job.rs",
        owner_symbol: "pub(in crate::db) struct ResumableJobRecord",
        action_patch: 2,
        disposition: MutationJobAuthorityDisposition::KeepSeparate,
    },
    MutationJobAuthorityEntry {
        id: "generic_pre_post_proof_coordinator",
        owner_file: "crates/icydb-core/src/db/session/resumable_job.rs",
        owner_symbol: "pub fn compare_proof_and_advance",
        action_patch: 2,
        disposition: MutationJobAuthorityDisposition::KeepSeparate,
    },
    MutationJobAuthorityEntry {
        id: "excluded_progress_allocation",
        owner_file: "crates/icydb-core/src/db/integrity/progress_store.rs",
        owner_symbol: "pub(in crate::db) struct InspectionProgressStore",
        action_patch: 2,
        disposition: MutationJobAuthorityDisposition::Extend,
    },
    MutationJobAuthorityEntry {
        id: "commit_marker_payload",
        owner_file: "crates/icydb-core/src/db/commit/marker.rs",
        owner_symbol: "pub(crate) struct CommitMarker",
        action_patch: 4,
        disposition: MutationJobAuthorityDisposition::Extend,
    },
    MutationJobAuthorityEntry {
        id: "request_aggregate_budget",
        owner_file: "crates/icydb-core/src/db/session/request.rs",
        owner_symbol: "pub struct RequestExecutionRoot",
        action_patch: 3,
        disposition: MutationJobAuthorityDisposition::Reuse,
    },
];

/// One immutable collection-scale or control operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableMutationJobFixture {
    /// Stable report identity.
    pub id: &'static str,
    /// Exact SQL lowered at start, or an empty string for a control-only call.
    pub sql: &'static str,
    /// Rows participating in the fixture.
    pub matching_rows: u32,
    /// Minimum separate successful advances required before completion.
    pub minimum_advances: u32,
}

/// Fixed fixture matrix. Later patches may add evidence but may not replace it.
pub const DURABLE_MUTATION_JOB_FIXTURES: &[DurableMutationJobFixture] = &[
    DurableMutationJobFixture {
        id: "eager_tier_reset_control_10001",
        sql: "UPDATE PerfAuditMutationToken SET tier = 'Default' WHERE collection_id = 7",
        matching_rows: DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        minimum_advances: 1,
    },
    DurableMutationJobFixture {
        id: "durable_tier_reset_10001",
        sql: "UPDATE PerfAuditMutationToken SET tier = 'Default' WHERE collection_id = 7",
        matching_rows: DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        minimum_advances: minimum_forward_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
    },
    DurableMutationJobFixture {
        id: "durable_scoring_invalidation_10001",
        sql: "UPDATE PerfAuditMutationScoringState SET score_stale = true WHERE collection_id = 7",
        matching_rows: DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        minimum_advances: minimum_forward_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
    },
    DurableMutationJobFixture {
        id: "durable_verify_clean_10001",
        sql: "UPDATE PerfAuditMutationToken SET tier = 'Default' WHERE collection_id = 7",
        matching_rows: DURABLE_MUTATION_JOB_FIXTURE_ROWS,
        minimum_advances: minimum_verify_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
    },
    DurableMutationJobFixture {
        id: "state_load_control",
        sql: "",
        matching_rows: 0,
        minimum_advances: 1,
    },
    DurableMutationJobFixture {
        id: "exact_replay_control",
        sql: "",
        matching_rows: 0,
        minimum_advances: 1,
    },
    DurableMutationJobFixture {
        id: "terminal_acknowledgement_control",
        sql: "",
        matching_rows: 0,
        minimum_advances: 1,
    },
];

/// Durable result expected after an injected interruption and recovery.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MutationJobFailpointOutcome {
    /// Neither target rows nor progress advanced.
    BeforeState,
    /// Recovery must finish the exact target plus progress transition.
    RecoverToAfterState,
    /// The exact after-state and receipt are already durable.
    AfterState,
}

/// One fixed interruption boundary for Patch 4 and final recovery evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationJobFailpointFixture {
    /// Stable failpoint identity.
    pub id: &'static str,
    /// Required state after normal recovery.
    pub outcome: MutationJobFailpointOutcome,
}

/// Fixed failpoint matrix for the marker-owned transition.
pub const MUTATION_JOB_FAILPOINT_FIXTURES: &[MutationJobFailpointFixture] = &[
    MutationJobFailpointFixture {
        id: "start_before_progress_insert",
        outcome: MutationJobFailpointOutcome::BeforeState,
    },
    MutationJobFailpointFixture {
        id: "start_after_progress_insert",
        outcome: MutationJobFailpointOutcome::AfterState,
    },
    MutationJobFailpointFixture {
        id: "forward_before_marker_persist",
        outcome: MutationJobFailpointOutcome::BeforeState,
    },
    MutationJobFailpointFixture {
        id: "forward_after_marker_persist",
        outcome: MutationJobFailpointOutcome::RecoverToAfterState,
    },
    MutationJobFailpointFixture {
        id: "forward_after_target_apply",
        outcome: MutationJobFailpointOutcome::RecoverToAfterState,
    },
    MutationJobFailpointFixture {
        id: "forward_after_progress_replace",
        outcome: MutationJobFailpointOutcome::RecoverToAfterState,
    },
    MutationJobFailpointFixture {
        id: "forward_after_marker_clear",
        outcome: MutationJobFailpointOutcome::AfterState,
    },
    MutationJobFailpointFixture {
        id: "verify_before_progress_replace",
        outcome: MutationJobFailpointOutcome::BeforeState,
    },
    MutationJobFailpointFixture {
        id: "verify_after_progress_replace",
        outcome: MutationJobFailpointOutcome::AfterState,
    },
];

/// Minimum mutating calls needed when every matching row needs the patch.
#[must_use]
pub const fn minimum_forward_advances(rows: u32) -> u32 {
    rows.div_ceil(DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT)
}

/// Minimum clean Verify calls needed to prove exhaustion.
#[must_use]
pub const fn minimum_verify_advances(rows: u32) -> u32 {
    rows.div_ceil(DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use super::*;

    #[test]
    fn published_baseline_and_bounded_scale_are_frozen() {
        assert_eq!(DURABLE_MUTATION_JOB_CONTRACT_VERSION, 1);
        assert_eq!(DURABLE_MUTATION_JOB_BASELINE_TAG, "v0.222.4");
        assert_eq!(DURABLE_MUTATION_JOB_BASELINE_COMMIT.len(), 40);
        assert_eq!(DURABLE_MUTATION_JOB_BASELINE_TREE.len(), 40);
        assert_eq!(minimum_forward_advances(10_001), 42);
        assert_eq!(minimum_verify_advances(10_001), 3);
        assert_eq!(BASELINE_FORWARD_INSTRUCTIONS.len(), 8);
        assert_eq!(BASELINE_VERIFY_INSTRUCTIONS.len(), 2);
        assert_eq!(CURRENT_DURABLE_FORWARD_INSTRUCTIONS.len(), 3);
        assert_eq!(CURRENT_DURABLE_VERIFY_INSTRUCTIONS.len(), 3);
        assert_eq!(BASELINE_DYNAMIC_QUERY_RAW_WASM_BYTES, 2_607_381);
        assert_eq!(BASELINE_TYPED_QUERY_RAW_WASM_BYTES, 1_792_657);
        assert!(
            BASELINE_FORWARD_INSTRUCTIONS
                .iter()
                .all(|value| *value < DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING)
        );
        assert!(
            BASELINE_VERIFY_INSTRUCTIONS
                .iter()
                .all(|value| *value < DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING)
        );
    }

    #[test]
    fn published_runtime_limits_are_source_anchored() {
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_CONTINUATION_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_CONTINUATION_BYTES)
                .expect("continuation byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_INTENT_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_INTENT_BYTES)
                .expect("intent byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_RECEIPT_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_RECEIPT_BYTES)
                .expect("receipt byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_RECORD_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_RECORD_BYTES)
                .expect("record byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES)
                .expect("idempotency-key byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_STEP_KEYS_SCANNED,
            u64::from(DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
            u64::from(DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT),
        );

        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let resumable_update = fs::read_to_string(
            workspace.join("crates/icydb-core/src/db/session/sql/resumable_update.rs"),
        )
        .expect("resumable update authority should be readable");
        assert!(resumable_update.contains(
            "MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES,\n    RESUMABLE_UPDATE_CONTINUATION_BYTES_POLICY,\n    2 * 1024"
        ));
        assert!(resumable_update.contains(
            "MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED,\n    RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED_POLICY,\n    4_096"
        ));
        assert!(resumable_update.contains(
            "MAX_RESUMABLE_UPDATE_FORWARD_ROWS,\n    RESUMABLE_UPDATE_FORWARD_ROWS_POLICY,\n    MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT"
        ));
        assert!(resumable_update.contains(
            "MAX_RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED,\n    RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED_POLICY,\n    4_096"
        ));
        assert!(resumable_update.contains("MUTATION_EXECUTION_BUDGET_POLICY_IDENTITY"));

        let execution_budget =
            fs::read_to_string(workspace.join("crates/icydb-core/src/db/executor/budget.rs"))
                .expect("mutation execution-budget authority should be readable");
        assert!(
            execution_budget
                .contains("const MUTATION_EXECUTION_INSTRUCTION_LIMIT: u64 = 30_000_000_000;")
        );
        assert!(execution_budget.contains(
            "const MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE: u64 = 5_000_000_000;"
        ));

        let exact_update = fs::read_to_string(
            workspace.join("crates/icydb-core/src/db/session/sql/update_policy/model.rs"),
        )
        .expect("exact update authority should be readable");
        assert!(exact_update.contains("const MAX_TRUSTED_EXACT_UPDATE_ROWS: u32 = 4_096;"));

        let progress_store = fs::read_to_string(
            workspace.join("crates/icydb-core/src/db/integrity/progress_store.rs"),
        )
        .expect("progress-store authority should be readable");
        assert!(progress_store.contains("const MAX_PROGRESS_RECORD_BYTES: u32 = 512 * 1024;"));
        assert!(progress_store.contains("const MAX_PROGRESS_JOBS_GLOBAL: u64 = 64;"));
        assert!(progress_store.contains("const MAX_PROGRESS_JOBS_NON_INTEGRITY: u64 = 56;"));
        assert!(
            progress_store.contains("MAX_PROGRESS_JOBS_GLOBAL - MAX_PROGRESS_JOBS_NON_INTEGRITY")
        );
        assert!(progress_store.contains("pub(in crate::db) fn cancel_unadvanced_mutation("));
        assert!(progress_store.contains("pub(in crate::db) fn inventory(&self)"));

        let canonical_intent =
            fs::read_to_string(workspace.join("crates/icydb-core/src/db/mutation_job/intent.rs"))
                .expect("canonical mutation intent authority should be readable");
        assert!(canonical_intent.contains("const INTENT_FORMAT_VERSION: u8 = 1;"));
        assert!(canonical_intent.contains("const MAX_CANONICAL_EXPR_DEPTH: usize = 32;"));
        assert!(canonical_intent.contains("const MAX_CANONICAL_EXPR_NODES: usize = 256;"));

        let mutation_session =
            fs::read_to_string(workspace.join("crates/icydb-core/src/db/session/mutation_job.rs"))
                .expect("mutation-job session authority should be readable");
        assert!(mutation_session.contains("pub fn start_trusted_sql_mutation_job("));
        assert!(mutation_session.contains("InsertMutationJobResult::Occupied(retained)"));
        assert!(mutation_session.contains("pub fn cancel_unadvanced_mutation_job("));
        assert!(mutation_session.contains("pub fn progress_job_inventory("));

        let commit_marker =
            fs::read_to_string(workspace.join("crates/icydb-core/src/db/commit/marker.rs"))
                .expect("commit-marker authority should be readable");
        assert!(commit_marker.contains("const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 1;"));
        assert!(commit_marker.contains("from_parts_with_mutation_progress"));
        assert!(commit_marker.contains("DatabaseControlOp::MutationProgress"));
        assert_eq!(CURRENT_MUTATION_PROGRESS_MARKER_VERSION, 1);
        assert_eq!(CURRENT_MUTATION_PROGRESS_MAX_MARKER_PAYLOAD_BYTES, 37_797);
    }

    #[test]
    fn current_authority_inventory_is_unique_and_source_anchored() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut ids = BTreeSet::new();
        let mut dispositions = BTreeSet::new();
        for entry in MUTATION_JOB_AUTHORITY_INVENTORY {
            assert!(ids.insert(entry.id), "duplicate authority id {}", entry.id);
            assert!((2..=6).contains(&entry.action_patch));
            dispositions.insert(entry.disposition);
            let source = fs::read_to_string(workspace.join(entry.owner_file))
                .unwrap_or_else(|error| panic!("{} should be readable: {error}", entry.owner_file));
            assert!(
                source.contains(entry.owner_symbol),
                "{} must retain source marker {}",
                entry.owner_file,
                entry.owner_symbol,
            );
        }
        assert_eq!(dispositions.len(), 3);
    }

    #[test]
    fn fixed_operation_and_failpoint_matrices_are_unique_and_complete() {
        let mut fixture_ids = BTreeSet::new();
        for fixture in DURABLE_MUTATION_JOB_FIXTURES {
            assert!(
                fixture_ids.insert(fixture.id),
                "duplicate fixture {}",
                fixture.id
            );
            assert!(fixture.sql.len() <= 512);
            assert!(fixture.minimum_advances > 0);
        }
        assert_eq!(
            DURABLE_MUTATION_JOB_FIXTURES
                .iter()
                .filter(|fixture| fixture.matching_rows == DURABLE_MUTATION_JOB_FIXTURE_ROWS)
                .count(),
            4,
        );

        let mut failpoint_ids = BTreeSet::new();
        let mut outcomes = BTreeSet::new();
        for fixture in MUTATION_JOB_FAILPOINT_FIXTURES {
            assert!(
                failpoint_ids.insert(fixture.id),
                "duplicate failpoint {}",
                fixture.id
            );
            outcomes.insert(fixture.outcome);
        }
        assert_eq!(outcomes.len(), 3);
    }
}
