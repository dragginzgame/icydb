//! Module: db::startup
//! Responsibility: derive bounded generated-database startup readiness.
//! Does not own: recovery execution, watchdog registration, or ordinary-operation admission.
//! Boundary: fixed durable controls plus runtime recovery witness -> readiness or typed failure.

mod driver;
mod observe;
pub(in crate::db) mod receipt;

use candid::CandidType;
use icydb_diagnostic_code::{Diagnostic, DiagnosticFactTag, MAX_PUBLIC_DIAGNOSTIC_FACTS};
use serde::Deserialize;

use crate::{db::StoreRegistry, error::InternalError, traits::CanisterKind};

/// Current generated-database startup readiness.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum DatabaseStartupState {
    /// Recovery controls are complete and the generated schema is reconciled.
    Ready,
    /// Dedicated replicated startup work remains.
    Recovering,
}

/// One bounded outcome from the hidden replicated startup coordinator.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedStartupDriverStep {
    /// Startup is already ready or has one durably observable terminal failure.
    Terminal,
    /// Recovery remains pending after exactly one bounded page attempt.
    Recovering,
    /// Recovery is complete and generated schema reconciliation must run now.
    ApplyGeneratedSchema,
}

/// Closed owner of one terminal startup failure.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum StartupFailureKind {
    /// Database boot, incarnation, or commit-control failure.
    DatabaseControl,
    /// Journal-tail or fold-continuation recovery failure.
    JournalRecovery,
    /// Generated-schema reconciliation failure.
    SchemaReconciliation,
}

/// Internal bounded startup failure projected by the public facade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupFailure {
    kind: StartupFailureKind,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
}

impl StartupFailure {
    pub(in crate::db) fn from_internal(kind: StartupFailureKind, error: &InternalError) -> Self {
        Self::new(kind, error.diagnostic(), error.diagnostic_facts())
    }

    pub(in crate::db) fn new(
        kind: StartupFailureKind,
        diagnostic: Diagnostic,
        facts: Vec<(DiagnosticFactTag, u64)>,
    ) -> Self {
        debug_assert!(facts.len() <= MAX_PUBLIC_DIAGNOSTIC_FACTS);
        Self {
            kind,
            diagnostic,
            facts,
        }
    }

    /// Return the subsystem that owns this terminal startup failure.
    #[must_use]
    pub const fn kind(&self) -> StartupFailureKind {
        self.kind
    }

    /// Return its compact diagnostic identity.
    #[must_use]
    pub const fn diagnostic(&self) -> &Diagnostic {
        &self.diagnostic
    }

    /// Borrow its bounded numeric diagnostic facts.
    #[must_use]
    pub const fn facts(&self) -> &[(DiagnosticFactTag, u64)] {
        self.facts.as_slice()
    }
}

#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the classifier is consumed by the next driver slice"
    )
)]
pub(in crate::db) fn classify_terminal_failure(
    kind: StartupFailureKind,
    error: &InternalError,
) -> Option<StartupFailure> {
    terminal_code_for_kind(kind, error.diagnostic().error_code())
        .then(|| StartupFailure::from_internal(kind, error))
}

pub(in crate::db) fn classify_terminal_failure_parts(
    kind: StartupFailureKind,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
) -> Option<StartupFailure> {
    terminal_code_for_kind(kind, diagnostic.error_code())
        .then(|| StartupFailure::new(kind, diagnostic, facts))
}

fn terminal_code_for_kind(
    kind: StartupFailureKind,
    code: icydb_diagnostic_code::ErrorCode,
) -> bool {
    use icydb_diagnostic_code::ErrorCode;

    let persisted_failure = code == ErrorCode::STORE_CORRUPTION
        || code == ErrorCode::STORE_INVARIANT_VIOLATION
        || code == ErrorCode::RUNTIME_CORRUPTION
        || code == ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT
        || code == ErrorCode::RUNTIME_INVARIANT_VIOLATION
        || code == ErrorCode::RUNTIME_BOUNDARY_PERSISTED_ROW_LAYOUT_OUTSIDE_ACCEPTED_WINDOW
        || code == ErrorCode::RUNTIME_BOUNDARY_PERSISTED_ROW_SLOT_COUNT_MISMATCH
        || code == ErrorCode::RUNTIME_BOUNDARY_ACCEPTED_ROW_CONSTRAINT_PROGRAM_CORRUPT;
    persisted_failure
        || match kind {
            StartupFailureKind::DatabaseControl => false,
            StartupFailureKind::JournalRecovery => {
                code == ErrorCode::RUNTIME_BOUNDARY_JOURNAL_MUTATION_REVISION_EXHAUSTED
            }
            StartupFailureKind::SchemaReconciliation => {
                code == ErrorCode::SCHEMA_DDL_ADMISSION
                    || code == ErrorCode::RUNTIME_CONFLICT
                    || code == ErrorCode::RUNTIME_UNSUPPORTED
                    || code == ErrorCode::RUNTIME_BOUNDARY_GENERATED_FIELD_AFTER_DDL_FIELD
                    || code == ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
                    || code == ErrorCode::RUNTIME_BOUNDARY_GENERATED_CONSTRAINT_ACTIVATION_STALE
            }
        }
}

/// Observe one generated database without opening a request or advancing recovery.
pub fn observe_generated_startup_state<C: CanisterKind>(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<DatabaseStartupState, StartupFailure> {
    observe::observe::<C>(stores, submission_key)
}

/// Run at most one bounded recovery page without admitting ordinary work.
#[doc(hidden)]
pub fn drive_generated_startup_recovery_page<C: CanisterKind>(
    session: &crate::db::DbSession<C>,
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    driver::drive_recovery_page(session, stores, submission_key)
}

/// Persist one deterministic generated-schema failure against fresh authority.
#[doc(hidden)]
pub fn record_generated_schema_startup_failure<C: CanisterKind>(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
) -> Result<bool, InternalError> {
    driver::record_schema_failure::<C>(stores, submission_key, diagnostic, facts)
}

/// Clear a stale startup failure after an authoritative successful handoff.
#[doc(hidden)]
pub fn clear_generated_startup_failure<C: CanisterKind>() -> Result<bool, InternalError> {
    receipt::clear::<C>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            DataStore, IndexStore, StoreAllocationIdentities, StoreRuntimeStorageCapabilities,
            commit::{
                CommitMarker, begin_commit, commit_memory_handle, configure_commit_memory_id,
                current_commit_memory_allocation, database_incarnation_id, finish_commit,
                mark_startup_recovery_complete_for_tests, persist_raw_commit_marker_for_tests,
            },
            database_format::{
                ensure_database_format_admitted, initialize_current_database_control_for_tests,
            },
            journal::{
                JournalBatch, JournalRecord, JournalSequence, JournalTailStore,
                encode_journal_batch,
            },
            registry::StoreAllocationIdentity,
            schema::{
                SchemaApplicationRecord, SchemaApplicationRecordOp, SchemaChangeOutcome,
                SchemaChangeReceipt, SchemaStore, apply_schema_application_record_op,
                corrupt_live_schema_checkpoint_header_for_tests, generated_schema_authority,
                load_schema_application_record_read_only,
            },
            session::RequestExecutionRoot,
        },
        testing::test_memory,
        traits::Path,
    };
    use ic_stable_structures::Memory;
    use icydb_diagnostic_code::{ErrorCode, ErrorOrigin as DiagnosticOrigin};
    use icydb_schema::{SchemaProposalDigest, SchemaSubmissionKey};
    use std::cell::RefCell;

    struct FreshCanister;

    impl Path for FreshCanister {
        const PATH: &'static str = "startup_tests::FreshCanister";
    }

    impl CanisterKind for FreshCanister {
        const COMMIT_MEMORY_ID: u8 = 232;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_fresh.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 233;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_fresh.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 234;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.startup_fresh.integrity.v1";
    }

    thread_local! {
        static FRESH_STORES: StoreRegistry = StoreRegistry::new();
        static CURRENT_STORES: StoreRegistry = StoreRegistry::new();
    }

    struct CurrentCanister;

    impl Path for CurrentCanister {
        const PATH: &'static str = "startup_tests::CurrentCanister";
    }

    impl CanisterKind for CurrentCanister {
        const COMMIT_MEMORY_ID: u8 = 228;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_current.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 229;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_current.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 230;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup_current.integrity.v1";
    }

    struct CorruptCanister;

    impl Path for CorruptCanister {
        const PATH: &'static str = "startup_tests::CorruptCanister";
    }

    impl CanisterKind for CorruptCanister {
        const COMMIT_MEMORY_ID: u8 = 224;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_corrupt.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 225;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_corrupt.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 226;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup_corrupt.integrity.v1";
    }

    thread_local! {
        static CORRUPT_STORES: StoreRegistry = StoreRegistry::new();
    }

    struct DriverCanister;

    impl Path for DriverCanister {
        const PATH: &'static str = "startup_tests::DriverCanister";
    }

    impl CanisterKind for DriverCanister {
        const COMMIT_MEMORY_ID: u8 = 248;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_driver.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 249;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_driver.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 250;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup_driver.integrity.v1";
    }

    thread_local! {
        static DRIVER_STORES: StoreRegistry = StoreRegistry::new();
    }

    struct HeapRecoveryFailureCanister;

    impl Path for HeapRecoveryFailureCanister {
        const PATH: &'static str = "startup_tests::HeapRecoveryFailureCanister";
    }

    impl CanisterKind for HeapRecoveryFailureCanister {
        const COMMIT_MEMORY_ID: u8 = 251;
        const COMMIT_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.recovery.failure.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 245;
        const STARTUP_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.recovery.failure.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 246;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.recovery.failure.integrity.v1";
    }

    thread_local! {
        static HEAP_RECOVERY_FAILURE_STORES: StoreRegistry = StoreRegistry::new();
    }

    struct HeapCheckpointFailureCanister;

    impl Path for HeapCheckpointFailureCanister {
        const PATH: &'static str = "startup_tests::HeapCheckpointFailureCanister";
    }

    impl CanisterKind for HeapCheckpointFailureCanister {
        const COMMIT_MEMORY_ID: u8 = 254;
        const COMMIT_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.checkpoint.failure.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 221;
        const STARTUP_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.checkpoint.failure.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 222;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup.heap.checkpoint.failure.integrity.v1";
    }

    thread_local! {
        static HEAP_CHECKPOINT_FAILURE_DATA: RefCell<DataStore> =
            const { RefCell::new(DataStore::init_heap()) };
        static HEAP_CHECKPOINT_FAILURE_INDEX: RefCell<IndexStore> =
            const { RefCell::new(IndexStore::init_heap()) };
        static HEAP_CHECKPOINT_FAILURE_SCHEMA: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static HEAP_CHECKPOINT_FAILURE_STORES: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                "startup_tests::HeapCheckpointFailureStore",
                &HEAP_CHECKPOINT_FAILURE_DATA,
                &HEAP_CHECKPOINT_FAILURE_INDEX,
                &HEAP_CHECKPOINT_FAILURE_SCHEMA,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("heap checkpoint failure store should register");
            registry
        };
    }

    const JOURNAL_RECOVERY_FAILURE_STORE_PATH: &str = "startup_tests::JournalRecoveryFailureStore";

    struct JournalRecoveryFailureCanister;

    impl Path for JournalRecoveryFailureCanister {
        const PATH: &'static str = "startup_tests::JournalRecoveryFailureCanister";
    }

    impl CanisterKind for JournalRecoveryFailureCanister {
        const COMMIT_MEMORY_ID: u8 = 185;
        const COMMIT_STABLE_KEY: &'static str =
            "icydb.test.startup.journal.recovery.failure.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 186;
        const STARTUP_STABLE_KEY: &'static str =
            "icydb.test.startup.journal.recovery.failure.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 187;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup.journal.recovery.failure.integrity.v1";
    }

    thread_local! {
        static JOURNAL_RECOVERY_FAILURE_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(181)));
        static JOURNAL_RECOVERY_FAILURE_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(182)));
        static JOURNAL_RECOVERY_FAILURE_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(183)));
        static JOURNAL_RECOVERY_FAILURE_TAIL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(184)));
        static JOURNAL_RECOVERY_FAILURE_STORES: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                JOURNAL_RECOVERY_FAILURE_STORE_PATH,
                &JOURNAL_RECOVERY_FAILURE_DATA,
                &JOURNAL_RECOVERY_FAILURE_INDEX,
                &JOURNAL_RECOVERY_FAILURE_SCHEMA,
                &JOURNAL_RECOVERY_FAILURE_TAIL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(
                        181,
                        "icydb.test.startup.journal.recovery.failure.data.v1",
                    ),
                    StoreAllocationIdentity::new(
                        182,
                        "icydb.test.startup.journal.recovery.failure.index.v1",
                    ),
                    StoreAllocationIdentity::new(
                        183,
                        "icydb.test.startup.journal.recovery.failure.schema.v1",
                    ),
                    StoreAllocationIdentity::new(
                        184,
                        "icydb.test.startup.journal.recovery.failure.journal.v1",
                    ),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("journal recovery failure store should register");
            registry
        };
    }

    #[test]
    fn fresh_observation_is_recovering_and_performs_no_stable_write() {
        assert_eq!(
            observe_generated_startup_state::<FreshCanister>(
                &FRESH_STORES,
                "generated/0123456789abcdef",
            ),
            Ok(DatabaseStartupState::Recovering)
        );
        let commit = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should configure"),
        )
        .expect("commit memory should reopen");
        let startup =
            receipt::startup_memory::<FreshCanister>().expect("startup memory should reopen");
        assert_eq!(commit.size(), 0);
        assert_eq!(startup.size(), 0);
    }

    #[test]
    fn terminal_classification_is_typed_and_pending_or_internal_failures_remain_retryable() {
        let corruption = InternalError::store_corruption();
        assert!(
            classify_terminal_failure(StartupFailureKind::JournalRecovery, &corruption).is_some()
        );
        let pending = InternalError::recovery_pending();
        assert!(classify_terminal_failure(StartupFailureKind::JournalRecovery, &pending).is_none());
        let transient = InternalError::recovery_database_format_control_unavailable();
        assert!(
            classify_terminal_failure(StartupFailureKind::DatabaseControl, &transient).is_none()
        );
    }

    #[test]
    fn malformed_fixed_boot_control_surfaces_directly_without_a_failure_receipt() {
        configure_commit_memory_id(
            CorruptCanister::COMMIT_MEMORY_ID,
            CorruptCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        assert_eq!(memory.grow(1), 0);
        memory.write(0, b"NOTICYDBCONTROL");

        let failure = observe_generated_startup_state::<CorruptCanister>(
            &CORRUPT_STORES,
            "generated/0123456789abcdef",
        )
        .expect_err("malformed boot control must fail directly");
        assert_eq!(failure.kind(), StartupFailureKind::DatabaseControl);
        assert_eq!(
            failure.diagnostic().class(),
            icydb_diagnostic_code::ErrorClass::Corruption,
        );
        assert_eq!(
            receipt::startup_memory::<CorruptCanister>()
                .expect("startup memory should open")
                .size(),
            0,
        );
    }

    #[test]
    fn heap_only_malformed_marker_becomes_a_durable_database_control_failure() {
        const SUBMISSION: &str = "generated/89abcdef01234567";

        configure_commit_memory_id(
            HeapRecoveryFailureCanister::COMMIT_MEMORY_ID,
            HeapRecoveryFailureCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        initialize_current_database_control_for_tests(&memory);
        persist_raw_commit_marker_for_tests(vec![0xff])
            .expect("malformed marker payload should persist inside valid control authority");

        assert_eq!(
            observe_generated_startup_state::<HeapRecoveryFailureCanister>(
                &HEAP_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            ),
            Ok(DatabaseStartupState::Recovering),
            "bounded observation must not decode marker payloads",
        );

        let request_root = RequestExecutionRoot::__new_runtime_root();
        let session = crate::db::DbSession::<HeapRecoveryFailureCanister>::new(
            &HEAP_RECOVERY_FAILURE_STORES,
            &request_root,
        );
        assert_eq!(
            drive_generated_startup_recovery_page(
                &session,
                &HEAP_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            )
            .expect("terminal corruption should publish one durable receipt"),
            GeneratedStartupDriverStep::Terminal,
        );

        let failure = observe_generated_startup_state::<HeapRecoveryFailureCanister>(
            &HEAP_RECOVERY_FAILURE_STORES,
            SUBMISSION,
        )
        .expect_err("the durable database-control failure should replace blind recovery retries");
        assert_eq!(failure.kind(), StartupFailureKind::DatabaseControl);
        assert_eq!(
            failure.diagnostic().error_code(),
            ErrorCode::RUNTIME_CORRUPTION
        );
        assert_eq!(
            drive_generated_startup_recovery_page(
                &session,
                &HEAP_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            )
            .expect("exact terminal replay should not attempt recovery again"),
            GeneratedStartupDriverStep::Terminal,
        );
    }

    #[test]
    fn heap_only_checkpoint_corruption_becomes_a_durable_database_control_failure() {
        const SUBMISSION: &str = "generated/76543210fedcba98";

        configure_commit_memory_id(
            HeapCheckpointFailureCanister::COMMIT_MEMORY_ID,
            HeapCheckpointFailureCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        initialize_current_database_control_for_tests(&memory);
        corrupt_live_schema_checkpoint_header_for_tests()
            .expect("checkpoint authority should admit focused corruption");

        let request_root = RequestExecutionRoot::__new_runtime_root();
        let session = crate::db::DbSession::<HeapCheckpointFailureCanister>::new(
            &HEAP_CHECKPOINT_FAILURE_STORES,
            &request_root,
        );
        assert_eq!(
            drive_generated_startup_recovery_page(
                &session,
                &HEAP_CHECKPOINT_FAILURE_STORES,
                SUBMISSION,
            )
            .expect("checkpoint corruption should publish one durable receipt"),
            GeneratedStartupDriverStep::Terminal,
        );

        let failure = observe_generated_startup_state::<HeapCheckpointFailureCanister>(
            &HEAP_CHECKPOINT_FAILURE_STORES,
            SUBMISSION,
        )
        .expect_err("the checkpoint failure should remain visible after the timer returns");
        assert_eq!(failure.kind(), StartupFailureKind::DatabaseControl);
        assert_eq!(
            failure.diagnostic().error_code(),
            ErrorCode::RUNTIME_CORRUPTION
        );
    }

    #[test]
    fn persisted_journal_record_corruption_becomes_a_durable_journal_failure() {
        const SUBMISSION: &str = "generated/2280bad0bad0bad0";

        configure_commit_memory_id(
            JournalRecoveryFailureCanister::COMMIT_MEMORY_ID,
            JournalRecoveryFailureCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        initialize_current_database_control_for_tests(&memory);
        let format_root = RequestExecutionRoot::__new_runtime_root();
        let format_database = crate::db::Db::<JournalRecoveryFailureCanister>::new(
            &JOURNAL_RECOVERY_FAILURE_STORES,
            format_root.scope(),
        );
        ensure_database_format_admitted(&format_database)
            .expect("current journal registry should initialize before corruption injection");

        let record = JournalRecord::schema_put(JOURNAL_RECOVERY_FAILURE_STORE_PATH, vec![0xff; 8])
            .expect("syntactically bounded schema record should build");
        let batch = JournalBatch::new(
            [0x22; 16],
            [0x28; 16],
            JournalSequence::new(1),
            vec![record],
        )
        .expect("syntactically current journal batch should build");
        JOURNAL_RECOVERY_FAILURE_TAIL.with(|tail| {
            tail.borrow_mut()
                .append_batch(&batch)
                .expect("persisted semantic-corruption fixture should insert");
        });

        assert_eq!(
            observe_generated_startup_state::<JournalRecoveryFailureCanister>(
                &JOURNAL_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            ),
            Ok(DatabaseStartupState::Recovering),
        );
        let request_root = RequestExecutionRoot::__new_runtime_root();
        let session = crate::db::DbSession::<JournalRecoveryFailureCanister>::new(
            &JOURNAL_RECOVERY_FAILURE_STORES,
            &request_root,
        );
        assert_eq!(
            drive_generated_startup_recovery_page(
                &session,
                &JOURNAL_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            )
            .expect("journal corruption should publish one durable receipt"),
            GeneratedStartupDriverStep::Terminal,
        );

        let failure = observe_generated_startup_state::<JournalRecoveryFailureCanister>(
            &JOURNAL_RECOVERY_FAILURE_STORES,
            SUBMISSION,
        )
        .expect_err("the durable journal failure should replace blind recovery retries");
        assert_eq!(failure.kind(), StartupFailureKind::JournalRecovery);
        assert_eq!(
            failure.diagnostic().error_code(),
            ErrorCode::STORE_CORRUPTION,
        );
        assert_eq!(
            drive_generated_startup_recovery_page(
                &session,
                &JOURNAL_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            )
            .expect("exact terminal replay should stop without retrying recovery"),
            GeneratedStartupDriverStep::Terminal,
        );

        let changed_record =
            JournalRecord::schema_put(JOURNAL_RECOVERY_FAILURE_STORE_PATH, vec![0xfe; 8])
                .expect("changed semantic-corruption record should build");
        let changed_batch = JournalBatch::new(
            [0x23; 16],
            [0x29; 16],
            JournalSequence::new(2),
            vec![changed_record],
        )
        .expect("changed journal batch should build");
        let changed_encoded =
            encode_journal_batch(&changed_batch).expect("changed journal batch should encode");
        JOURNAL_RECOVERY_FAILURE_TAIL.with(|tail| {
            tail.borrow_mut()
                .insert_raw_batch_for_tests(JournalSequence::new(2), changed_encoded)
                .expect("changed journal authority should insert");
        });
        assert_eq!(
            observe_generated_startup_state::<JournalRecoveryFailureCanister>(
                &JOURNAL_RECOVERY_FAILURE_STORES,
                SUBMISSION,
            ),
            Ok(DatabaseStartupState::Recovering),
            "a receipt bound to the predecessor tail proof must become stale",
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one lifecycle test keeps pending, ready, marker, and receipt precedence in one scenario"
    )]
    fn completed_recovery_stays_recovering_until_exact_generated_schema_receipt_then_is_ready() {
        const SUBMISSION: &str = "generated/0123456789abcdef";

        configure_commit_memory_id(
            CurrentCanister::COMMIT_MEMORY_ID,
            CurrentCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        initialize_current_database_control_for_tests(&memory);
        let incarnation = database_incarnation_id().expect("control should initialize");
        mark_startup_recovery_complete_for_tests(&CURRENT_STORES)
            .expect("recovery witness should publish");

        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(&CURRENT_STORES, SUBMISSION),
            Ok(DatabaseStartupState::Recovering),
        );

        let (database_identity, accepted_head) =
            generated_schema_authority(&CURRENT_STORES, incarnation)
                .expect("schema authority should resolve");
        let submission_key =
            SchemaSubmissionKey::try_new(SUBMISSION).expect("submission should admit");
        let receipt = SchemaChangeReceipt::new(
            database_identity,
            submission_key.clone(),
            SchemaProposalDigest::from_bytes([1; 32]),
            accepted_head.clone(),
            SchemaChangeOutcome::NoOp {
                accepted_head: accepted_head.clone(),
            },
        )
        .expect("terminal schema receipt should admit");
        let record = SchemaApplicationRecord::new(receipt, Vec::new())
            .expect("terminal schema record should admit");
        apply_schema_application_record_op(
            &SchemaApplicationRecordOp::insert(&record)
                .expect("schema record operation should admit"),
        )
        .expect("schema record should publish");
        let before = load_schema_application_record_read_only(database_identity, &submission_key)
            .expect("record should load");

        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(&CURRENT_STORES, SUBMISSION),
            Ok(DatabaseStartupState::Ready),
        );
        assert_eq!(
            load_schema_application_record_read_only(database_identity, &submission_key)
                .expect("record should reload"),
            before,
            "pure readiness observation must not rewrite schema application state",
        );
        assert_eq!(
            receipt::startup_memory::<CurrentCanister>()
                .expect("startup memory should open")
                .size(),
            0,
            "readiness without a failure must not allocate the receipt cell",
        );

        let marker = CommitMarker::from_parts([0x5a; 16], Vec::new())
            .expect("empty marker should admit for control observation");
        let interrupted = begin_commit(marker).expect("marker should persist");
        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(&CURRENT_STORES, SUBMISSION),
            Ok(DatabaseStartupState::Recovering),
            "a marker must take precedence over a completed volatile witness",
        );
        finish_commit(interrupted, |_| Ok(())).expect("empty marker should clear");
        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(&CURRENT_STORES, SUBMISSION),
            Ok(DatabaseStartupState::Ready),
        );

        let accepted_head_binding = match accepted_head {
            icydb_schema::ExpectedAcceptedHead::Empty => receipt::AcceptedHeadBinding::Empty,
            icydb_schema::ExpectedAcceptedHead::Exact {
                revision,
                fingerprint,
            } => receipt::AcceptedHeadBinding::Exact {
                revision,
                fingerprint: fingerprint.to_bytes(),
            },
        };
        let terminal_failure = StartupFailure::new(
            StartupFailureKind::SchemaReconciliation,
            ErrorCode::RUNTIME_CONFLICT.diagnostic(DiagnosticOrigin::Recovery),
            Vec::new(),
        );
        let memoized = receipt::StartupFailureReceipt::new(
            terminal_failure.clone(),
            receipt::StartupFailureBinding::SchemaReconciliation {
                incarnation,
                submission_key: SUBMISSION.to_string(),
                accepted_head: accepted_head_binding,
            },
        )
        .expect("memoized schema failure should admit");
        assert!(
            receipt::publish::<CurrentCanister>(&memoized)
                .expect("memoized failure should publish")
        );
        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(&CURRENT_STORES, SUBMISSION),
            Err(terminal_failure),
            "one exact matching failure receipt has priority over Ready evidence",
        );
        assert_eq!(
            observe_generated_startup_state::<CurrentCanister>(
                &CURRENT_STORES,
                "generated/fedcba9876543210",
            ),
            Ok(DatabaseStartupState::Recovering),
            "a receipt bound to another generated submission must be stale",
        );
        assert!(receipt::clear::<CurrentCanister>().expect("test receipt should clear"));
    }

    #[test]
    fn driver_completes_one_recovery_page_then_memoizes_only_terminal_schema_failure() {
        const SUBMISSION: &str = "generated/0011223344556677";

        configure_commit_memory_id(
            DriverCanister::COMMIT_MEMORY_ID,
            DriverCanister::COMMIT_STABLE_KEY,
        )
        .expect("commit allocation should configure");
        let memory = commit_memory_handle(
            current_commit_memory_allocation().expect("commit allocation should resolve"),
        )
        .expect("commit memory should open");
        initialize_current_database_control_for_tests(&memory);
        let request_root = RequestExecutionRoot::__new_runtime_root();
        let session = crate::db::DbSession::<DriverCanister>::new(&DRIVER_STORES, &request_root);

        assert_eq!(
            drive_generated_startup_recovery_page(&session, &DRIVER_STORES, SUBMISSION)
                .expect("empty recovery page should complete"),
            GeneratedStartupDriverStep::ApplyGeneratedSchema,
        );
        assert_eq!(
            observe_generated_startup_state::<DriverCanister>(&DRIVER_STORES, SUBMISSION),
            Ok(DatabaseStartupState::Recovering),
            "recovery completion alone must not claim generated reconciliation",
        );

        let retryable = InternalError::recovery_pending();
        assert!(
            !record_generated_schema_startup_failure::<DriverCanister>(
                &DRIVER_STORES,
                SUBMISSION,
                retryable.diagnostic(),
                retryable.diagnostic_facts(),
            )
            .expect("retryable classification should complete without publication")
        );
        assert_eq!(
            receipt::startup_memory::<DriverCanister>()
                .expect("startup memory should open")
                .size(),
            0,
            "retryable failure must not allocate the receipt cell",
        );

        let terminal = InternalError::store_corruption();
        let marker = CommitMarker::from_parts([0x7b; 16], Vec::new())
            .expect("empty marker should admit for receipt priority");
        let interrupted = begin_commit(marker).expect("marker should persist");
        assert!(
            record_generated_schema_startup_failure::<DriverCanister>(
                &DRIVER_STORES,
                SUBMISSION,
                terminal.diagnostic(),
                terminal.diagnostic_facts(),
            )
            .expect("terminal failure should publish")
        );
        let observed =
            observe_generated_startup_state::<DriverCanister>(&DRIVER_STORES, SUBMISSION)
                .expect_err("matching terminal receipt should surface");
        assert_eq!(observed.kind(), StartupFailureKind::SchemaReconciliation);
        assert_eq!(
            observed.diagnostic().error_code(),
            ErrorCode::STORE_CORRUPTION
        );
        finish_commit(interrupted, |_| Ok(())).expect("test marker should clear");
        assert!(
            clear_generated_startup_failure::<DriverCanister>()
                .expect("authoritative correction should clear the receipt")
        );
    }
}
