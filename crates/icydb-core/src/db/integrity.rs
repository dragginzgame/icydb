//! Module: db::integrity
//! Responsibility: bounded integrity-inspection result vocabulary and lifecycle identity.
//! Does not own: accepted schema meaning, physical traversal, or inspection progress persistence.
//! Boundary: database control + accepted inspection plan -> typed Quick inspection result.

mod deep;
mod derived;
mod job;
mod progress_store;
mod proof;
mod row;

use crate::{
    db::{
        commit::{database_control_proof_identity, database_incarnation_id, ensure_recovered},
        registry::{
            StoreAllocationIdentities, StoreHandle, StoreRuntimeStorageCapabilities,
            StoreRuntimeStorageMode,
        },
        relation::{RelationConstraintProjection, ReverseRelationSourceInfo},
        schema::AcceptedInspectionPlan,
    },
    entity::EntityKind,
    error::{ErrorClass, InternalError},
    traits::{CanisterKind, Path},
};
use candid::CandidType;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use deep::run_integrity_retention_page_for_tests;
pub(in crate::db) use deep::{
    abort_deep_integrity_job, continue_deep_integrity_job, run_next_integrity_retention_page,
    start_deep_integrity_job,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use deep::{
    reset_integrity_retention_cursor_for_tests, run_next_integrity_retention_page_for_tests,
};
#[cfg(test)]
pub(in crate::db) use derived::DerivedIntegrityPage;
pub(in crate::db) use derived::{
    DerivedInspectionLimits, execute_index_integrity_page, execute_reverse_integrity_page,
};
pub use job::{
    DeepIntegrityPage, DeepIntegrityPageStatus, IntegrityAbortReceipt, IntegrityAbortStatus,
    IntegrityDeepError, IntegrityJobError, IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt,
    IntegrityPendingTerminal, IntegritySubmissionKey, IntegrityTerminalOutcome,
};
pub(in crate::db) use job::{
    IntegrityCheckpoint, IntegrityJob, IntegrityJobState, IntegrityReceiptEnvelope,
    IntegrityReceiptReplayKey, MAX_INTEGRITY_IN_PROGRESS_PAGES,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use progress_store::{
    clear_progress_store_for_tests, corrupt_progress_job_for_tests,
    progress_job_encoded_len_for_tests, set_progress_job_lease_deadline_for_tests,
};
pub(in crate::db) use proof::{IntegrityProofVector, capture_integrity_proof_vector};
#[cfg(test)]
pub(in crate::db) use row::RowIntegrityPage;
pub(in crate::db) use row::{
    PhysicalUnitCheckpoint, RowInspectionLimits, execute_row_integrity_page,
};

pub(in crate::db) const MAX_INTEGRITY_PATH_BYTES: usize = 4 * 1024;

/// One authorization-bound typed integrity operation.
///
/// Entity-bearing variants pin the generated selector identity that the
/// session must match against current accepted authority. Continuation and
/// abort carry only the opaque job identity; private checkpoints never cross
/// this boundary.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityCheckRequest {
    /// Execute one bounded metadata/control inspection.
    Quick {
        /// Accepted entity selector to resolve and verify.
        entity: IntegrityEntityIdentity,
    },
    /// Create or replay one idempotent Deep job.
    DeepStart {
        /// Accepted entity selector to resolve and verify.
        entity: IntegrityEntityIdentity,
        /// Owner-scoped idempotency key.
        submission_key: IntegritySubmissionKey,
    },
    /// Advance or replay one retained Deep job.
    DeepContinue {
        /// Opaque engine-issued job identity.
        job_id: IntegrityJobId,
        /// Sequence of the outstanding receipt being acknowledged.
        acknowledged_sequence: u64,
    },
    /// Freeze one retained Deep job for replayable abort.
    DeepAbort {
        /// Opaque engine-issued job identity.
        job_id: IntegrityJobId,
    },
}

impl IntegrityCheckRequest {
    /// Build a Quick request for one generated entity selector.
    #[must_use]
    pub fn quick<E: EntityKind>() -> Self {
        Self::Quick {
            entity: IntegrityEntityIdentity::for_entity::<E>(),
        }
    }

    /// Build an idempotent Deep-start request for one generated entity selector.
    #[must_use]
    pub fn deep_start<E: EntityKind>(submission_key: IntegritySubmissionKey) -> Self {
        Self::DeepStart {
            entity: IntegrityEntityIdentity::for_entity::<E>(),
            submission_key,
        }
    }

    /// Build one Deep continuation or exact replay request.
    #[must_use]
    pub const fn deep_continue(job_id: IntegrityJobId, acknowledged_sequence: u64) -> Self {
        Self::DeepContinue {
            job_id,
            acknowledged_sequence,
        }
    }

    /// Build one replayable Deep-abort request.
    #[must_use]
    pub const fn deep_abort(job_id: IntegrityJobId) -> Self {
        Self::DeepAbort { job_id }
    }
}

/// Typed result shared by trusted Rust and SQL integrity frontends.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityCheckResult {
    /// Bounded one-call Quick result.
    Quick(QuickIntegrityResult),
    /// Start, continuation, terminal, or abort Deep receipt.
    Deep(IntegrityJobReceipt),
}

fn accepted_relation_projections<C: CanisterKind>(
    db: &crate::db::Db<C>,
    plan: &AcceptedInspectionPlan,
) -> Result<Vec<RelationConstraintProjection>, InternalError> {
    let identity = plan.identity();
    let source = ReverseRelationSourceInfo::new(identity.entity_path(), identity.entity_tag());

    plan.snapshot()
        .persisted_snapshot()
        .relations()
        .iter()
        .map(|edge| {
            RelationConstraintProjection::new_active(
                db,
                source,
                plan.snapshot().persisted_snapshot(),
                plan.row_contract(),
                edge,
            )
        })
        .collect()
}

fn validate_quick_integrity_control<C: CanisterKind>(
    db: &crate::db::Db<C>,
    plan: &AcceptedInspectionPlan,
) -> Result<Vec<IntegrityFinding>, InternalError> {
    let identity = plan.identity();
    let source_store = db.store_handle(identity.store_path())?;
    let relations = accepted_relation_projections(db, plan)?;
    let mut participating_stores =
        BTreeMap::from([(identity.store_path().to_string(), source_store)]);
    for relation in &relations {
        participating_stores
            .entry(relation.target_store_path().to_string())
            .or_insert_with(|| relation.target_store());
    }

    let _database_control = database_control_proof_identity()?;
    proof::validate_integrity_allocation_registry()?;
    let mut findings = Vec::new();
    for (store_path, store) in &participating_stores {
        if let Some(finding) = validate_quick_store_control(plan, store_path, *store)? {
            findings.push(finding);
        }
    }
    for ordinal in 0..plan.index_inspection().len() {
        let _domain = plan
            .index_inspection()
            .domain(ordinal, identity.entity_tag())?;
    }

    Ok(findings)
}

fn validate_quick_store_control(
    plan: &AcceptedInspectionPlan,
    store_path: &str,
    store: StoreHandle,
) -> Result<Option<IntegrityFinding>, InternalError> {
    let capabilities = store.storage_capabilities();
    let allocations = store.allocation_identities();
    match capabilities.storage_mode() {
        StoreRuntimeStorageMode::Heap => {
            if capabilities != StoreRuntimeStorageCapabilities::heap()
                || allocations != StoreAllocationIdentities::absent()
                || store.journal_tail_store().is_some()
            {
                return Err(InternalError::store_invariant());
            }
            Ok(None)
        }
        StoreRuntimeStorageMode::Journaled => {
            if capabilities != StoreRuntimeStorageCapabilities::journaled()
                || !allocations.matches_storage_capabilities(capabilities)
            {
                return Err(InternalError::store_invariant());
            }
            let journal = store
                .journal_tail_store()
                .ok_or_else(InternalError::store_invariant)?
                .with_borrow(crate::db::journal::JournalTailStore::proof_identity)?;
            if !journal.is_well_formed() {
                return Ok(Some(quick_journal_control_finding(plan, store_path)));
            }
            Ok(None)
        }
    }
}

fn quick_journal_control_finding(
    plan: &AcceptedInspectionPlan,
    store_path: &str,
) -> IntegrityFinding {
    let error = InternalError::store_corruption();
    IntegrityFinding {
        diagnostic_code: error.diagnostic_code().error_code().raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind: IntegrityFindingKind::JournalControlMismatch,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: store_path.to_string(),
        phase: IntegrityPhase::QuickMetadata,
        verifier_family: IntegrityVerifierFamily::JournalEnvelope,
        physical_key: Vec::new(),
        primary_key: None,
        field_paths: Vec::new(),
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: Some("well-formed-journal-control".to_string()),
        observed: Some("inconsistent-journal-control".to_string()),
    }
}

fn relation_field_paths(plan: &AcceptedInspectionPlan, relation_id: u32) -> Vec<String> {
    let snapshot = plan.snapshot().persisted_snapshot();
    let Some(relation) = snapshot
        .relations()
        .iter()
        .find(|relation| relation.id().get() == relation_id)
    else {
        return Vec::new();
    };

    relation
        .local_field_ids()
        .iter()
        .filter_map(|field_id| {
            snapshot
                .fields()
                .iter()
                .find(|field| field.id() == *field_id)
                .map(|field| field.name().to_string())
        })
        .collect()
}

const MAX_QUICK_RETURNED_FINDINGS: usize = 64;
#[cfg(target_arch = "wasm32")]
const DATABASE_INCARNATION_DOMAIN: &[u8] = b"icydb.database-incarnation.v1";
static DATABASE_INCARNATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Durable identity of one database lifecycle.
///
/// The identity is independent of accepted schema, row, index, relation, and
/// journal revisions. Ordinary reopen preserves it. Any future restore,
/// replacement, or import lane that can reuse those revisions must mint and
/// publish a fresh identity before the restored database becomes available.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct DatabaseIncarnationId([u8; 16]);

impl DatabaseIncarnationId {
    /// Decode one current-form nonzero incarnation identity.
    pub(crate) fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, InternalError> {
        if bytes == [0; 16] {
            return Err(InternalError::database_incarnation_invalid());
        }

        Ok(Self(bytes))
    }

    /// Return the canonical persisted identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    fn generate() -> Result<Self, InternalError> {
        let sequence = DATABASE_INCARNATION_SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| InternalError::database_incarnation_generation_failed())?
            .checked_add(1)
            .ok_or_else(InternalError::database_incarnation_generation_failed)?;

        #[cfg(not(target_arch = "wasm32"))]
        let bytes = {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes)
                .map_err(|_| InternalError::database_incarnation_generation_failed())?;
            bytes
        };

        #[cfg(target_arch = "wasm32")]
        let bytes = {
            use sha2::{Digest, Sha256};

            let mut hasher = Sha256::new();
            hasher.update(DATABASE_INCARNATION_DOMAIN);
            hasher.update(ic_cdk::api::canister_self().as_slice());
            hasher.update(ic_cdk::api::time().to_be_bytes());
            hasher.update(sequence.to_be_bytes());
            let digest = hasher.finalize();
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&digest[..16]);
            bytes
        };

        let _ = sequence;
        Self::try_from_bytes(bytes)
    }

    #[cfg(test)]
    pub(crate) const fn for_tests(fill: u8) -> Self {
        let mut bytes = [fill; 16];
        if fill == 0 {
            bytes[15] = 1;
        }
        Self(bytes)
    }
}

/// Stable entity identity projected into integrity responses.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct IntegrityEntityIdentity {
    entity_tag: u64,
    entity_path: String,
    store_path: String,
}

impl IntegrityEntityIdentity {
    fn from_plan(plan: &AcceptedInspectionPlan) -> Self {
        Self::from_accepted_identity(plan.identity())
    }

    pub(in crate::db) fn from_accepted_identity(
        identity: crate::db::schema::AcceptedCatalogIdentity,
    ) -> Self {
        Self {
            entity_tag: identity.entity_tag().value(),
            entity_path: identity.entity_path().to_string(),
            store_path: identity.store_path().to_string(),
        }
    }

    /// Build one non-authoritative selector from registered runtime routing.
    ///
    /// Integrity execution must still resolve accepted authority and require
    /// an exact match before inspecting the selected entity.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_runtime_selector(
        entity_tag: u64,
        entity_path: &str,
        store_path: &str,
    ) -> Self {
        Self {
            entity_tag,
            entity_path: entity_path.to_string(),
            store_path: store_path.to_string(),
        }
    }

    /// Build a selector for one generated IcyDB entity.
    ///
    /// The selector is not runtime authority. Integrity execution resolves the
    /// current accepted entity and requires all three identity components to
    /// match before inspection.
    #[must_use]
    pub fn for_entity<E: EntityKind>() -> Self {
        Self {
            entity_tag: E::ENTITY_TAG.value(),
            entity_path: <E as Path>::PATH.to_string(),
            store_path: <E::Store as Path>::PATH.to_string(),
        }
    }

    pub(in crate::db) const fn validate(&self) -> Result<(), IntegrityJobError> {
        if self.entity_tag == 0
            || self.entity_path.is_empty()
            || self.entity_path.len() > MAX_INTEGRITY_PATH_BYTES
            || self.store_path.is_empty()
            || self.store_path.len() > MAX_INTEGRITY_PATH_BYTES
        {
            return Err(IntegrityJobError::InvalidEntityIdentity);
        }
        Ok(())
    }

    /// Return the stable accepted entity tag.
    #[must_use]
    pub const fn entity_tag(&self) -> u64 {
        self.entity_tag
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the accepted store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }
}

/// Broad machine-readable accepted-authority failure class.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityAuthorityClass {
    /// Accepted authority bytes or closure are corrupt.
    Corruption,
    /// Accepted authority uses an unsupported persisted form.
    IncompatiblePersistedFormat,
    /// Accepted authority violates an internal invariant.
    InvariantViolation,
    /// The selected entity or storage contract is unsupported.
    Unsupported,
    /// The engine could not complete accepted-authority inspection.
    Internal,
}

/// Broad machine-readable integrity finding class.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityFindingClass {
    /// Accepted or physical bytes are corrupt.
    Corruption,
    /// Current-form persisted bytes cannot be decoded by this build.
    IncompatiblePersistedFormat,
    /// A required bounded proof could not be completed.
    ResourceLimited,
}

/// Stable semantic family of one integrity finding.

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityFindingKind {
    /// The physical data key is not a valid current key for its entity interval.
    MalformedDataKey,

    /// The maintained row envelope or slot table is malformed.
    MalformedRow,

    /// The row exceeds the maintained current raw-byte bound.
    OversizedRow,

    /// One active accepted field payload violates its exact field contract.
    InvalidFieldValue,

    /// The physical key and decoded primary-key field values disagree.
    PrimaryKeyMismatch,

    /// One validated accepted check constraint evaluates to false.
    ConstraintViolation,

    /// One row-derived active forward-index witness is absent.
    MissingIndexEntry,

    /// One row-derived active forward-index witness has invalid value bytes.
    DivergentIndexEntry,

    /// One active forward-index entry has malformed key, identity, or value framing.
    MalformedIndexEntry,

    /// One active forward-index entry points at no authoritative source row.
    OrphanIndexEntry,

    /// One unique logical key has more than one physical row witness.
    DuplicateUniqueIndexKey,

    /// One accepted relation points to an absent target row.
    MissingRelationTarget,

    /// One expected active reverse-relation witness is absent.
    MissingReverseRelationEntry,

    /// One expected active reverse-relation witness has invalid value bytes.
    DivergentReverseRelationEntry,

    /// One active reverse-relation entry has malformed key, identity, or value framing.
    MalformedReverseRelationEntry,

    /// One active reverse-relation entry points at no authoritative source row.
    OrphanReverseRelationEntry,

    /// One durable journal batch is not a valid current-form envelope.
    MalformedJournalBatch,

    /// The durable journal tail omits one or more expected sequence values.
    JournalSequenceGap,

    /// Two durable journal batches carry the same logical batch identity.
    DuplicateJournalBatchIdentity,

    /// Bounded journal control records disagree without requiring tail traversal.
    JournalControlMismatch,
}

/// Canonical Deep inspection phase.

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityPhase {
    /// Bounded accepted metadata and control closure.
    QuickMetadata,

    /// Canonical physical row storage.
    Rows,

    /// Active forward-index storage.
    IndexEntries,

    /// Active source-owned reverse-relation storage.
    ReverseRelations,

    /// Durable journal tails.
    JournalTails,

    /// Final unchanged-proof-vector comparison.
    FinalProofVectorCheck,
}

/// Deterministic verifier family within one physical inspection unit.

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IntegrityVerifierFamily {
    /// Current physical data-key framing and identity.
    DataKey,

    /// Current row envelope, layout stamp, slot count, and table framing.
    RowEnvelope,

    /// One accepted field payload or frozen historical fill.
    FieldValue,

    /// Physical key versus accepted row primary-key fields.
    PrimaryKey,

    /// Validated accepted row-local checks.
    ValidatedConstraints,

    /// One expected active forward-index witness.
    ForwardIndex,

    /// One physical active forward-index entry.
    IndexEntry,

    /// One unique-key multiplicity proof.
    UniqueIndex,

    /// One accepted relation's target and reverse witness projection.
    Relation,

    /// One physical active source-owned reverse-relation entry.
    ReverseRelationEntry,

    /// Current durable journal batch framing and sequence continuity.
    JournalEnvelope,

    /// Durable journal batch identity uniqueness.
    JournalBatchIdentity,
}

/// Severity of one definite integrity finding.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegritySeverity {
    /// The finding identifies invalid maintained state.
    Error,
    /// The finding is an operator advisory and does not invalidate a clean proof.
    Advisory,
}

/// One bounded machine-readable integrity finding.
///
/// Raw row payloads and unbounded application values are deliberately absent.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct IntegrityFinding {
    diagnostic_code: u16,
    class: IntegrityFindingClass,
    severity: IntegritySeverity,
    kind: IntegrityFindingKind,
    entity: IntegrityEntityIdentity,
    store_path: String,
    phase: IntegrityPhase,
    verifier_family: IntegrityVerifierFamily,
    physical_key: Vec<u8>,
    primary_key: Option<Vec<u8>>,
    field_paths: Vec<String>,
    constraint_id: Option<u32>,
    constraint_name: Option<String>,
    schema_index_id: Option<u32>,
    relation_id: Option<u32>,
    expected: Option<String>,
    observed: Option<String>,
}

impl IntegrityFinding {
    /// Return the stable compact diagnostic code.
    #[must_use]
    pub const fn diagnostic_code(&self) -> u16 {
        self.diagnostic_code
    }

    /// Return the broad finding class.
    #[must_use]
    pub const fn class(&self) -> IntegrityFindingClass {
        self.class
    }

    /// Return the finding severity.
    #[must_use]
    pub const fn severity(&self) -> IntegritySeverity {
        self.severity
    }

    /// Return the stable semantic finding family.
    #[must_use]
    pub const fn kind(&self) -> IntegrityFindingKind {
        self.kind
    }

    /// Borrow the accepted entity identity.
    #[must_use]
    pub const fn entity(&self) -> &IntegrityEntityIdentity {
        &self.entity
    }

    /// Borrow the affected store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Return the Deep phase that observed this finding.
    #[must_use]
    pub const fn phase(&self) -> IntegrityPhase {
        self.phase
    }

    /// Return the deterministic verifier family that observed this finding.
    #[must_use]
    pub const fn verifier_family(&self) -> IntegrityVerifierFamily {
        self.verifier_family
    }

    /// Borrow the bounded exact physical key.
    #[must_use]
    pub const fn physical_key(&self) -> &[u8] {
        self.physical_key.as_slice()
    }

    /// Borrow the canonical primary-key suffix after successful key decoding.
    #[must_use]
    pub fn primary_key(&self) -> Option<&[u8]> {
        self.primary_key.as_deref()
    }

    /// Borrow bounded accepted field paths relevant to the finding.
    #[must_use]
    pub const fn field_paths(&self) -> &[String] {
        self.field_paths.as_slice()
    }

    /// Return the accepted constraint identity when applicable.
    #[must_use]
    pub const fn constraint_id(&self) -> Option<u32> {
        self.constraint_id
    }

    /// Borrow the accepted constraint name when applicable.
    #[must_use]
    pub fn constraint_name(&self) -> Option<&str> {
        self.constraint_name.as_deref()
    }

    /// Return the accepted logical index identity when applicable.
    #[must_use]
    pub const fn schema_index_id(&self) -> Option<u32> {
        self.schema_index_id
    }

    /// Return the accepted relation identity when applicable.
    #[must_use]
    pub const fn relation_id(&self) -> Option<u32> {
        self.relation_id
    }

    /// Borrow the bounded expected-state label, when applicable.
    #[must_use]
    pub fn expected(&self) -> Option<&str> {
        self.expected.as_deref()
    }

    /// Borrow the bounded observed-state label, when applicable.
    #[must_use]
    pub fn observed(&self) -> Option<&str> {
        self.observed.as_deref()
    }
}

/// Typed reason that accepted authority could not be inspected.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct IntegrityAuthorityDiagnostic {
    diagnostic_code: u16,
    class: IntegrityAuthorityClass,
}

impl IntegrityAuthorityDiagnostic {
    fn from_internal(error: &InternalError) -> Self {
        let class = match error.class {
            ErrorClass::Corruption => IntegrityAuthorityClass::Corruption,
            ErrorClass::IncompatiblePersistedFormat => {
                IntegrityAuthorityClass::IncompatiblePersistedFormat
            }
            ErrorClass::InvariantViolation => IntegrityAuthorityClass::InvariantViolation,
            ErrorClass::Unsupported | ErrorClass::NotFound | ErrorClass::Conflict => {
                IntegrityAuthorityClass::Unsupported
            }
            ErrorClass::Internal => IntegrityAuthorityClass::Internal,
        };
        Self {
            diagnostic_code: error.diagnostic_code().error_code().raw(),
            class,
        }
    }

    /// Return the stable compact diagnostic code.
    #[must_use]
    pub const fn diagnostic_code(&self) -> u16 {
        self.diagnostic_code
    }

    /// Return the broad failure class.
    #[must_use]
    pub const fn class(&self) -> IntegrityAuthorityClass {
        self.class
    }
}

/// Typed bounded-resource failure.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct IntegrityResourceDiagnostic {
    diagnostic_code: u16,
}

impl IntegrityResourceDiagnostic {
    /// Return the stable compact diagnostic code.
    #[must_use]
    pub const fn diagnostic_code(&self) -> u16 {
        self.diagnostic_code
    }
}

/// Physical container whose traversal could not prove progress or exhaustion.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityPhysicalContainer {
    /// Canonical row storage.
    Rows,
    /// Active forward-index storage.
    IndexEntries,
    /// Active reverse-relation storage.
    ReverseRelations,
    /// Journal-tail storage.
    JournalTails,
}

/// Typed load-bearing physical traversal failure.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StorageTraversalCorruption {
    diagnostic_code: u16,
    store_path: String,
    container: IntegrityPhysicalContainer,
    phase: IntegrityPhase,
    last_verified_physical_key: Option<Vec<u8>>,
}

impl StorageTraversalCorruption {
    /// Return the stable compact diagnostic code.
    #[must_use]
    pub const fn diagnostic_code(&self) -> u16 {
        self.diagnostic_code
    }

    /// Borrow the affected store path.
    #[must_use]
    pub const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Return the affected physical container.
    #[must_use]
    pub const fn container(&self) -> IntegrityPhysicalContainer {
        self.container
    }

    /// Return the phase whose physical traversal failed.
    #[must_use]
    pub const fn phase(&self) -> IntegrityPhase {
        self.phase
    }

    /// Borrow the last physical key whose classification completed.
    #[must_use]
    pub fn last_verified_physical_key(&self) -> Option<&[u8]> {
        self.last_verified_physical_key.as_deref()
    }
}

/// Outcome of one bounded Quick integrity inspection.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum QuickIntegrityStatus {
    /// Every bounded Quick family was inspected without findings.
    CompleteClean,
    /// Every bounded Quick family was inspected and definite findings exist.
    CompleteWithFindings,
    /// Load-bearing accepted authority could not be inspected.
    Uninspectable(IntegrityAuthorityDiagnostic),
    /// A physical container could not prove progress or exhaustion.
    UninspectableStorage(StorageTraversalCorruption),
    /// The minimum bounded inspection atom could not be completed.
    ResourceLimited(IntegrityResourceDiagnostic),
}

/// Complete result of one bounded accepted-native Quick inspection.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct QuickIntegrityResult {
    entity: IntegrityEntityIdentity,
    database_incarnation_id: DatabaseIncarnationId,
    accepted_schema_version: u32,
    accepted_schema_fingerprint: [u8; 16],
    status: QuickIntegrityStatus,
    total_findings: u64,
    omitted_findings: u64,
    findings: Vec<IntegrityFinding>,
}

impl QuickIntegrityResult {
    /// Borrow the accepted entity identity.
    #[must_use]
    pub const fn entity(&self) -> &IntegrityEntityIdentity {
        &self.entity
    }

    /// Return the durable database incarnation inspected by this call.
    #[must_use]
    pub const fn database_incarnation_id(&self) -> DatabaseIncarnationId {
        self.database_incarnation_id
    }

    /// Return the accepted entity schema version.
    #[must_use]
    pub const fn accepted_schema_version(&self) -> u32 {
        self.accepted_schema_version
    }

    /// Return the accepted entity schema fingerprint.
    #[must_use]
    pub const fn accepted_schema_fingerprint(&self) -> [u8; 16] {
        self.accepted_schema_fingerprint
    }

    /// Borrow the Quick completion status.
    #[must_use]
    pub const fn status(&self) -> &QuickIntegrityStatus {
        &self.status
    }

    /// Return the exact number of findings observed.
    #[must_use]
    pub const fn total_findings(&self) -> u64 {
        self.total_findings
    }

    /// Return the number of findings omitted from the bounded response prefix.
    #[must_use]
    pub const fn omitted_findings(&self) -> u64 {
        self.omitted_findings
    }

    /// Borrow the bounded canonical finding prefix.
    #[must_use]
    pub const fn findings(&self) -> &[IntegrityFinding] {
        self.findings.as_slice()
    }
}

struct QuickIntegrityAccumulator {
    total_findings: u64,
    findings: Vec<IntegrityFinding>,
}

impl QuickIntegrityAccumulator {
    const fn new() -> Self {
        Self {
            total_findings: 0,
            findings: Vec::new(),
        }
    }

    fn record(&mut self, finding: IntegrityFinding) -> Result<(), IntegrityResourceDiagnostic> {
        self.total_findings =
            self.total_findings
                .checked_add(1)
                .ok_or(IntegrityResourceDiagnostic {
                    diagnostic_code: icydb_diagnostic_code::ErrorCode::RUNTIME_INTERNAL.raw(),
                })?;
        if self.findings.len() < MAX_QUICK_RETURNED_FINDINGS {
            self.findings.push(finding);
        }
        Ok(())
    }

    fn complete(
        self,
        plan: &AcceptedInspectionPlan,
        incarnation: DatabaseIncarnationId,
    ) -> QuickIntegrityResult {
        let status = if self.total_findings == 0 {
            QuickIntegrityStatus::CompleteClean
        } else {
            QuickIntegrityStatus::CompleteWithFindings
        };
        let omitted_findings = self
            .total_findings
            .saturating_sub(self.findings.len() as u64);
        let identity = plan.identity();

        QuickIntegrityResult {
            entity: IntegrityEntityIdentity::from_plan(plan),
            database_incarnation_id: incarnation,
            accepted_schema_version: identity.accepted_schema_version().get(),
            accepted_schema_fingerprint: identity.accepted_schema_fingerprint(),
            status,
            total_findings: self.total_findings,
            omitted_findings,
            findings: self.findings,
        }
    }

    fn resource_limited(
        self,
        plan: &AcceptedInspectionPlan,
        incarnation: DatabaseIncarnationId,
        diagnostic: IntegrityResourceDiagnostic,
    ) -> QuickIntegrityResult {
        let omitted_findings = self
            .total_findings
            .saturating_sub(self.findings.len() as u64);
        let identity = plan.identity();

        QuickIntegrityResult {
            entity: IntegrityEntityIdentity::from_plan(plan),
            database_incarnation_id: incarnation,
            accepted_schema_version: identity.accepted_schema_version().get(),
            accepted_schema_fingerprint: identity.accepted_schema_fingerprint(),
            status: QuickIntegrityStatus::ResourceLimited(diagnostic),
            total_findings: self.total_findings,
            omitted_findings,
            findings: self.findings,
        }
    }
}

pub(in crate::db) fn uninspectable_quick_integrity(
    identity: crate::db::schema::AcceptedCatalogIdentity,
    incarnation: DatabaseIncarnationId,
    error: &InternalError,
) -> QuickIntegrityResult {
    QuickIntegrityResult {
        entity: IntegrityEntityIdentity::from_accepted_identity(identity),
        database_incarnation_id: incarnation,
        accepted_schema_version: identity.accepted_schema_version().get(),
        accepted_schema_fingerprint: identity.accepted_schema_fingerprint(),
        status: QuickIntegrityStatus::Uninspectable(IntegrityAuthorityDiagnostic::from_internal(
            error,
        )),
        total_findings: 0,
        omitted_findings: 0,
        findings: Vec::new(),
    }
}

pub(in crate::db) fn execute_quick_integrity<C: CanisterKind>(
    db: &crate::db::Db<C>,
    plan: &AcceptedInspectionPlan,
) -> Result<QuickIntegrityResult, InternalError> {
    ensure_recovered(db)?;
    let incarnation = database_incarnation_id()?;
    let findings = match validate_quick_integrity_control(db, plan) {
        Ok(findings) => findings,
        Err(error) => {
            return Ok(uninspectable_quick_integrity(
                plan.identity(),
                incarnation,
                &error,
            ));
        }
    };
    let mut accumulator = QuickIntegrityAccumulator::new();
    for finding in findings {
        if let Err(diagnostic) = accumulator.record(finding) {
            return Ok(accumulator.resource_limited(plan, incarnation, diagnostic));
        }
    }

    Ok(accumulator.complete(plan, incarnation))
}

pub(crate) fn generate_database_incarnation_id() -> Result<DatabaseIncarnationId, InternalError> {
    DatabaseIncarnationId::generate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            commit::CommitSchemaFingerprint,
            schema::{
                AcceptedCatalogIdentity, AcceptedCompositeCatalog, AcceptedFieldKind,
                AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
                FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
                SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
                enum_catalog::build_initial_accepted_enum_catalog,
            },
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
    };

    fn plan() -> AcceptedInspectionPlan {
        let revision = AcceptedSchemaRevision::INITIAL;
        let identity = AcceptedCatalogIdentity::new(
            EntityTag::new(23),
            "tests::QuickEntity",
            "tests::QuickStore",
            revision,
            SchemaVersion::initial(),
            CommitSchemaFingerprint::from([0x44; 16]),
        );
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::QuickEntity".to_string(),
            "QuickEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
        ));
        let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
            build_initial_accepted_enum_catalog(&[])
                .expect("empty accepted enum catalog should build"),
            AcceptedCompositeCatalog::empty(),
            revision,
        );

        AcceptedInspectionPlan::compile(identity, snapshot, value_catalog)
            .expect("accepted Quick plan should compile")
    }

    fn finding(plan: &AcceptedInspectionPlan) -> IntegrityFinding {
        IntegrityFinding {
            diagnostic_code: icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
            class: IntegrityFindingClass::Corruption,
            severity: IntegritySeverity::Error,
            kind: IntegrityFindingKind::MalformedRow,
            entity: IntegrityEntityIdentity::from_plan(plan),
            store_path: plan.identity().store_path().to_string(),
            phase: IntegrityPhase::Rows,
            verifier_family: IntegrityVerifierFamily::RowEnvelope,
            physical_key: vec![1],
            primary_key: None,
            field_paths: Vec::new(),
            constraint_id: None,
            constraint_name: None,
            schema_index_id: None,
            relation_id: None,
            expected: None,
            observed: None,
        }
    }

    #[test]
    fn database_incarnation_rejects_zero_and_round_trips_current_bytes() {
        assert!(DatabaseIncarnationId::try_from_bytes([0; 16]).is_err());

        let identity = DatabaseIncarnationId::for_tests(7);
        assert_eq!(
            DatabaseIncarnationId::try_from_bytes(identity.to_bytes())
                .expect("nonzero incarnation should decode"),
            identity,
        );
    }

    #[test]
    fn quick_clean_result_binds_incarnation_and_accepted_plan_identity() {
        let plan = plan();
        let incarnation = DatabaseIncarnationId::for_tests(8);
        let result = QuickIntegrityAccumulator::new().complete(&plan, incarnation);

        assert_eq!(result.status(), &QuickIntegrityStatus::CompleteClean);
        assert_eq!(result.database_incarnation_id(), incarnation);
        assert_eq!(result.accepted_schema_version(), 1);
        assert_eq!(result.accepted_schema_fingerprint(), [0x44; 16]);
        assert_eq!(result.total_findings(), 0);
        assert_eq!(result.omitted_findings(), 0);
    }

    #[test]
    fn quick_findings_keep_a_bounded_prefix_and_exact_omitted_count() {
        let plan = plan();
        let mut accumulator = QuickIntegrityAccumulator::new();
        for _ in 0..=MAX_QUICK_RETURNED_FINDINGS {
            accumulator
                .record(finding(&plan))
                .expect("bounded test finding count should fit");
        }
        let result = accumulator.complete(&plan, DatabaseIncarnationId::for_tests(9));

        assert_eq!(result.status(), &QuickIntegrityStatus::CompleteWithFindings,);
        assert_eq!(result.total_findings(), 65);
        assert_eq!(result.findings().len(), MAX_QUICK_RETURNED_FINDINGS);
        assert_eq!(result.omitted_findings(), 1);
        assert_eq!(
            result.total_findings(),
            result.findings().len() as u64 + result.omitted_findings(),
        );
    }

    #[test]
    fn quick_selected_authority_failure_is_not_a_clean_completion() {
        let plan = plan();
        let error = InternalError::accepted_row_constraint_program_corrupt();
        let result = uninspectable_quick_integrity(
            plan.identity(),
            DatabaseIncarnationId::for_tests(10),
            &error,
        );

        assert!(matches!(
            result.status(),
            QuickIntegrityStatus::Uninspectable(IntegrityAuthorityDiagnostic {
                class: IntegrityAuthorityClass::Corruption,
                ..
            }),
        ));
        assert_eq!(result.total_findings(), 0);
        assert_eq!(result.omitted_findings(), 0);
    }
}
