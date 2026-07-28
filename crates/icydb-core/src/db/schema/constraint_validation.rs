//! Module: db::schema::constraint_validation
//! Responsibility: bounded durable progress for accepted constraint activation.
//! Does not own: activation semantics, row mutation, or final structural publication.
//! Boundary: binds one schema-owned Forward/Verify job to an exact activation.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        schema::{
            AcceptedSchemaFingerprint, AcceptedTargetPath, AcceptedTargetPathComponent,
            ConstraintActivationFingerprint, ConstraintActivationKind,
            ConstraintActivationSnapshot, ConstraintActivationState, ConstraintId, FieldId,
            MAX_ACCEPTED_TARGET_PATH_COMPONENTS, PersistedSchemaSnapshot,
            composite_catalog::{CompositeFieldId, CompositeTypeId},
            enum_catalog::{EnumTypeId, EnumVariantId},
        },
    },
    error::InternalError,
    types::EntityTag,
};
use candid::{CandidType, Decode, Encode};
use ic_stable_structures::Storable;
use serde::Deserialize;
use std::borrow::Cow;

const CONSTRAINT_VALIDATION_JOB_CODEC_VERSION: u32 = 1;
const CONSTRAINT_VALIDATION_JOB_PROFILE: u32 = u32::from_be_bytes(*b"ICJB");
pub(in crate::db) const MAX_CONSTRAINT_VALIDATION_JOB_BYTES: usize = 64 * 1024;
const MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES: usize = 4 * 1024;
const MAX_CONSTRAINT_VALIDATION_STORE_REVISIONS: usize = 16;
const MAX_CONSTRAINT_VALIDATION_FINDINGS_PER_RECEIPT: usize = 64;
const MAX_CONSTRAINT_VALIDATION_FINDING_FIELDS: usize = 32;

/// Project durable accepted field identities into bounded diagnostic paths.
pub(in crate::db) fn accepted_constraint_field_paths(
    snapshot: &PersistedSchemaSnapshot,
    field_ids: &[FieldId],
) -> Result<Vec<String>, InternalError> {
    field_ids
        .iter()
        .map(|field_id| {
            snapshot
                .fields()
                .iter()
                .find(|field| field.id() == *field_id)
                .map(|field| field.name().to_string())
                .ok_or_else(InternalError::store_corruption)
        })
        .collect()
}

/// Current bounded proof phase for one activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ConstraintValidationPhase {
    /// Scan historical rows and converge isolated candidate state.
    Forward,
    /// Read-only full proof at one captured participating revision vector.
    Verify,
}

impl ConstraintValidationPhase {
    /// Borrow the stable introspection label for this proof phase.
    #[must_use]
    pub(in crate::db) const fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Verify => "verify",
        }
    }
}

/// One participating store revision captured for stable verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintStoreRevision {
    store_path: String,
    revision: u64,
}

impl ConstraintStoreRevision {
    /// Build one participating revision from current store authority.
    #[must_use]
    pub(in crate::db) const fn new(store_path: String, revision: u64) -> Self {
        Self {
            store_path,
            revision,
        }
    }

    /// Borrow the participating store path.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Return the captured durable mutation revision.
    #[must_use]
    pub(in crate::db) const fn revision(&self) -> u64 {
        self.revision
    }
}

/// One bounded historical validation finding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintValidationFinding {
    primary_key: RawDataStoreKey,
    field_ids: Vec<FieldId>,
    value_path: Option<AcceptedTargetPath>,
    error_code: u16,
}

impl ConstraintValidationFinding {
    /// Build one finding after a row has been fully classified.
    #[must_use]
    pub(in crate::db) const fn new(
        primary_key: RawDataStoreKey,
        field_ids: Vec<FieldId>,
        error_code: u16,
    ) -> Self {
        Self {
            primary_key,
            field_ids,
            value_path: None,
            error_code,
        }
    }

    /// Build one targeted-rule finding with its bounded concrete occurrence.
    #[must_use]
    pub(in crate::db) const fn new_targeted(
        primary_key: RawDataStoreKey,
        field_ids: Vec<FieldId>,
        value_path: AcceptedTargetPath,
        error_code: u16,
    ) -> Self {
        Self {
            primary_key,
            field_ids,
            value_path: Some(value_path),
            error_code,
        }
    }

    /// Borrow the canonical persisted primary key.
    #[must_use]
    pub(in crate::db) const fn primary_key(&self) -> &RawDataStoreKey {
        &self.primary_key
    }

    /// Borrow sorted unique implicated field identities.
    #[must_use]
    pub(in crate::db) const fn field_ids(&self) -> &[FieldId] {
        self.field_ids.as_slice()
    }

    /// Borrow the targeted concrete value path, when this is a targeted rule.
    #[must_use]
    pub(in crate::db) const fn value_path(&self) -> Option<&AcceptedTargetPath> {
        self.value_path.as_ref()
    }

    /// Return the stable public error-code identity.
    #[must_use]
    pub(in crate::db) const fn error_code(&self) -> u16 {
        self.error_code
    }
}

/// Last bounded finding page retained until explicitly acknowledged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintValidationReceipt {
    page_sequence: u64,
    findings: Vec<ConstraintValidationFinding>,
}

impl ConstraintValidationReceipt {
    /// Build one non-empty, monotonically sequenced finding receipt.
    #[must_use]
    pub(in crate::db) const fn new(
        page_sequence: u64,
        findings: Vec<ConstraintValidationFinding>,
    ) -> Self {
        Self {
            page_sequence,
            findings,
        }
    }

    /// Return the acknowledgement sequence for this exact page.
    #[must_use]
    pub(in crate::db) const fn page_sequence(&self) -> u64 {
        self.page_sequence
    }

    /// Borrow the bounded findings retained by this page.
    #[must_use]
    pub(in crate::db) const fn findings(&self) -> &[ConstraintValidationFinding] {
        self.findings.as_slice()
    }
}

/// Durable schema-owned validation progress for one exact activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintValidationJob {
    entity_tag: EntityTag,
    entity_path: String,
    constraint_id: ConstraintId,
    activation_epoch: u64,
    activation_fingerprint: ConstraintActivationFingerprint,
    base_schema_fingerprint: AcceptedSchemaFingerprint,
    phase: ConstraintValidationPhase,
    checkpoint: Option<RawDataStoreKey>,
    captured_store_revisions: Option<Vec<ConstraintStoreRevision>>,
    staged_generation: Option<u64>,
    rows_scanned: u64,
    findings_seen: u64,
    restarts: u64,
    forward_findings: u64,
    receipt_sequence: u64,
    last_receipt: Option<ConstraintValidationReceipt>,
}

impl ConstraintValidationJob {
    /// Start one Forward proof bound to a validating activation.
    pub(in crate::db) fn start(
        entity_tag: EntityTag,
        entity_path: String,
        activation: &ConstraintActivationSnapshot,
        staged_generation: Option<u64>,
    ) -> Result<Self, InternalError> {
        let job = Self {
            entity_tag,
            entity_path,
            constraint_id: activation.id(),
            activation_epoch: activation.activation_epoch(),
            activation_fingerprint: activation.fingerprint(),
            base_schema_fingerprint: activation.base_schema_fingerprint(),
            phase: ConstraintValidationPhase::Forward,
            checkpoint: None,
            captured_store_revisions: None,
            staged_generation,
            rows_scanned: 0,
            findings_seen: 0,
            restarts: 0,
            forward_findings: 0,
            receipt_sequence: 0,
            last_receipt: None,
        };
        job.validate(Some(activation))?;
        Ok(job)
    }

    /// Return the owning entity tag.
    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow the owning entity path.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Return the reserved constraint identity.
    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.constraint_id
    }

    /// Return the bound activation epoch.
    #[must_use]
    pub(in crate::db) const fn activation_epoch(&self) -> u64 {
        self.activation_epoch
    }

    /// Return the bound activation semantic fingerprint.
    #[must_use]
    pub(in crate::db) const fn activation_fingerprint(&self) -> ConstraintActivationFingerprint {
        self.activation_fingerprint
    }

    /// Return the accepted root against which activation began.
    #[must_use]
    pub(in crate::db) const fn base_schema_fingerprint(&self) -> AcceptedSchemaFingerprint {
        self.base_schema_fingerprint
    }

    /// Return the current bounded proof phase.
    #[must_use]
    pub(in crate::db) const fn phase(&self) -> ConstraintValidationPhase {
        self.phase
    }

    /// Borrow the exclusive canonical primary-key checkpoint.
    #[must_use]
    pub(in crate::db) const fn checkpoint(&self) -> Option<&RawDataStoreKey> {
        self.checkpoint.as_ref()
    }

    /// Borrow the participating revisions captured for Verify.
    #[must_use]
    pub(in crate::db) const fn captured_store_revisions(
        &self,
    ) -> Option<&[ConstraintStoreRevision]> {
        match self.captured_store_revisions.as_ref() {
            Some(revisions) => Some(revisions.as_slice()),
            None => None,
        }
    }

    /// Return isolated candidate generation identity, when required.
    #[must_use]
    pub(in crate::db) const fn staged_generation(&self) -> Option<u64> {
        self.staged_generation
    }

    /// Return whether recovery must rebuild candidate state for this row.
    ///
    /// Forward owns only the inclusive prefix through its durable checkpoint;
    /// Verify owns the complete generation after Forward proved exhaustion.
    #[must_use]
    pub(in crate::db) fn candidate_staging_contains(&self, key: &RawDataStoreKey) -> bool {
        if self.staged_generation.is_none() {
            return false;
        }
        match self.phase {
            ConstraintValidationPhase::Forward => self
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| key <= checkpoint),
            ConstraintValidationPhase::Verify => true,
        }
    }

    /// Return the cumulative saturating classified-row count.
    #[must_use]
    pub(in crate::db) const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    /// Return the cumulative saturating finding count.
    #[must_use]
    pub(in crate::db) const fn findings_seen(&self) -> u64 {
        self.findings_seen
    }

    /// Return the cumulative saturating proof restart count.
    #[must_use]
    pub(in crate::db) const fn restarts(&self) -> u64 {
        self.restarts
    }

    /// Borrow the unacknowledged bounded finding receipt.
    #[must_use]
    pub(in crate::db) const fn last_receipt(&self) -> Option<&ConstraintValidationReceipt> {
        self.last_receipt.as_ref()
    }

    /// Acknowledge the retained finding page before allowing further progress.
    pub(in crate::db) fn acknowledge_receipt(
        &mut self,
        acknowledged_sequence: Option<u64>,
    ) -> bool {
        let Some(receipt) = self.last_receipt.as_ref() else {
            return acknowledged_sequence.is_none();
        };
        if acknowledged_sequence != Some(receipt.page_sequence()) {
            return false;
        }
        self.last_receipt = None;
        true
    }

    /// Record one bounded Forward page after every visited row was classified.
    pub(in crate::db) fn record_forward_page(
        &mut self,
        checkpoint: Option<RawDataStoreKey>,
        rows_scanned: usize,
        findings: Vec<ConstraintValidationFinding>,
        exhausted: bool,
        captured_revisions: Option<Vec<ConstraintStoreRevision>>,
    ) -> Result<(), InternalError> {
        if self.phase != ConstraintValidationPhase::Forward
            || checkpoint
                .as_ref()
                .is_some_and(|key| !raw_key_matches_entity(key, self.entity_tag))
            || page_checkpoint_is_invalid(
                self.checkpoint.as_ref(),
                checkpoint.as_ref(),
                rows_scanned,
            )
            || (exhausted != captured_revisions.is_some())
        {
            return Err(InternalError::store_invariant());
        }
        self.record_page_counters(rows_scanned, findings.as_slice())?;
        self.forward_findings = self
            .forward_findings
            .saturating_add(u64::try_from(findings.len()).unwrap_or(u64::MAX));
        self.retain_findings(findings)?;
        self.checkpoint = checkpoint;
        if exhausted {
            self.checkpoint = None;
            if self.forward_findings == 0 {
                self.phase = ConstraintValidationPhase::Verify;
                self.captured_store_revisions = captured_revisions;
            }
            self.forward_findings = 0;
        }
        self.validate(None)
    }

    /// Record one clean bounded Verify page.
    pub(in crate::db) fn record_verify_page(
        &mut self,
        checkpoint: Option<RawDataStoreKey>,
        rows_scanned: usize,
    ) -> Result<(), InternalError> {
        if self.phase != ConstraintValidationPhase::Verify
            || checkpoint
                .as_ref()
                .is_some_and(|key| !raw_key_matches_entity(key, self.entity_tag))
            || page_checkpoint_is_invalid(
                self.checkpoint.as_ref(),
                checkpoint.as_ref(),
                rows_scanned,
            )
        {
            return Err(InternalError::store_invariant());
        }
        self.record_page_counters(rows_scanned, &[])?;
        self.checkpoint = checkpoint;
        self.validate(None)
    }

    /// Restart Forward after revision drift or Verify residual work.
    pub(in crate::db) fn restart_forward(
        &mut self,
        rows_scanned: usize,
        findings: Vec<ConstraintValidationFinding>,
    ) -> Result<(), InternalError> {
        if self.phase != ConstraintValidationPhase::Verify {
            return Err(InternalError::store_invariant());
        }
        self.record_page_counters(rows_scanned, findings.as_slice())?;
        self.retain_findings(findings)?;
        self.phase = ConstraintValidationPhase::Forward;
        self.checkpoint = None;
        self.captured_store_revisions = None;
        self.forward_findings = 0;
        self.restarts = self.restarts.saturating_add(1);
        self.validate(None)
    }

    fn record_page_counters(
        &mut self,
        rows_scanned: usize,
        findings: &[ConstraintValidationFinding],
    ) -> Result<(), InternalError> {
        if findings.len() > MAX_CONSTRAINT_VALIDATION_FINDINGS_PER_RECEIPT {
            return Err(InternalError::store_unsupported());
        }
        self.rows_scanned = self
            .rows_scanned
            .saturating_add(u64::try_from(rows_scanned).unwrap_or(u64::MAX));
        self.findings_seen = self
            .findings_seen
            .saturating_add(u64::try_from(findings.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn retain_findings(
        &mut self,
        findings: Vec<ConstraintValidationFinding>,
    ) -> Result<(), InternalError> {
        if findings.is_empty() {
            return Ok(());
        }
        self.receipt_sequence = self
            .receipt_sequence
            .checked_add(1)
            .ok_or_else(InternalError::store_unsupported)?;
        self.last_receipt = Some(ConstraintValidationReceipt::new(
            self.receipt_sequence,
            findings,
        ));
        Ok(())
    }

    pub(in crate::db::schema) fn validate(
        &self,
        activation: Option<&ConstraintActivationSnapshot>,
    ) -> Result<(), InternalError> {
        if self.entity_path.is_empty()
            || self.entity_path.len() > MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES
            || self.activation_epoch == 0
            || self
                .checkpoint
                .as_ref()
                .is_some_and(|key| !raw_key_matches_entity(key, self.entity_tag))
            || !phase_state_is_valid(self)
            || revisions_are_invalid(self.captured_store_revisions.as_deref())
            || self
                .last_receipt
                .as_ref()
                .is_some_and(|receipt| receipt_is_invalid(receipt, self.entity_tag))
            || self
                .last_receipt
                .as_ref()
                .is_some_and(|receipt| receipt.page_sequence() != self.receipt_sequence)
            || (self.receipt_sequence == 0 && self.last_receipt.is_some())
            || (self.phase == ConstraintValidationPhase::Verify && self.forward_findings != 0)
            || self.last_receipt.as_ref().is_some_and(|receipt| {
                self.findings_seen < u64::try_from(receipt.findings.len()).unwrap_or(u64::MAX)
            })
        {
            return Err(InternalError::store_corruption());
        }
        if let Some(activation) = activation
            && (activation.state() != ConstraintActivationState::Validating
                || activation.id() != self.constraint_id
                || activation.activation_epoch() != self.activation_epoch
                || activation.fingerprint() != self.activation_fingerprint
                || activation.base_schema_fingerprint() != self.base_schema_fingerprint
                || self.staged_generation
                    != match activation.kind() {
                        crate::db::schema::ConstraintActivationKind::Unique { .. }
                        | crate::db::schema::ConstraintActivationKind::Relation { .. } => {
                            Some(activation.activation_epoch())
                        }
                        crate::db::schema::ConstraintActivationKind::Check { .. }
                        | crate::db::schema::ConstraintActivationKind::NotNull { .. }
                        | crate::db::schema::ConstraintActivationKind::TargetedRule { .. } => None,
                    })
        {
            return Err(InternalError::store_corruption());
        }
        if let Some(activation) = activation
            && self.last_receipt.as_ref().is_some_and(|receipt| {
                receipt
                    .findings()
                    .iter()
                    .any(|finding| !finding_matches_activation(finding, activation.kind()))
            })
        {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

fn finding_matches_activation(
    finding: &ConstraintValidationFinding,
    kind: &ConstraintActivationKind,
) -> bool {
    match kind {
        ConstraintActivationKind::TargetedRule { target, .. } => {
            finding.field_ids() == [target.root_field_id()]
                && finding.value_path().is_some_and(|path| {
                    matches!(
                        path.components(),
                        [AcceptedTargetPathComponent::RootField(field_id), ..]
                            if *field_id == target.root_field_id()
                    )
                })
        }
        ConstraintActivationKind::Check { .. }
        | ConstraintActivationKind::NotNull { .. }
        | ConstraintActivationKind::Unique { .. }
        | ConstraintActivationKind::Relation { .. } => finding.value_path().is_none(),
    }
}

fn phase_state_is_valid(job: &ConstraintValidationJob) -> bool {
    match job.phase {
        ConstraintValidationPhase::Forward => job.captured_store_revisions.is_none(),
        ConstraintValidationPhase::Verify => job
            .captured_store_revisions
            .as_ref()
            .is_some_and(|revisions| !revisions.is_empty()),
    }
}

fn revisions_are_invalid(revisions: Option<&[ConstraintStoreRevision]>) -> bool {
    let Some(revisions) = revisions else {
        return false;
    };
    if revisions.len() > MAX_CONSTRAINT_VALIDATION_STORE_REVISIONS {
        return true;
    }
    revisions.iter().enumerate().any(|(index, revision)| {
        revision.store_path.is_empty()
            || revision.store_path.len() > MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES
            || revision.revision == 0
            || revisions[..index]
                .iter()
                .any(|prior| prior.store_path >= revision.store_path)
    })
}

fn receipt_is_invalid(receipt: &ConstraintValidationReceipt, entity_tag: EntityTag) -> bool {
    receipt.page_sequence == 0
        || receipt.findings.is_empty()
        || receipt.findings.len() > MAX_CONSTRAINT_VALIDATION_FINDINGS_PER_RECEIPT
        || receipt.findings.iter().any(|finding| {
            !raw_key_matches_entity(&finding.primary_key, entity_tag)
                || finding.field_ids.len() > MAX_CONSTRAINT_VALIDATION_FINDING_FIELDS
                || finding
                    .value_path
                    .as_ref()
                    .is_some_and(accepted_target_path_is_invalid)
                || finding.error_code == 0
                || finding.field_ids.windows(2).any(|pair| pair[0] >= pair[1])
        })
}

fn accepted_target_path_is_invalid(path: &AcceptedTargetPath) -> bool {
    path.components().is_empty()
        || path.components().len() > MAX_ACCEPTED_TARGET_PATH_COMPONENTS
        || !matches!(
            path.components(),
            [AcceptedTargetPathComponent::RootField(_), ..]
        )
}

fn raw_key_matches_entity(key: &RawDataStoreKey, entity_tag: EntityTag) -> bool {
    DecodedDataStoreKey::try_from_raw(key).is_ok_and(|decoded| decoded.entity_tag() == entity_tag)
}

fn page_checkpoint_is_invalid(
    current: Option<&RawDataStoreKey>,
    next: Option<&RawDataStoreKey>,
    rows_scanned: usize,
) -> bool {
    if rows_scanned == 0 {
        return current != next;
    }
    next.is_none_or(|next| current.is_some_and(|current| next <= current))
}

#[derive(CandidType, Deserialize)]
struct ConstraintValidationJobWire {
    codec_version: u32,
    contract_profile: u32,
    entity_tag: u64,
    entity_path: String,
    constraint_id: u32,
    activation_epoch: u64,
    activation_fingerprint: [u8; 32],
    base_schema_fingerprint: [u8; 32],
    phase: ConstraintValidationPhaseWire,
    checkpoint: Option<Vec<u8>>,
    captured_store_revisions: Option<Vec<ConstraintStoreRevisionWire>>,
    staged_generation: Option<u64>,
    rows_scanned: u64,
    findings_seen: u64,
    restarts: u64,
    forward_findings: u64,
    receipt_sequence: u64,
    last_receipt: Option<ConstraintValidationReceiptWire>,
}

#[derive(CandidType, Deserialize)]
enum ConstraintValidationPhaseWire {
    Forward,
    Verify,
}

#[derive(CandidType, Deserialize)]
struct ConstraintStoreRevisionWire {
    store_path: String,
    revision: u64,
}

#[derive(CandidType, Deserialize)]
struct ConstraintValidationReceiptWire {
    page_sequence: u64,
    findings: Vec<ConstraintValidationFindingWire>,
}

#[derive(CandidType, Deserialize)]
struct ConstraintValidationFindingWire {
    primary_key: Vec<u8>,
    field_ids: Vec<u32>,
    value_path: Option<Vec<ConstraintValidationPathComponentWire>>,
    error_code: u16,
}

#[derive(CandidType, Deserialize)]
enum ConstraintValidationPathComponentWire {
    RootField {
        field_id: u32,
    },
    RecordMember {
        composite_type_id: u32,
        member_id: u32,
    },
    TupleElement {
        composite_type_id: u32,
        ordinal: u32,
    },
    Newtype {
        composite_type_id: u32,
    },
    EnumVariant {
        enum_type_id: u32,
        variant_id: u32,
    },
    ListElement {
        index: u32,
    },
    SetElement {
        index: u32,
    },
    MapEntryKey {
        index: u32,
    },
    MapEntryValue {
        index: u32,
    },
}

/// Encode one closed current validation job.
pub(in crate::db) fn encode_constraint_validation_job(
    job: &ConstraintValidationJob,
) -> Result<Vec<u8>, InternalError> {
    job.validate(None)?;
    let encoded = Encode!(&ConstraintValidationJobWire::from_job(job))
        .map_err(|_| InternalError::store_invariant())?;
    if encoded.len() > MAX_CONSTRAINT_VALIDATION_JOB_BYTES {
        return Err(InternalError::store_unsupported());
    }
    Ok(encoded)
}

/// Decode one current validation job and reject malformed or obsolete bytes.
pub(in crate::db) fn decode_constraint_validation_job(
    bytes: &[u8],
) -> Result<ConstraintValidationJob, InternalError> {
    if bytes.len() > MAX_CONSTRAINT_VALIDATION_JOB_BYTES {
        return Err(InternalError::store_corruption());
    }
    let wire = Decode!(bytes, ConstraintValidationJobWire)
        .map_err(|_| InternalError::store_corruption())?;
    wire.into_job()
}

impl ConstraintValidationJobWire {
    fn from_job(job: &ConstraintValidationJob) -> Self {
        Self {
            codec_version: CONSTRAINT_VALIDATION_JOB_CODEC_VERSION,
            contract_profile: CONSTRAINT_VALIDATION_JOB_PROFILE,
            entity_tag: job.entity_tag.value(),
            entity_path: job.entity_path.clone(),
            constraint_id: job.constraint_id.get(),
            activation_epoch: job.activation_epoch(),
            activation_fingerprint: job.activation_fingerprint().as_bytes(),
            base_schema_fingerprint: job.base_schema_fingerprint().as_bytes(),
            phase: ConstraintValidationPhaseWire::from_phase(job.phase),
            checkpoint: job
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.as_bytes().to_vec()),
            captured_store_revisions: job.captured_store_revisions.as_ref().map(|revisions| {
                revisions
                    .iter()
                    .map(ConstraintStoreRevisionWire::from_revision)
                    .collect()
            }),
            staged_generation: job.staged_generation(),
            rows_scanned: job.rows_scanned,
            findings_seen: job.findings_seen(),
            restarts: job.restarts(),
            forward_findings: job.forward_findings,
            receipt_sequence: job.receipt_sequence,
            last_receipt: job
                .last_receipt
                .as_ref()
                .map(ConstraintValidationReceiptWire::from_receipt),
        }
    }

    fn into_job(self) -> Result<ConstraintValidationJob, InternalError> {
        if self.codec_version != CONSTRAINT_VALIDATION_JOB_CODEC_VERSION
            || self.contract_profile != CONSTRAINT_VALIDATION_JOB_PROFILE
        {
            return Err(InternalError::serialize_incompatible_persisted_format());
        }
        let constraint_id =
            ConstraintId::new(self.constraint_id).ok_or_else(InternalError::store_corruption)?;
        let checkpoint = self.checkpoint.map(raw_key_from_wire).transpose()?;
        let job = ConstraintValidationJob {
            entity_tag: EntityTag::new(self.entity_tag),
            entity_path: self.entity_path,
            constraint_id,
            activation_epoch: self.activation_epoch,
            activation_fingerprint: ConstraintActivationFingerprint::new(
                self.activation_fingerprint,
            ),
            base_schema_fingerprint: AcceptedSchemaFingerprint::new(self.base_schema_fingerprint),
            phase: self.phase.into_phase(),
            checkpoint,
            captured_store_revisions: self.captured_store_revisions.map(|revisions| {
                revisions
                    .into_iter()
                    .map(ConstraintStoreRevisionWire::into_revision)
                    .collect()
            }),
            staged_generation: self.staged_generation,
            rows_scanned: self.rows_scanned,
            findings_seen: self.findings_seen,
            restarts: self.restarts,
            forward_findings: self.forward_findings,
            receipt_sequence: self.receipt_sequence,
            last_receipt: self
                .last_receipt
                .map(ConstraintValidationReceiptWire::into_receipt)
                .transpose()?,
        };
        job.validate(None)?;
        Ok(job)
    }
}

impl ConstraintValidationPhaseWire {
    const fn from_phase(phase: ConstraintValidationPhase) -> Self {
        match phase {
            ConstraintValidationPhase::Forward => Self::Forward,
            ConstraintValidationPhase::Verify => Self::Verify,
        }
    }

    const fn into_phase(self) -> ConstraintValidationPhase {
        match self {
            Self::Forward => ConstraintValidationPhase::Forward,
            Self::Verify => ConstraintValidationPhase::Verify,
        }
    }
}

impl ConstraintStoreRevisionWire {
    fn from_revision(revision: &ConstraintStoreRevision) -> Self {
        Self {
            store_path: revision.store_path.clone(),
            revision: revision.revision,
        }
    }

    fn into_revision(self) -> ConstraintStoreRevision {
        ConstraintStoreRevision::new(self.store_path, self.revision)
    }
}

impl ConstraintValidationReceiptWire {
    fn from_receipt(receipt: &ConstraintValidationReceipt) -> Self {
        Self {
            page_sequence: receipt.page_sequence,
            findings: receipt
                .findings()
                .iter()
                .map(ConstraintValidationFindingWire::from_finding)
                .collect(),
        }
    }

    fn into_receipt(self) -> Result<ConstraintValidationReceipt, InternalError> {
        Ok(ConstraintValidationReceipt::new(
            self.page_sequence,
            self.findings
                .into_iter()
                .map(ConstraintValidationFindingWire::into_finding)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}

impl ConstraintValidationFindingWire {
    fn from_finding(finding: &ConstraintValidationFinding) -> Self {
        Self {
            primary_key: finding.primary_key().as_bytes().to_vec(),
            field_ids: finding
                .field_ids()
                .iter()
                .map(|field| field.get())
                .collect(),
            value_path: finding.value_path().map(|path| {
                path.components()
                    .iter()
                    .map(ConstraintValidationPathComponentWire::from_component)
                    .collect()
            }),
            error_code: finding.error_code(),
        }
    }

    fn into_finding(self) -> Result<ConstraintValidationFinding, InternalError> {
        let primary_key = raw_key_from_wire(self.primary_key)?;
        let field_ids = self.field_ids.into_iter().map(FieldId::new).collect();
        match self.value_path {
            Some(path) => Ok(ConstraintValidationFinding::new_targeted(
                primary_key,
                field_ids,
                AcceptedTargetPath::new(
                    path.into_iter()
                        .map(ConstraintValidationPathComponentWire::into_component)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                self.error_code,
            )),
            None => Ok(ConstraintValidationFinding::new(
                primary_key,
                field_ids,
                self.error_code,
            )),
        }
    }
}

impl ConstraintValidationPathComponentWire {
    const fn from_component(component: &AcceptedTargetPathComponent) -> Self {
        match component {
            AcceptedTargetPathComponent::RootField(field_id) => Self::RootField {
                field_id: field_id.get(),
            },
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id,
                member_id,
            } => Self::RecordMember {
                composite_type_id: composite_type_id.get(),
                member_id: member_id.get(),
            },
            AcceptedTargetPathComponent::TupleElement {
                composite_type_id,
                ordinal,
            } => Self::TupleElement {
                composite_type_id: composite_type_id.get(),
                ordinal: *ordinal,
            },
            AcceptedTargetPathComponent::Newtype { composite_type_id } => Self::Newtype {
                composite_type_id: composite_type_id.get(),
            },
            AcceptedTargetPathComponent::EnumVariant {
                enum_type_id,
                variant_id,
            } => Self::EnumVariant {
                enum_type_id: enum_type_id.get(),
                variant_id: variant_id.get(),
            },
            AcceptedTargetPathComponent::ListElement { index } => {
                Self::ListElement { index: *index }
            }
            AcceptedTargetPathComponent::SetElement { index } => Self::SetElement { index: *index },
            AcceptedTargetPathComponent::MapEntryKey { index } => {
                Self::MapEntryKey { index: *index }
            }
            AcceptedTargetPathComponent::MapEntryValue { index } => {
                Self::MapEntryValue { index: *index }
            }
        }
    }

    fn into_component(self) -> Result<AcceptedTargetPathComponent, InternalError> {
        match self {
            Self::RootField { field_id } => Ok(AcceptedTargetPathComponent::RootField(
                FieldId::new(field_id),
            )),
            Self::RecordMember {
                composite_type_id,
                member_id,
            } => Ok(AcceptedTargetPathComponent::RecordMember {
                composite_type_id: CompositeTypeId::new(composite_type_id)
                    .ok_or_else(InternalError::store_corruption)?,
                member_id: CompositeFieldId::new(member_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::TupleElement {
                composite_type_id,
                ordinal,
            } => Ok(AcceptedTargetPathComponent::TupleElement {
                composite_type_id: CompositeTypeId::new(composite_type_id)
                    .ok_or_else(InternalError::store_corruption)?,
                ordinal,
            }),
            Self::Newtype { composite_type_id } => Ok(AcceptedTargetPathComponent::Newtype {
                composite_type_id: CompositeTypeId::new(composite_type_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::EnumVariant {
                enum_type_id,
                variant_id,
            } => Ok(AcceptedTargetPathComponent::EnumVariant {
                enum_type_id: EnumTypeId::new(enum_type_id)
                    .ok_or_else(InternalError::store_corruption)?,
                variant_id: EnumVariantId::new(variant_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::ListElement { index } => Ok(AcceptedTargetPathComponent::ListElement { index }),
            Self::SetElement { index } => Ok(AcceptedTargetPathComponent::SetElement { index }),
            Self::MapEntryKey { index } => Ok(AcceptedTargetPathComponent::MapEntryKey { index }),
            Self::MapEntryValue { index } => {
                Ok(AcceptedTargetPathComponent::MapEntryValue { index })
            }
        }
    }
}

fn raw_key_from_wire(bytes: Vec<u8>) -> Result<RawDataStoreKey, InternalError> {
    if bytes.len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE {
        return Err(InternalError::store_corruption());
    }
    let key = <RawDataStoreKey as Storable>::from_bytes(Cow::Owned(bytes));
    DecodedDataStoreKey::try_from_raw(&key).map_err(|_| InternalError::store_corruption())?;
    Ok(key)
}

#[cfg(test)]
mod tests;
