//! Module: db::schema::constraint_validation
//! Responsibility: bounded durable progress for accepted constraint activation.
//! Does not own: activation semantics, row mutation, or final structural publication.
//! Boundary: binds one schema-owned Forward/Verify job to an exact activation.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        database_format::crc32c,
        schema::{
            AcceptedSchemaFingerprint, AcceptedTargetPath, AcceptedTargetPathComponent,
            ConstraintActivationFingerprint, ConstraintActivationKind,
            ConstraintActivationSnapshot, ConstraintActivationState, ConstraintId, FieldId,
            MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
            composite_catalog::{CompositeFieldId, CompositeTypeId},
            enum_catalog::{EnumTypeId, EnumVariantId},
            wire::{SchemaWireReader, SchemaWireWriter},
        },
    },
    error::InternalError,
    types::EntityTag,
};

const CONSTRAINT_VALIDATION_JOB_MAGIC: [u8; 8] = *b"ICYCVJOB";
const CONSTRAINT_VALIDATION_JOB_CODEC_VERSION: u8 = 1;
pub(in crate::db) const MAX_CONSTRAINT_VALIDATION_JOB_BYTES: usize = 64 * 1024;
const CONSTRAINT_VALIDATION_JOB_CHECKSUM_BYTES: usize = size_of::<u32>();
const OPTIONAL_ABSENT_TAG: u8 = 0;
const OPTIONAL_PRESENT_TAG: u8 = 1;
const PHASE_FORWARD_TAG: u8 = 1;
const PHASE_VERIFY_TAG: u8 = 2;
const PATH_ROOT_FIELD_TAG: u8 = 1;
const PATH_RECORD_MEMBER_TAG: u8 = 2;
const PATH_TUPLE_ELEMENT_TAG: u8 = 3;
const PATH_NEWTYPE_TAG: u8 = 4;
const PATH_ENUM_VARIANT_TAG: u8 = 5;
const PATH_LIST_ELEMENT_TAG: u8 = 6;
const PATH_SET_ELEMENT_TAG: u8 = 7;
const PATH_MAP_ENTRY_KEY_TAG: u8 = 8;
const PATH_MAP_ENTRY_VALUE_TAG: u8 = 9;
const MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES: usize = 4 * 1024;
const MAX_CONSTRAINT_VALIDATION_STORE_REVISIONS: usize = 16;
const MAX_CONSTRAINT_VALIDATION_FINDINGS_PER_RECEIPT: usize = 64;
const MAX_CONSTRAINT_VALIDATION_FINDING_FIELDS: usize = 32;

type ConstraintValidationJobWriter = SchemaWireWriter<
    { MAX_CONSTRAINT_VALIDATION_JOB_BYTES - CONSTRAINT_VALIDATION_JOB_CHECKSUM_BYTES },
>;
type ConstraintValidationJobReader<'a> = SchemaWireReader<'a>;

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

/// Encode one closed current validation job.
pub(in crate::db) fn encode_constraint_validation_job(
    job: &ConstraintValidationJob,
) -> Result<Vec<u8>, InternalError> {
    job.validate(None)?;

    let mut writer = ConstraintValidationJobWriter::new();
    writer.push_bytes(&CONSTRAINT_VALIDATION_JOB_MAGIC);
    writer.push_u8(CONSTRAINT_VALIDATION_JOB_CODEC_VERSION);
    writer.push_u64(job.entity_tag.value());
    writer.push_bounded_string(
        job.entity_path.as_str(),
        MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES,
    )?;
    writer.push_u32(job.constraint_id.get());
    writer.push_u64(job.activation_epoch);
    writer.push_bytes(&job.activation_fingerprint.as_bytes());
    writer.push_bytes(&job.base_schema_fingerprint.as_bytes());
    writer.push_u8(match job.phase {
        ConstraintValidationPhase::Forward => PHASE_FORWARD_TAG,
        ConstraintValidationPhase::Verify => PHASE_VERIFY_TAG,
    });
    encode_optional_raw_key(&mut writer, job.checkpoint.as_ref())?;
    encode_optional_revisions(&mut writer, job.captured_store_revisions.as_deref())?;
    encode_optional_u64(&mut writer, job.staged_generation);
    writer.push_u64(job.rows_scanned);
    writer.push_u64(job.findings_seen);
    writer.push_u64(job.restarts);
    writer.push_u64(job.forward_findings);
    writer.push_u64(job.receipt_sequence);
    encode_optional_receipt(&mut writer, job.last_receipt.as_ref())?;

    let mut encoded = writer.finish()?;
    encoded.extend_from_slice(&crc32c(&encoded).to_be_bytes());
    Ok(encoded)
}

/// Decode one current validation job and reject malformed or obsolete bytes.
pub(in crate::db) fn decode_constraint_validation_job(
    bytes: &[u8],
) -> Result<ConstraintValidationJob, InternalError> {
    if bytes.len() <= CONSTRAINT_VALIDATION_JOB_CHECKSUM_BYTES
        || bytes.len() > MAX_CONSTRAINT_VALIDATION_JOB_BYTES
    {
        return Err(InternalError::store_corruption());
    }

    let checksum_offset = bytes
        .len()
        .checked_sub(CONSTRAINT_VALIDATION_JOB_CHECKSUM_BYTES)
        .ok_or_else(InternalError::store_corruption)?;
    let (body, checksum) = bytes.split_at(checksum_offset);
    let expected_checksum = u32::from_be_bytes(
        checksum
            .try_into()
            .map_err(|_| InternalError::store_corruption())?,
    );
    if crc32c(body) != expected_checksum {
        return Err(InternalError::store_corruption());
    }

    let mut reader = ConstraintValidationJobReader::new(body);
    if reader.read_array::<8>()? != CONSTRAINT_VALIDATION_JOB_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u8()? != CONSTRAINT_VALIDATION_JOB_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let entity_tag = EntityTag::new(reader.read_u64()?);
    let entity_path = reader.read_bounded_string(MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES)?;
    let constraint_id =
        ConstraintId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let activation_epoch = reader.read_u64()?;
    let activation_fingerprint = ConstraintActivationFingerprint::new(reader.read_array()?);
    let base_schema_fingerprint = AcceptedSchemaFingerprint::new(reader.read_array()?);
    let phase = match reader.read_u8()? {
        PHASE_FORWARD_TAG => ConstraintValidationPhase::Forward,
        PHASE_VERIFY_TAG => ConstraintValidationPhase::Verify,
        _ => return Err(InternalError::store_corruption()),
    };
    let checkpoint = decode_optional_raw_key(&mut reader)?;
    let captured_store_revisions = decode_optional_revisions(&mut reader)?;
    let staged_generation = decode_optional_u64(&mut reader)?;
    let rows_scanned = reader.read_u64()?;
    let findings_seen = reader.read_u64()?;
    let restarts = reader.read_u64()?;
    let forward_findings = reader.read_u64()?;
    let receipt_sequence = reader.read_u64()?;
    let last_receipt = decode_optional_receipt(&mut reader)?;
    reader.finish()?;

    let job = ConstraintValidationJob {
        entity_tag,
        entity_path,
        constraint_id,
        activation_epoch,
        activation_fingerprint,
        base_schema_fingerprint,
        phase,
        checkpoint,
        captured_store_revisions,
        staged_generation,
        rows_scanned,
        findings_seen,
        restarts,
        forward_findings,
        receipt_sequence,
        last_receipt,
    };
    job.validate(None)?;
    Ok(job)
}

fn encode_optional_raw_key(
    writer: &mut ConstraintValidationJobWriter,
    key: Option<&RawDataStoreKey>,
) -> Result<(), InternalError> {
    match key {
        None => writer.push_u8(OPTIONAL_ABSENT_TAG),
        Some(key) => {
            writer.push_u8(OPTIONAL_PRESENT_TAG);
            writer.push_bounded_len_prefixed_bytes(
                key.as_bytes(),
                RawDataStoreKey::MAX_STORED_SIZE_USIZE,
            )?;
        }
    }
    Ok(())
}

fn decode_optional_raw_key(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<Option<RawDataStoreKey>, InternalError> {
    match reader.read_u8()? {
        OPTIONAL_ABSENT_TAG => Ok(None),
        OPTIONAL_PRESENT_TAG => decode_raw_key(reader).map(Some),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_revisions(
    writer: &mut ConstraintValidationJobWriter,
    revisions: Option<&[ConstraintStoreRevision]>,
) -> Result<(), InternalError> {
    let Some(revisions) = revisions else {
        writer.push_u8(OPTIONAL_ABSENT_TAG);
        return Ok(());
    };
    writer.push_u8(OPTIONAL_PRESENT_TAG);
    writer.push_len(revisions.len())?;
    for revision in revisions {
        writer.push_bounded_string(
            revision.store_path.as_str(),
            MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES,
        )?;
        writer.push_u64(revision.revision);
    }
    Ok(())
}

fn decode_optional_revisions(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<Option<Vec<ConstraintStoreRevision>>, InternalError> {
    match reader.read_u8()? {
        OPTIONAL_ABSENT_TAG => Ok(None),
        OPTIONAL_PRESENT_TAG => {
            let count = reader.read_bounded_count(MAX_CONSTRAINT_VALIDATION_STORE_REVISIONS)?;
            let mut revisions = Vec::new();
            revisions
                .try_reserve_exact(count)
                .map_err(|_| InternalError::store_corruption())?;
            for _ in 0..count {
                revisions.push(ConstraintStoreRevision::new(
                    reader.read_bounded_string(MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES)?,
                    reader.read_u64()?,
                ));
            }
            Ok(Some(revisions))
        }
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_u64(writer: &mut ConstraintValidationJobWriter, value: Option<u64>) {
    match value {
        None => writer.push_u8(OPTIONAL_ABSENT_TAG),
        Some(value) => {
            writer.push_u8(OPTIONAL_PRESENT_TAG);
            writer.push_u64(value);
        }
    }
}

fn decode_optional_u64(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<Option<u64>, InternalError> {
    match reader.read_u8()? {
        OPTIONAL_ABSENT_TAG => Ok(None),
        OPTIONAL_PRESENT_TAG => reader.read_u64().map(Some),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_receipt(
    writer: &mut ConstraintValidationJobWriter,
    receipt: Option<&ConstraintValidationReceipt>,
) -> Result<(), InternalError> {
    let Some(receipt) = receipt else {
        writer.push_u8(OPTIONAL_ABSENT_TAG);
        return Ok(());
    };
    writer.push_u8(OPTIONAL_PRESENT_TAG);
    writer.push_u64(receipt.page_sequence);
    writer.push_len(receipt.findings.len())?;
    for finding in &receipt.findings {
        encode_finding(writer, finding)?;
    }
    Ok(())
}

fn decode_optional_receipt(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<Option<ConstraintValidationReceipt>, InternalError> {
    match reader.read_u8()? {
        OPTIONAL_ABSENT_TAG => Ok(None),
        OPTIONAL_PRESENT_TAG => {
            let page_sequence = reader.read_u64()?;
            let count =
                reader.read_bounded_count(MAX_CONSTRAINT_VALIDATION_FINDINGS_PER_RECEIPT)?;
            let mut findings = Vec::new();
            findings
                .try_reserve_exact(count)
                .map_err(|_| InternalError::store_corruption())?;
            for _ in 0..count {
                findings.push(decode_finding(reader)?);
            }
            Ok(Some(ConstraintValidationReceipt::new(
                page_sequence,
                findings,
            )))
        }
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_finding(
    writer: &mut ConstraintValidationJobWriter,
    finding: &ConstraintValidationFinding,
) -> Result<(), InternalError> {
    writer.push_bounded_len_prefixed_bytes(
        finding.primary_key.as_bytes(),
        RawDataStoreKey::MAX_STORED_SIZE_USIZE,
    )?;
    writer.push_len(finding.field_ids.len())?;
    for field_id in &finding.field_ids {
        writer.push_u32(field_id.get());
    }
    match finding.value_path.as_ref() {
        None => writer.push_u8(OPTIONAL_ABSENT_TAG),
        Some(path) => {
            writer.push_u8(OPTIONAL_PRESENT_TAG);
            writer.push_len(path.components().len())?;
            for component in path.components() {
                encode_path_component(writer, component);
            }
        }
    }
    writer.push_u16(finding.error_code);
    Ok(())
}

fn decode_finding(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<ConstraintValidationFinding, InternalError> {
    let primary_key = decode_raw_key(reader)?;
    let field_count = reader.read_bounded_count(MAX_CONSTRAINT_VALIDATION_FINDING_FIELDS)?;
    let mut field_ids = Vec::new();
    field_ids
        .try_reserve_exact(field_count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..field_count {
        field_ids.push(FieldId::new(reader.read_u32()?));
    }
    let value_path = match reader.read_u8()? {
        OPTIONAL_ABSENT_TAG => None,
        OPTIONAL_PRESENT_TAG => {
            let component_count = reader.read_bounded_count(MAX_ACCEPTED_TARGET_PATH_COMPONENTS)?;
            let mut components = Vec::new();
            components
                .try_reserve_exact(component_count)
                .map_err(|_| InternalError::store_corruption())?;
            for _ in 0..component_count {
                components.push(decode_path_component(reader)?);
            }
            Some(AcceptedTargetPath::new(components))
        }
        _ => return Err(InternalError::store_corruption()),
    };
    let error_code = reader.read_u16()?;
    Ok(match value_path {
        Some(value_path) => ConstraintValidationFinding::new_targeted(
            primary_key,
            field_ids,
            value_path,
            error_code,
        ),
        None => ConstraintValidationFinding::new(primary_key, field_ids, error_code),
    })
}

fn encode_path_component(
    writer: &mut ConstraintValidationJobWriter,
    component: &AcceptedTargetPathComponent,
) {
    match component {
        AcceptedTargetPathComponent::RootField(field_id) => {
            writer.push_u8(PATH_ROOT_FIELD_TAG);
            writer.push_u32(field_id.get());
        }
        AcceptedTargetPathComponent::RecordMember {
            composite_type_id,
            member_id,
        } => {
            writer.push_u8(PATH_RECORD_MEMBER_TAG);
            writer.push_u32(composite_type_id.get());
            writer.push_u32(member_id.get());
        }
        AcceptedTargetPathComponent::TupleElement {
            composite_type_id,
            ordinal,
        } => {
            writer.push_u8(PATH_TUPLE_ELEMENT_TAG);
            writer.push_u32(composite_type_id.get());
            writer.push_u32(*ordinal);
        }
        AcceptedTargetPathComponent::Newtype { composite_type_id } => {
            writer.push_u8(PATH_NEWTYPE_TAG);
            writer.push_u32(composite_type_id.get());
        }
        AcceptedTargetPathComponent::EnumVariant {
            enum_type_id,
            variant_id,
        } => {
            writer.push_u8(PATH_ENUM_VARIANT_TAG);
            writer.push_u32(enum_type_id.get());
            writer.push_u32(variant_id.get());
        }
        AcceptedTargetPathComponent::ListElement { index } => {
            writer.push_u8(PATH_LIST_ELEMENT_TAG);
            writer.push_u32(*index);
        }
        AcceptedTargetPathComponent::SetElement { index } => {
            writer.push_u8(PATH_SET_ELEMENT_TAG);
            writer.push_u32(*index);
        }
        AcceptedTargetPathComponent::MapEntryKey { index } => {
            writer.push_u8(PATH_MAP_ENTRY_KEY_TAG);
            writer.push_u32(*index);
        }
        AcceptedTargetPathComponent::MapEntryValue { index } => {
            writer.push_u8(PATH_MAP_ENTRY_VALUE_TAG);
            writer.push_u32(*index);
        }
    }
}

fn decode_path_component(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<AcceptedTargetPathComponent, InternalError> {
    match reader.read_u8()? {
        PATH_ROOT_FIELD_TAG => Ok(AcceptedTargetPathComponent::RootField(FieldId::new(
            reader.read_u32()?,
        ))),
        PATH_RECORD_MEMBER_TAG => Ok(AcceptedTargetPathComponent::RecordMember {
            composite_type_id: decode_composite_type_id(reader)?,
            member_id: CompositeFieldId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        PATH_TUPLE_ELEMENT_TAG => Ok(AcceptedTargetPathComponent::TupleElement {
            composite_type_id: decode_composite_type_id(reader)?,
            ordinal: reader.read_u32()?,
        }),
        PATH_NEWTYPE_TAG => Ok(AcceptedTargetPathComponent::Newtype {
            composite_type_id: decode_composite_type_id(reader)?,
        }),
        PATH_ENUM_VARIANT_TAG => Ok(AcceptedTargetPathComponent::EnumVariant {
            enum_type_id: EnumTypeId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
            variant_id: EnumVariantId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        PATH_LIST_ELEMENT_TAG => Ok(AcceptedTargetPathComponent::ListElement {
            index: reader.read_u32()?,
        }),
        PATH_SET_ELEMENT_TAG => Ok(AcceptedTargetPathComponent::SetElement {
            index: reader.read_u32()?,
        }),
        PATH_MAP_ENTRY_KEY_TAG => Ok(AcceptedTargetPathComponent::MapEntryKey {
            index: reader.read_u32()?,
        }),
        PATH_MAP_ENTRY_VALUE_TAG => Ok(AcceptedTargetPathComponent::MapEntryValue {
            index: reader.read_u32()?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn decode_composite_type_id(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<CompositeTypeId, InternalError> {
    CompositeTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)
}

fn decode_raw_key(
    reader: &mut ConstraintValidationJobReader<'_>,
) -> Result<RawDataStoreKey, InternalError> {
    let bytes = reader.read_bounded_len_prefixed_bytes(RawDataStoreKey::MAX_STORED_SIZE_USIZE)?;
    let key = RawDataStoreKey::from_persisted_bytes(bytes.to_vec());
    DecodedDataStoreKey::try_from_raw(&key).map_err(|_| InternalError::store_corruption())?;
    Ok(key)
}

#[cfg(test)]
mod tests;
