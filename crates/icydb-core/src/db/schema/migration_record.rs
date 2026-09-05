//! Durable source-migration lifecycle authority.
//!
//! This module owns the sole current migration record and its exact
//! compare-and-replace effect. It stores accepted IDs and opaque digests only;
//! generated Rust models and executable migration code are never persisted.

use crate::{
    db::{
        database_format::crc32c,
        schema::{
            FieldId,
            wire::{SchemaWireReader, SchemaWireWriter},
        },
    },
    error::InternalError,
    types::EntityTag,
};
use icydb_schema::{
    EntitySourceDigest, EntitySourceKey, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
    SchemaMigrationPlanDigest, SchemaProposalDigest, TargetDatabaseIdentity, TargetStoreIdentity,
};

pub(in crate::db) const MAX_SCHEMA_MIGRATION_RECORD_BYTES: usize = 2 * 1024 * 1024;
pub(in crate::db::schema) const MAX_SCHEMA_MIGRATION_RECORD_ENTITIES: usize = 4_096;
pub(in crate::db::schema) const MAX_SCHEMA_MIGRATION_RECORD_INDEXES: usize = 4_096;
pub(in crate::db::schema) const MAX_SCHEMA_MIGRATION_CURSOR_BYTES: usize = 64 * 1024;
pub(in crate::db::schema) const MAX_SCHEMA_MIGRATION_FINDINGS: usize = 64;

const MIGRATION_MAGIC: &[u8; 8] = b"ICYSMIG1";
const MIGRATION_VERSION: u8 = 1;
const MIGRATION_HEADER_BYTES: usize = 8 + 1 + 4;
const MIGRATION_CHECKSUM_BYTES: usize = 4;

type MigrationWriter = SchemaWireWriter<MAX_SCHEMA_MIGRATION_RECORD_BYTES>;
type MigrationReader<'a> = SchemaWireReader<'a>;

/// Persisted lifecycle phase. Public source-only phases are derived rather
/// than stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum PersistedSchemaMigrationPhase {
    Prepared,
    Validating,
    ReadyToRewrite,
    RewritingRows,
    RebuildingIndexes,
    FinalValidation,
    Publishing,
    Applied,
    Rejected,
    Aborted,
}

impl PersistedSchemaMigrationPhase {
    #[must_use]
    pub(in crate::db::schema) const fn blocks_ordinary_operations(self) -> bool {
        matches!(
            self,
            Self::Validating
                | Self::ReadyToRewrite
                | Self::RewritingRows
                | Self::RebuildingIndexes
                | Self::FinalValidation
                | Self::Publishing
                | Self::Rejected
        )
    }

    #[must_use]
    pub(in crate::db::schema) const fn terminal(self) -> bool {
        matches!(self, Self::Applied | Self::Aborted)
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn abortable(self) -> bool {
        matches!(
            self,
            Self::Prepared | Self::Validating | Self::ReadyToRewrite | Self::Rejected
        )
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Prepared => 0,
            Self::Validating => 1,
            Self::ReadyToRewrite => 2,
            Self::RewritingRows => 3,
            Self::RebuildingIndexes => 4,
            Self::FinalValidation => 5,
            Self::Publishing => 6,
            Self::Applied => 7,
            Self::Rejected => 8,
            Self::Aborted => 9,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, InternalError> {
        match tag {
            0 => Ok(Self::Prepared),
            1 => Ok(Self::Validating),
            2 => Ok(Self::ReadyToRewrite),
            3 => Ok(Self::RewritingRows),
            4 => Ok(Self::RebuildingIndexes),
            5 => Ok(Self::FinalValidation),
            6 => Ok(Self::Publishing),
            7 => Ok(Self::Applied),
            8 => Ok(Self::Rejected),
            9 => Ok(Self::Aborted),
            _ => Err(InternalError::store_corruption()),
        }
    }

    #[cfg(any(test, feature = "migration"))]
    fn may_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Prepared,
                    Self::Validating | Self::Rejected | Self::Aborted
                ) | (
                    Self::Validating,
                    Self::ReadyToRewrite | Self::Rejected | Self::Aborted
                ) | (Self::ReadyToRewrite, Self::RewritingRows | Self::Aborted)
                    | (Self::RewritingRows, Self::RebuildingIndexes)
                    | (Self::RebuildingIndexes, Self::FinalValidation)
                    | (Self::FinalValidation, Self::Publishing)
                    | (Self::Publishing, Self::Applied)
                    | (Self::Rejected, Self::Aborted)
            )
    }
}

/// One source-declared immediate-predecessor transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PersistedSchemaMigrationTransition {
    entity: EntitySourceKey,
    from_version: u32,
    to_version: u32,
}

impl PersistedSchemaMigrationTransition {
    pub(in crate::db::schema) fn try_new(
        entity: EntitySourceKey,
        from_version: u32,
        to_version: u32,
    ) -> Result<Self, InternalError> {
        if from_version == 0 || from_version.checked_add(1) != Some(to_version) {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            entity,
            from_version,
            to_version,
        })
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn entity(&self) -> &EntitySourceKey {
        &self.entity
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn predecessor_version(&self) -> u32 {
        self.from_version
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn target_version(&self) -> u32 {
        self.to_version
    }
}

/// Accepted entity identity plus the current generated-source meaning.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::schema) struct PersistedSchemaMigrationEntity {
    store: TargetStoreIdentity,
    entity: EntityTag,
    source_digest: EntitySourceDigest,
}

impl PersistedSchemaMigrationEntity {
    pub(in crate::db::schema) fn try_new(
        store: TargetStoreIdentity,
        entity: EntityTag,
        source_digest: EntitySourceDigest,
    ) -> Result<Self, InternalError> {
        if entity.value() == 0 || source_digest.to_bytes() == [0; 32] {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            store,
            entity,
            source_digest,
        })
    }
}

/// One staged physical index generation reserved by the migration.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::schema) struct PersistedSchemaMigrationIndex {
    store: TargetStoreIdentity,
    entity: EntityTag,
    index: u64,
    generation: u64,
}

impl PersistedSchemaMigrationIndex {
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn try_new(
        store: TargetStoreIdentity,
        entity: EntityTag,
        index: u64,
        generation: u64,
    ) -> Result<Self, InternalError> {
        if entity.value() == 0 || index == 0 || generation == 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            store,
            entity,
            index,
            generation,
        })
    }
}

/// One accepted-ID row cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PersistedSchemaMigrationRowCursor {
    store: TargetStoreIdentity,
    entity: EntityTag,
    primary_key: Vec<u8>,
}

impl PersistedSchemaMigrationRowCursor {
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn try_new(
        store: TargetStoreIdentity,
        entity: EntityTag,
        primary_key: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let cursor = Self {
            store,
            entity,
            primary_key,
        };
        if cursor.entity.value() == 0
            || cursor.primary_key.is_empty()
            || cursor.primary_key.len() > MAX_SCHEMA_MIGRATION_CURSOR_BYTES
        {
            return Err(InternalError::store_invariant());
        }
        Ok(cursor)
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn store(&self) -> TargetStoreIdentity {
        self.store
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn entity(&self) -> EntityTag {
        self.entity
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn primary_key(&self) -> &[u8] {
        self.primary_key.as_slice()
    }
}

/// One accepted-ID index cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PersistedSchemaMigrationIndexCursor {
    store: TargetStoreIdentity,
    entity: EntityTag,
    index: u64,
    key: Vec<u8>,
}

impl PersistedSchemaMigrationIndexCursor {
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn try_new(
        store: TargetStoreIdentity,
        entity: EntityTag,
        index: u64,
        key: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let cursor = Self {
            store,
            entity,
            index,
            key,
        };
        if cursor.entity.value() == 0
            || cursor.index == 0
            || cursor.key.is_empty()
            || cursor.key.len() > MAX_SCHEMA_MIGRATION_CURSOR_BYTES
        {
            return Err(InternalError::store_invariant());
        }
        Ok(cursor)
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn store(&self) -> TargetStoreIdentity {
        self.store
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn key(&self) -> &[u8] {
        self.key.as_slice()
    }
}

/// Persisted typed finding family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum PersistedSchemaMigrationFindingKind {
    Transform,
    UniqueIndex,
    Relation,
    Constraint,
}

/// Exact row-local reason retained for a failed closed transform.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum PersistedSchemaMigrationTransformReason {
    Overflow,
    NegativeToUnsigned,
    PrecisionLoss,
    NullSource,
    ValueContract,
}

/// One exact accepted-ID finding retained for acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PersistedSchemaMigrationFinding {
    kind: PersistedSchemaMigrationFindingKind,
    store: TargetStoreIdentity,
    entity: EntityTag,
    primary_key: Vec<u8>,
    source_field: Option<FieldId>,
    target_field: Option<FieldId>,
    transform_reason: Option<PersistedSchemaMigrationTransformReason>,
}

impl PersistedSchemaMigrationFinding {
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn try_new(
        kind: PersistedSchemaMigrationFindingKind,
        store: TargetStoreIdentity,
        entity: EntityTag,
        primary_key: Vec<u8>,
    ) -> Result<Self, InternalError> {
        if kind == PersistedSchemaMigrationFindingKind::Transform {
            return Err(InternalError::store_invariant());
        }
        let finding = Self {
            kind,
            store,
            entity,
            primary_key,
            source_field: None,
            target_field: None,
            transform_reason: None,
        };
        if !finding.has_valid_shape() {
            return Err(InternalError::store_invariant());
        }
        Ok(finding)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn try_new_transform(
        store: TargetStoreIdentity,
        entity: EntityTag,
        primary_key: Vec<u8>,
        source_field: Option<FieldId>,
        target_field: FieldId,
        transform_reason: PersistedSchemaMigrationTransformReason,
    ) -> Result<Self, InternalError> {
        let finding = Self {
            kind: PersistedSchemaMigrationFindingKind::Transform,
            store,
            entity,
            primary_key,
            source_field,
            target_field: Some(target_field),
            transform_reason: Some(transform_reason),
        };
        if !finding.has_valid_shape() {
            return Err(InternalError::store_invariant());
        }
        Ok(finding)
    }

    fn has_valid_shape(&self) -> bool {
        self.entity.value() != 0
            && !self.primary_key.is_empty()
            && self.primary_key.len() <= MAX_SCHEMA_MIGRATION_CURSOR_BYTES
            && match self.kind {
                PersistedSchemaMigrationFindingKind::Transform => {
                    self.source_field.is_none_or(|field| field.get() != 0)
                        && self.target_field.is_some_and(|field| field.get() != 0)
                        && self.transform_reason.is_some()
                }
                PersistedSchemaMigrationFindingKind::UniqueIndex
                | PersistedSchemaMigrationFindingKind::Relation
                | PersistedSchemaMigrationFindingKind::Constraint => {
                    self.source_field.is_none()
                        && self.target_field.is_none()
                        && self.transform_reason.is_none()
                }
            }
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn kind(&self) -> PersistedSchemaMigrationFindingKind {
        self.kind
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn entity(&self) -> EntityTag {
        self.entity
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn primary_key(&self) -> &[u8] {
        self.primary_key.as_slice()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn source_field(&self) -> Option<FieldId> {
        self.source_field
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn target_field(&self) -> Option<FieldId> {
        self.target_field
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn transform_reason(
        &self,
    ) -> Option<PersistedSchemaMigrationTransformReason> {
        self.transform_reason
    }
}

/// Bounded durable progress inside one phase.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db::schema) struct PersistedSchemaMigrationProgress {
    row_cursor: Option<PersistedSchemaMigrationRowCursor>,
    index_cursor: Option<PersistedSchemaMigrationIndexCursor>,
    rows_validated: u64,
    rows_rewritten: u64,
    indexes_rebuilt: u32,
    finding_page: Option<u64>,
    findings: Vec<PersistedSchemaMigrationFinding>,
}

impl PersistedSchemaMigrationProgress {
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn begin_row_phase(&self) -> Result<Self, InternalError> {
        let progress = Self {
            row_cursor: None,
            index_cursor: self.index_cursor.clone(),
            rows_validated: self.rows_validated,
            rows_rewritten: self.rows_rewritten,
            indexes_rebuilt: self.indexes_rebuilt,
            finding_page: self.finding_page,
            findings: self.findings.clone(),
        };
        progress.validate()?;
        Ok(progress)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn with_rewrite_page(
        &self,
        row_cursor: Option<PersistedSchemaMigrationRowCursor>,
        rows_rewritten: u64,
    ) -> Result<Self, InternalError> {
        let progress = Self {
            row_cursor,
            index_cursor: self.index_cursor.clone(),
            rows_validated: self.rows_validated,
            rows_rewritten: self
                .rows_rewritten
                .checked_add(rows_rewritten)
                .ok_or_else(InternalError::store_invariant)?,
            indexes_rebuilt: self.indexes_rebuilt,
            finding_page: self.finding_page,
            findings: self.findings.clone(),
        };
        progress.validate()?;
        Ok(progress)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn with_index_progress(
        &self,
        index_cursor: Option<PersistedSchemaMigrationIndexCursor>,
        indexes_rebuilt: u32,
    ) -> Result<Self, InternalError> {
        let progress = Self {
            row_cursor: self.row_cursor.clone(),
            index_cursor,
            rows_validated: self.rows_validated,
            rows_rewritten: self.rows_rewritten,
            indexes_rebuilt: self
                .indexes_rebuilt
                .checked_add(indexes_rebuilt)
                .ok_or_else(InternalError::store_invariant)?,
            finding_page: self.finding_page,
            findings: self.findings.clone(),
        };
        progress.validate()?;
        Ok(progress)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn with_validation_page(
        &self,
        row_cursor: Option<PersistedSchemaMigrationRowCursor>,
        rows_validated: u64,
        findings: Vec<PersistedSchemaMigrationFinding>,
    ) -> Result<Self, InternalError> {
        if findings.len() > MAX_SCHEMA_MIGRATION_FINDINGS {
            return Err(InternalError::store_invariant());
        }
        let finding_page = if findings.is_empty() {
            None
        } else {
            Some(
                self.finding_page
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(InternalError::store_invariant)?,
            )
        };
        let progress = Self {
            row_cursor,
            index_cursor: self.index_cursor.clone(),
            rows_validated: self
                .rows_validated
                .checked_add(rows_validated)
                .ok_or_else(InternalError::store_invariant)?,
            rows_rewritten: self.rows_rewritten,
            indexes_rebuilt: self.indexes_rebuilt,
            finding_page,
            findings,
        };
        progress.validate()?;
        Ok(progress)
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn row_cursor(
        &self,
    ) -> Option<&PersistedSchemaMigrationRowCursor> {
        self.row_cursor.as_ref()
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn index_cursor(
        &self,
    ) -> Option<&PersistedSchemaMigrationIndexCursor> {
        self.index_cursor.as_ref()
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn finding_page(&self) -> Option<u64> {
        self.finding_page
    }
    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn rows_validated(&self) -> u64 {
        self.rows_validated
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn rows_rewritten(&self) -> u64 {
        self.rows_rewritten
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn indexes_rebuilt(&self) -> u32 {
        self.indexes_rebuilt
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn findings(&self) -> &[PersistedSchemaMigrationFinding] {
        self.findings.as_slice()
    }

    fn validate(&self) -> Result<(), InternalError> {
        if self.findings.len() > MAX_SCHEMA_MIGRATION_FINDINGS
            || self.row_cursor.as_ref().is_some_and(|cursor| {
                cursor.entity.value() == 0
                    || cursor.primary_key.is_empty()
                    || cursor.primary_key.len() > MAX_SCHEMA_MIGRATION_CURSOR_BYTES
            })
            || self.index_cursor.as_ref().is_some_and(|cursor| {
                cursor.entity.value() == 0
                    || cursor.index == 0
                    || cursor.key.is_empty()
                    || cursor.key.len() > MAX_SCHEMA_MIGRATION_CURSOR_BYTES
            })
            || self
                .findings
                .iter()
                .any(|finding| !finding.has_valid_shape())
            || self.finding_page.is_none() != self.findings.is_empty()
        {
            return Err(InternalError::store_invariant());
        }
        Ok(())
    }

    #[cfg(any(test, feature = "migration"))]
    fn monotonic_from(&self, before: &Self) -> bool {
        self.rows_validated >= before.rows_validated
            && self.rows_rewritten >= before.rows_rewritten
            && self.indexes_rebuilt >= before.indexes_rebuilt
            && self.finding_page >= before.finding_page
    }
}

/// Sole current durable migration lifecycle record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaMigrationRecord {
    database_identity: TargetDatabaseIdentity,
    accepted_before: ExpectedAcceptedHead,
    candidate_head: ExpectedAcceptedHead,
    submission_digest: SchemaProposalDigest,
    plan_digest: SchemaMigrationPlanDigest,
    phase: PersistedSchemaMigrationPhase,
    transitions: Vec<PersistedSchemaMigrationTransition>,
    entities: Vec<PersistedSchemaMigrationEntity>,
    staged_indexes: Vec<PersistedSchemaMigrationIndex>,
    progress: PersistedSchemaMigrationProgress,
}

impl SchemaMigrationRecord {
    #[expect(
        clippy::too_many_arguments,
        reason = "the constructor binds every immutable durable migration identity"
    )]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn prepared(
        database_identity: TargetDatabaseIdentity,
        accepted_before: ExpectedAcceptedHead,
        candidate_head: ExpectedAcceptedHead,
        submission_digest: SchemaProposalDigest,
        plan_digest: SchemaMigrationPlanDigest,
        transitions: Vec<PersistedSchemaMigrationTransition>,
        entities: Vec<PersistedSchemaMigrationEntity>,
        staged_indexes: Vec<PersistedSchemaMigrationIndex>,
    ) -> Result<Self, InternalError> {
        let record = Self {
            database_identity,
            accepted_before,
            candidate_head,
            submission_digest,
            plan_digest,
            phase: PersistedSchemaMigrationPhase::Prepared,
            transitions,
            entities,
            staged_indexes,
            progress: PersistedSchemaMigrationProgress::default(),
        };
        record.validate()?;
        Ok(record)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn transition(
        &self,
        phase: PersistedSchemaMigrationPhase,
        progress: PersistedSchemaMigrationProgress,
    ) -> Result<Self, InternalError> {
        if !self.phase.may_transition_to(phase) || !progress.monotonic_from(&self.progress) {
            return Err(InternalError::store_invariant());
        }
        let mut next = self.clone();
        next.phase = phase;
        next.progress = progress;
        next.validate()?;
        Ok(next)
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn accepted_before(&self) -> &ExpectedAcceptedHead {
        &self.accepted_before
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn candidate_head(&self) -> &ExpectedAcceptedHead {
        &self.candidate_head
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn submission_digest(&self) -> SchemaProposalDigest {
        self.submission_digest
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn plan_digest(&self) -> SchemaMigrationPlanDigest {
        self.plan_digest
    }

    #[must_use]
    pub(in crate::db::schema) const fn phase(&self) -> PersistedSchemaMigrationPhase {
        self.phase
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn transitions(&self) -> &[PersistedSchemaMigrationTransition] {
        self.transitions.as_slice()
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) const fn progress(&self) -> &PersistedSchemaMigrationProgress {
        &self.progress
    }

    /// Return whether one private row/index journal effect is bound to this
    /// exact resumable or terminal migration authority.
    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn permits_private_physical_journal(
        &self,
        plan_digest: SchemaMigrationPlanDigest,
    ) -> bool {
        self.plan_digest == plan_digest
            && matches!(
                self.phase,
                PersistedSchemaMigrationPhase::RewritingRows
                    | PersistedSchemaMigrationPhase::RebuildingIndexes
                    | PersistedSchemaMigrationPhase::FinalValidation
                    | PersistedSchemaMigrationPhase::Publishing
                    | PersistedSchemaMigrationPhase::Applied
            )
    }

    fn validate(&self) -> Result<(), InternalError> {
        validate_exact_head(&self.accepted_before)?;
        validate_exact_head(&self.candidate_head)?;
        if self.accepted_before == self.candidate_head
            || self.database_identity.to_bytes() == [0; 32]
            || self.submission_digest.to_bytes() == [0; 32]
            || self.plan_digest.to_bytes() == [0; 32]
            || self.transitions.is_empty()
            || self.transitions.len() > MAX_SCHEMA_MIGRATION_RECORD_ENTITIES
            || self.entities.is_empty()
            || self.entities.len() > MAX_SCHEMA_MIGRATION_RECORD_ENTITIES
            || self.staged_indexes.len() > MAX_SCHEMA_MIGRATION_RECORD_INDEXES
        {
            return Err(InternalError::store_invariant());
        }
        if !strictly_ordered(
            self.transitions
                .iter()
                .map(|transition| transition.entity.as_str()),
        ) || self.transitions.iter().any(|transition| {
            transition.from_version == 0
                || transition.from_version.checked_add(1) != Some(transition.to_version)
        }) || !strictly_ordered(
            self.entities
                .iter()
                .map(|entity| (entity.store, entity.entity)),
        ) || self
            .entities
            .iter()
            .any(|entity| entity.entity.value() == 0 || entity.source_digest.to_bytes() == [0; 32])
            || !strictly_ordered(
                self.staged_indexes
                    .iter()
                    .map(|index| (index.store, index.entity, index.index, index.generation)),
            )
            || self
                .staged_indexes
                .iter()
                .any(|index| index.entity.value() == 0 || index.index == 0 || index.generation == 0)
        {
            return Err(InternalError::store_invariant());
        }
        self.progress.validate()?;
        let _ = encode_schema_migration_record(self)?;
        Ok(())
    }
}

fn strictly_ordered<T: Ord>(values: impl IntoIterator<Item = T>) -> bool {
    let mut prior = None;
    for value in values {
        if prior.as_ref().is_some_and(|prior| prior >= &value) {
            return false;
        }
        prior = Some(value);
    }
    true
}

/// Exact compare-and-replace effect carried by a commit marker.
#[derive(Clone, Debug)]
#[cfg(any(test, feature = "migration"))]
pub(in crate::db) struct SchemaMigrationRecordOp {
    before: Option<Vec<u8>>,
    after: Vec<u8>,
}

#[cfg(any(test, feature = "migration"))]
impl SchemaMigrationRecordOp {
    pub(in crate::db::schema) fn insert(
        after: &SchemaMigrationRecord,
    ) -> Result<Self, InternalError> {
        Self::from_encoded(None, encode_schema_migration_record(after)?)
    }

    pub(in crate::db::schema) fn replace(
        before: &SchemaMigrationRecord,
        after: &SchemaMigrationRecord,
    ) -> Result<Self, InternalError> {
        Self::from_encoded(
            Some(encode_schema_migration_record(before)?),
            encode_schema_migration_record(after)?,
        )
    }

    pub(in crate::db) fn from_encoded(
        before: Option<Vec<u8>>,
        after: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let operation = Self { before, after };
        operation.validate()?;
        Ok(operation)
    }

    pub(in crate::db) fn before_bytes(&self) -> Option<&[u8]> {
        self.before.as_deref()
    }

    pub(in crate::db) const fn after_bytes(&self) -> &[u8] {
        self.after.as_slice()
    }

    pub(in crate::db) fn validate(&self) -> Result<(), InternalError> {
        let after = decode_schema_migration_record(&self.after)?;
        if let Some(before) = self.before.as_deref() {
            let before = decode_schema_migration_record(before)?;
            // The application owner drains the old journal before handing the
            // singleton to a fresh plan. This is an exact terminal CAS, not a
            // phase transition within the old plan or a reset of active work.
            if before.phase.terminal() && after.phase == PersistedSchemaMigrationPhase::Prepared {
                if before.database_identity != after.database_identity
                    || before.submission_digest == after.submission_digest
                    || after.progress != PersistedSchemaMigrationProgress::default()
                {
                    return Err(InternalError::store_corruption());
                }
                return Ok(());
            }
            if before == after
                || before.database_identity != after.database_identity
                || before.accepted_before != after.accepted_before
                || before.candidate_head != after.candidate_head
                || before.submission_digest != after.submission_digest
                || before.plan_digest != after.plan_digest
                || before.transitions != after.transitions
                || before.entities != after.entities
                || before.staged_indexes != after.staged_indexes
                || !before.phase.may_transition_to(after.phase)
                || !after.progress.monotonic_from(&before.progress)
            {
                return Err(InternalError::store_corruption());
            }
        } else if after.phase != PersistedSchemaMigrationPhase::Prepared {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

pub(in crate::db::schema) fn encode_schema_migration_record(
    record: &SchemaMigrationRecord,
) -> Result<Vec<u8>, InternalError> {
    let mut writer = MigrationWriter::new();
    writer.push_bytes(MIGRATION_MAGIC);
    writer.push_u8(MIGRATION_VERSION);
    writer.push_u32(0);
    writer.push_bytes(&record.database_identity.to_bytes());
    encode_head(&mut writer, &record.accepted_before)?;
    encode_head(&mut writer, &record.candidate_head)?;
    writer.push_bytes(&record.submission_digest.to_bytes());
    writer.push_bytes(&record.plan_digest.to_bytes());
    writer.push_u8(record.phase.tag());
    writer.push_len(record.transitions.len())?;
    for transition in &record.transitions {
        writer.push_string(transition.entity.as_str())?;
        writer.push_u32(transition.from_version);
        writer.push_u32(transition.to_version);
    }
    writer.push_len(record.entities.len())?;
    for entity in &record.entities {
        writer.push_bytes(&entity.store.to_bytes());
        writer.push_u64(entity.entity.value());
        writer.push_bytes(&entity.source_digest.to_bytes());
    }
    writer.push_len(record.staged_indexes.len())?;
    for index in &record.staged_indexes {
        writer.push_bytes(&index.store.to_bytes());
        writer.push_u64(index.entity.value());
        writer.push_u64(index.index);
        writer.push_u64(index.generation);
    }
    encode_progress(&mut writer, &record.progress)?;
    let mut encoded = writer.finish()?;
    let payload_len = encoded
        .len()
        .checked_sub(MIGRATION_HEADER_BYTES)
        .ok_or_else(InternalError::store_invariant)?;
    encoded[9..13].copy_from_slice(
        &u32::try_from(payload_len)
            .map_err(|_| InternalError::store_unsupported())?
            .to_be_bytes(),
    );
    if encoded.len() > MAX_SCHEMA_MIGRATION_RECORD_BYTES.saturating_sub(MIGRATION_CHECKSUM_BYTES) {
        return Err(InternalError::store_unsupported());
    }
    encoded.extend_from_slice(&crc32c(&encoded).to_be_bytes());
    Ok(encoded)
}

pub(in crate::db::schema) fn decode_schema_migration_record(
    bytes: &[u8],
) -> Result<SchemaMigrationRecord, InternalError> {
    if bytes.len() < MIGRATION_HEADER_BYTES + MIGRATION_CHECKSUM_BYTES
        || bytes.len() > MAX_SCHEMA_MIGRATION_RECORD_BYTES
    {
        return Err(InternalError::store_corruption());
    }
    let checksum_offset = bytes
        .len()
        .checked_sub(MIGRATION_CHECKSUM_BYTES)
        .ok_or_else(InternalError::store_corruption)?;
    let (body, checksum) = bytes.split_at(checksum_offset);
    if crc32c(body)
        != u32::from_be_bytes(
            checksum
                .try_into()
                .map_err(|_| InternalError::store_corruption())?,
        )
    {
        return Err(InternalError::store_corruption());
    }
    let mut reader = MigrationReader::new(body);
    if reader.read_array::<8>()? != *MIGRATION_MAGIC || reader.read_u8()? != MIGRATION_VERSION {
        return Err(InternalError::store_corruption());
    }
    let payload_len = reader.read_u32()? as usize;
    if payload_len != body.len().saturating_sub(MIGRATION_HEADER_BYTES) {
        return Err(InternalError::store_corruption());
    }
    let database_identity = TargetDatabaseIdentity::from_bytes(reader.read_array()?);
    let accepted_before = decode_head(&mut reader)?;
    let candidate_head = decode_head(&mut reader)?;
    let submission_digest = SchemaProposalDigest::from_bytes(reader.read_array()?);
    let plan_digest = SchemaMigrationPlanDigest::from_bytes(reader.read_array()?);
    let phase = PersistedSchemaMigrationPhase::from_tag(reader.read_u8()?)?;
    let transition_count = bounded_count(&mut reader, MAX_SCHEMA_MIGRATION_RECORD_ENTITIES)?;
    let mut transitions = Vec::new();
    transitions
        .try_reserve_exact(transition_count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..transition_count {
        transitions.push(
            PersistedSchemaMigrationTransition::try_new(
                EntitySourceKey::try_new(reader.read_string()?)
                    .map_err(|_| InternalError::store_corruption())?,
                reader.read_u32()?,
                reader.read_u32()?,
            )
            .map_err(|_| InternalError::store_corruption())?,
        );
    }
    let entity_count = bounded_count(&mut reader, MAX_SCHEMA_MIGRATION_RECORD_ENTITIES)?;
    let mut entities = Vec::new();
    entities
        .try_reserve_exact(entity_count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..entity_count {
        entities.push(
            PersistedSchemaMigrationEntity::try_new(
                TargetStoreIdentity::from_bytes(reader.read_array()?),
                EntityTag::new(reader.read_u64()?),
                EntitySourceDigest::from_bytes(reader.read_array()?),
            )
            .map_err(|_| InternalError::store_corruption())?,
        );
    }
    let index_count = bounded_count(&mut reader, MAX_SCHEMA_MIGRATION_RECORD_INDEXES)?;
    let mut staged_indexes = Vec::new();
    staged_indexes
        .try_reserve_exact(index_count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..index_count {
        staged_indexes.push(PersistedSchemaMigrationIndex {
            store: TargetStoreIdentity::from_bytes(reader.read_array()?),
            entity: EntityTag::new(reader.read_u64()?),
            index: reader.read_u64()?,
            generation: reader.read_u64()?,
        });
    }
    let progress = decode_progress(&mut reader)?;
    reader.finish()?;
    let record = SchemaMigrationRecord {
        database_identity,
        accepted_before,
        candidate_head,
        submission_digest,
        plan_digest,
        phase,
        transitions,
        entities,
        staged_indexes,
        progress,
    };
    record
        .validate()
        .map_err(|_| InternalError::store_corruption())?;
    if encode_schema_migration_record(&record)? != bytes {
        return Err(InternalError::store_corruption());
    }
    Ok(record)
}

fn bounded_count(reader: &mut MigrationReader<'_>, maximum: usize) -> Result<usize, InternalError> {
    let count = reader.read_u32()? as usize;
    if count > maximum {
        return Err(InternalError::store_corruption());
    }
    Ok(count)
}

fn encode_head(
    writer: &mut MigrationWriter,
    head: &ExpectedAcceptedHead,
) -> Result<(), InternalError> {
    let ExpectedAcceptedHead::Exact {
        revision,
        fingerprint,
    } = head
    else {
        return Err(InternalError::store_invariant());
    };
    writer.push_u64(*revision);
    writer.push_bytes(&fingerprint.to_bytes());
    Ok(())
}

fn decode_head(reader: &mut MigrationReader<'_>) -> Result<ExpectedAcceptedHead, InternalError> {
    let head = ExpectedAcceptedHead::Exact {
        revision: reader.read_u64()?,
        fingerprint: ExpectedSchemaFingerprint::from_bytes(reader.read_array()?),
    };
    validate_exact_head(&head).map_err(|_| InternalError::store_corruption())?;
    Ok(head)
}

fn validate_exact_head(head: &ExpectedAcceptedHead) -> Result<(), InternalError> {
    match head {
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } if *revision > 0 && fingerprint.to_bytes() != [0; 32] => Ok(()),
        ExpectedAcceptedHead::Empty | ExpectedAcceptedHead::Exact { .. } => {
            Err(InternalError::store_invariant())
        }
    }
}

fn encode_progress(
    writer: &mut MigrationWriter,
    progress: &PersistedSchemaMigrationProgress,
) -> Result<(), InternalError> {
    encode_row_cursor(writer, progress.row_cursor.as_ref())?;
    encode_index_cursor(writer, progress.index_cursor.as_ref())?;
    writer.push_u64(progress.rows_validated);
    writer.push_u64(progress.rows_rewritten);
    writer.push_u32(progress.indexes_rebuilt);
    match progress.finding_page {
        None => writer.push_u8(0),
        Some(sequence) => {
            writer.push_u8(1);
            writer.push_u64(sequence);
        }
    }
    writer.push_len(progress.findings.len())?;
    for finding in &progress.findings {
        writer.push_u8(match finding.kind {
            PersistedSchemaMigrationFindingKind::Transform => 0,
            PersistedSchemaMigrationFindingKind::UniqueIndex => 1,
            PersistedSchemaMigrationFindingKind::Relation => 2,
            PersistedSchemaMigrationFindingKind::Constraint => 3,
        });
        writer.push_bytes(&finding.store.to_bytes());
        writer.push_u64(finding.entity.value());
        writer.push_len_prefixed_bytes(&finding.primary_key)?;
        if finding.kind == PersistedSchemaMigrationFindingKind::Transform {
            match finding.source_field {
                None => writer.push_u8(0),
                Some(field) => {
                    writer.push_u8(1);
                    writer.push_u32(field.get());
                }
            }
            writer.push_u32(
                finding
                    .target_field
                    .ok_or_else(InternalError::store_invariant)?
                    .get(),
            );
            writer.push_u8(
                match finding
                    .transform_reason
                    .ok_or_else(InternalError::store_invariant)?
                {
                    PersistedSchemaMigrationTransformReason::Overflow => 0,
                    PersistedSchemaMigrationTransformReason::NegativeToUnsigned => 1,
                    PersistedSchemaMigrationTransformReason::PrecisionLoss => 2,
                    PersistedSchemaMigrationTransformReason::NullSource => 3,
                    PersistedSchemaMigrationTransformReason::ValueContract => 4,
                },
            );
        }
    }
    Ok(())
}

fn decode_progress(
    reader: &mut MigrationReader<'_>,
) -> Result<PersistedSchemaMigrationProgress, InternalError> {
    let row_cursor = decode_row_cursor(reader)?;
    let index_cursor = decode_index_cursor(reader)?;
    let rows_validated = reader.read_u64()?;
    let rows_rewritten = reader.read_u64()?;
    let indexes_rebuilt = reader.read_u32()?;
    let finding_page = match reader.read_u8()? {
        0 => None,
        1 => Some(reader.read_u64()?),
        _ => return Err(InternalError::store_corruption()),
    };
    let count = bounded_count(reader, MAX_SCHEMA_MIGRATION_FINDINGS)?;
    let mut findings = Vec::new();
    findings
        .try_reserve_exact(count)
        .map_err(|_| InternalError::store_corruption())?;
    for _ in 0..count {
        let kind = match reader.read_u8()? {
            0 => PersistedSchemaMigrationFindingKind::Transform,
            1 => PersistedSchemaMigrationFindingKind::UniqueIndex,
            2 => PersistedSchemaMigrationFindingKind::Relation,
            3 => PersistedSchemaMigrationFindingKind::Constraint,
            _ => return Err(InternalError::store_corruption()),
        };
        let store = TargetStoreIdentity::from_bytes(reader.read_array()?);
        let entity = EntityTag::new(reader.read_u64()?);
        let primary_key = reader.read_len_prefixed_bytes()?.to_vec();
        let (source_field, target_field, transform_reason) =
            if kind == PersistedSchemaMigrationFindingKind::Transform {
                let source_field = match reader.read_u8()? {
                    0 => None,
                    1 => FieldId::new(reader.read_u32()?).into(),
                    _ => return Err(InternalError::store_corruption()),
                };
                let target_field = Some(FieldId::new(reader.read_u32()?));
                let reason = match reader.read_u8()? {
                    0 => PersistedSchemaMigrationTransformReason::Overflow,
                    1 => PersistedSchemaMigrationTransformReason::NegativeToUnsigned,
                    2 => PersistedSchemaMigrationTransformReason::PrecisionLoss,
                    3 => PersistedSchemaMigrationTransformReason::NullSource,
                    4 => PersistedSchemaMigrationTransformReason::ValueContract,
                    _ => return Err(InternalError::store_corruption()),
                };
                (source_field, target_field, Some(reason))
            } else {
                (None, None, None)
            };
        findings.push(PersistedSchemaMigrationFinding {
            kind,
            store,
            entity,
            primary_key,
            source_field,
            target_field,
            transform_reason,
        });
    }
    let progress = PersistedSchemaMigrationProgress {
        row_cursor,
        index_cursor,
        rows_validated,
        rows_rewritten,
        indexes_rebuilt,
        finding_page,
        findings,
    };
    progress
        .validate()
        .map_err(|_| InternalError::store_corruption())?;
    Ok(progress)
}

fn encode_row_cursor(
    writer: &mut MigrationWriter,
    cursor: Option<&PersistedSchemaMigrationRowCursor>,
) -> Result<(), InternalError> {
    match cursor {
        None => writer.push_u8(0),
        Some(cursor) => {
            writer.push_u8(1);
            writer.push_bytes(&cursor.store.to_bytes());
            writer.push_u64(cursor.entity.value());
            writer.push_len_prefixed_bytes(&cursor.primary_key)?;
        }
    }
    Ok(())
}

fn decode_row_cursor(
    reader: &mut MigrationReader<'_>,
) -> Result<Option<PersistedSchemaMigrationRowCursor>, InternalError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(PersistedSchemaMigrationRowCursor {
            store: TargetStoreIdentity::from_bytes(reader.read_array()?),
            entity: EntityTag::new(reader.read_u64()?),
            primary_key: reader.read_len_prefixed_bytes()?.to_vec(),
        })),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_index_cursor(
    writer: &mut MigrationWriter,
    cursor: Option<&PersistedSchemaMigrationIndexCursor>,
) -> Result<(), InternalError> {
    match cursor {
        None => writer.push_u8(0),
        Some(cursor) => {
            writer.push_u8(1);
            writer.push_bytes(&cursor.store.to_bytes());
            writer.push_u64(cursor.entity.value());
            writer.push_u64(cursor.index);
            writer.push_len_prefixed_bytes(&cursor.key)?;
        }
    }
    Ok(())
}

fn decode_index_cursor(
    reader: &mut MigrationReader<'_>,
) -> Result<Option<PersistedSchemaMigrationIndexCursor>, InternalError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(PersistedSchemaMigrationIndexCursor {
            store: TargetStoreIdentity::from_bytes(reader.read_array()?),
            entity: EntityTag::new(reader.read_u64()?),
            index: reader.read_u64()?,
            key: reader.read_len_prefixed_bytes()?.to_vec(),
        })),
        _ => Err(InternalError::store_corruption()),
    }
}

#[cfg(test)]
pub(in crate::db) fn prepared_schema_migration_record_op_for_tests()
-> Result<SchemaMigrationRecordOp, InternalError> {
    Ok(schema_migration_record_lifecycle_ops_for_tests()?.0)
}

#[cfg(test)]
pub(in crate::db) fn schema_migration_record_lifecycle_ops_for_tests() -> Result<
    (
        SchemaMigrationRecordOp,
        SchemaMigrationRecordOp,
        SchemaMigrationRecordOp,
    ),
    InternalError,
> {
    let record = SchemaMigrationRecord::prepared(
        TargetDatabaseIdentity::from_bytes([0x91; 32]),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x92; 32]),
        },
        ExpectedAcceptedHead::Exact {
            revision: 2,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x93; 32]),
        },
        SchemaProposalDigest::from_bytes([0x94; 32]),
        SchemaMigrationPlanDigest::from_bytes([0x95; 32]),
        vec![PersistedSchemaMigrationTransition::try_new(
            EntitySourceKey::try_new("RecoveryEntity")
                .map_err(|_| InternalError::store_invariant())?,
            1,
            2,
        )?],
        vec![PersistedSchemaMigrationEntity::try_new(
            TargetStoreIdentity::from_bytes([0x96; 32]),
            EntityTag::new(1),
            EntitySourceDigest::from_bytes([0x97; 32]),
        )?],
        Vec::new(),
    )?;
    let validating = record.transition(
        PersistedSchemaMigrationPhase::Validating,
        PersistedSchemaMigrationProgress::default(),
    )?;
    let aborted = validating.transition(
        PersistedSchemaMigrationPhase::Aborted,
        PersistedSchemaMigrationProgress::default(),
    )?;
    Ok((
        SchemaMigrationRecordOp::insert(&record)?,
        SchemaMigrationRecordOp::replace(&record, &validating)?,
        SchemaMigrationRecordOp::replace(&validating, &aborted)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::commit::{
        CommitMarker, DatabaseControlOp, decode_commit_marker_payload, encode_commit_marker_payload,
    };

    fn record() -> SchemaMigrationRecord {
        SchemaMigrationRecord::prepared(
            TargetDatabaseIdentity::from_bytes([1; 32]),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([2; 32]),
            },
            ExpectedAcceptedHead::Exact {
                revision: 2,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
            },
            SchemaProposalDigest::from_bytes([4; 32]),
            SchemaMigrationPlanDigest::from_bytes([5; 32]),
            vec![
                PersistedSchemaMigrationTransition::try_new(
                    EntitySourceKey::try_new("User").expect("entity source should admit"),
                    1,
                    2,
                )
                .expect("transition should admit"),
            ],
            vec![
                PersistedSchemaMigrationEntity::try_new(
                    TargetStoreIdentity::from_bytes([6; 32]),
                    EntityTag::new(7),
                    EntitySourceDigest::from_bytes([8; 32]),
                )
                .expect("entity identity should admit"),
            ],
            Vec::new(),
        )
        .expect("record should admit")
    }

    #[test]
    fn migration_record_codec_is_canonical_bounded_and_checksum_bound() {
        let record = record();
        let encoded = encode_schema_migration_record(&record).expect("record should encode");
        assert_eq!(
            decode_schema_migration_record(&encoded).expect("record should decode"),
            record,
        );

        let mut corrupted = encoded;
        *corrupted.last_mut().expect("record has checksum") ^= 0x80;
        assert!(decode_schema_migration_record(&corrupted).is_err());
        assert!(
            decode_schema_migration_record(&vec![0; MAX_SCHEMA_MIGRATION_RECORD_BYTES + 1])
                .is_err()
        );
    }

    #[test]
    fn migration_record_codec_retains_exact_transform_finding_identity() {
        let prepared = record();
        let finding = PersistedSchemaMigrationFinding::try_new_transform(
            TargetStoreIdentity::from_bytes([6; 32]),
            EntityTag::new(7),
            vec![0x81, 0x02],
            Some(FieldId::new(2)),
            FieldId::new(3),
            PersistedSchemaMigrationTransformReason::NegativeToUnsigned,
        )
        .expect("transform finding should admit");
        let progress = PersistedSchemaMigrationProgress::default()
            .with_validation_page(
                Some(
                    PersistedSchemaMigrationRowCursor::try_new(
                        TargetStoreIdentity::from_bytes([6; 32]),
                        EntityTag::new(7),
                        vec![0x81, 0x02],
                    )
                    .expect("cursor should admit"),
                ),
                1,
                vec![finding],
            )
            .expect("progress should admit");
        let rejected = prepared
            .transition(PersistedSchemaMigrationPhase::Rejected, progress)
            .expect("prepared validation may reject");
        let encoded = encode_schema_migration_record(&rejected).expect("record should encode");
        let decoded = decode_schema_migration_record(&encoded).expect("record should decode");
        let [decoded_finding] = decoded.progress().findings() else {
            panic!("one transform finding should survive");
        };
        assert_eq!(decoded_finding.source_field(), Some(FieldId::new(2)));
        assert_eq!(decoded_finding.target_field(), Some(FieldId::new(3)));
        assert_eq!(
            decoded_finding.transform_reason(),
            Some(PersistedSchemaMigrationTransformReason::NegativeToUnsigned),
        );
    }

    #[test]
    fn migration_phase_machine_is_closed_and_abort_boundary_is_exact() {
        let prepared = record();
        let validating = prepared
            .transition(
                PersistedSchemaMigrationPhase::Validating,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validation should start");
        assert!(validating.phase().blocks_ordinary_operations());
        assert!(validating.phase().abortable());
        let aborted = validating
            .transition(
                PersistedSchemaMigrationPhase::Aborted,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validation should remain abortable");
        assert!(aborted.phase().terminal());
        assert!(
            validating
                .transition(
                    PersistedSchemaMigrationPhase::RewritingRows,
                    PersistedSchemaMigrationProgress::default(),
                )
                .is_err(),
            "validation cannot skip the ready boundary",
        );
        let ready = validating
            .transition(
                PersistedSchemaMigrationPhase::ReadyToRewrite,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validated migration should become ready");
        let rewriting = ready
            .transition(
                PersistedSchemaMigrationPhase::RewritingRows,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("rewrite should start once ready");
        assert!(!rewriting.phase().abortable());
        assert!(
            rewriting
                .transition(
                    PersistedSchemaMigrationPhase::Aborted,
                    PersistedSchemaMigrationProgress::default(),
                )
                .is_err(),
        );
    }

    #[test]
    fn migration_record_operation_binds_immutable_identity_and_monotonic_phase() {
        let prepared = record();
        let validating = prepared
            .transition(
                PersistedSchemaMigrationPhase::Validating,
                PersistedSchemaMigrationProgress::default(),
            )
            .expect("validation should start");
        SchemaMigrationRecordOp::insert(&prepared).expect("prepare insert should admit");
        SchemaMigrationRecordOp::replace(&prepared, &validating)
            .expect("phase replacement should admit");
        assert!(SchemaMigrationRecordOp::insert(&validating).is_err());
        assert!(SchemaMigrationRecordOp::replace(&validating, &prepared).is_err());
    }

    #[test]
    fn terminal_handoff_record_rejects_active_reset_wrong_database_and_carried_progress() {
        let original = record();
        let mut next = original.clone();
        next.submission_digest = SchemaProposalDigest::from_bytes([0x71; 32]);
        for phase in [
            PersistedSchemaMigrationPhase::Applied,
            PersistedSchemaMigrationPhase::Aborted,
        ] {
            let mut terminal = original.clone();
            terminal.phase = phase;
            let operation = SchemaMigrationRecordOp::replace(&terminal, &next)
                .expect("fresh terminal handoff should admit");
            let marker = CommitMarker::from_parts_with_database_control(
                [0x72; 16],
                Vec::new(),
                vec![DatabaseControlOp::SchemaMigration(operation)],
            )
            .expect("handoff marker should admit");
            let encoded =
                encode_commit_marker_payload(&marker).expect("handoff marker should encode");
            decode_commit_marker_payload(&encoded).expect("handoff marker should decode");
            let mut invalid = next.clone();
            invalid.database_identity = TargetDatabaseIdentity::from_bytes([0x73; 32]);
            assert!(SchemaMigrationRecordOp::replace(&terminal, &invalid).is_err());
            invalid = next.clone();
            invalid.progress.rows_validated = 1;
            assert!(SchemaMigrationRecordOp::replace(&terminal, &invalid).is_err());
            assert!(SchemaMigrationRecordOp::replace(&terminal, &original).is_err());
        }
        assert!(SchemaMigrationRecordOp::replace(&original, &next).is_err());
    }

    #[test]
    fn migration_record_operation_round_trips_inside_current_marker() {
        let prepared = record();
        let operation = SchemaMigrationRecordOp::insert(&prepared)
            .expect("prepared migration insertion should admit");
        let marker = CommitMarker::from_parts_with_database_control(
            [0x41; 16],
            Vec::new(),
            vec![DatabaseControlOp::SchemaMigration(operation.clone())],
        )
        .expect("migration marker should admit");
        let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
        let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
        let [DatabaseControlOp::SchemaMigration(decoded)] = decoded.database_control() else {
            panic!("marker should retain exactly one migration operation");
        };
        assert_eq!(decoded.before_bytes(), operation.before_bytes());
        assert_eq!(decoded.after_bytes(), operation.after_bytes());
    }
}
