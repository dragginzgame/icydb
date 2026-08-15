//! Module: db::journal::codec
//! Responsibility: bounded/fallible journal batch encoding.
//! Does not own: journal-tail storage, commit marker lifecycle, recovery, or fold.
//! Boundary: logical journal records -> stable-memory journal batch bytes.

use crate::db::index::{IndexKey, IndexKeyKind, RawIndexStoreKey};
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::{CommitSchemaFingerprint, MAX_COMMIT_BYTES},
        data::{DecodedDataStoreKey, RawDataStoreKey},
        integrity::DatabaseIncarnationId,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, ConstraintId, ConstraintValidationJob,
            FieldId, IdentityRangeAdvance, IdentityStateOwner, MAX_CONSTRAINT_VALIDATION_JOB_BYTES,
            MAX_SCHEMA_SNAPSHOT_BYTES, decode_constraint_validation_job,
            encode_constraint_validation_job,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use ic_stable_structures::{Storable, storable::Bound};
#[cfg(any(test, feature = "migration"))]
use icydb_schema::SchemaMigrationPlanDigest;
use std::{borrow::Cow, collections::BTreeSet};

pub(in crate::db) const JOURNAL_BATCH_FORMAT_VERSION_CURRENT: u8 = 1;
pub(in crate::db) const MAX_JOURNAL_BATCH_BYTES: u32 = MAX_COMMIT_BYTES;
pub(in crate::db) const MAX_JOURNAL_BATCH_RECORDS: usize = 16 * 1024;
const MAX_JOURNAL_PATH_BYTES: usize = 4 * 1024;
const JOURNAL_BATCH_MAGIC: [u8; 4] = *b"IJBT";
const JOURNAL_BATCH_HEADER_BYTES: usize = 9;
const JOURNAL_BATCH_ID_BYTES: usize = 16;
const JOURNAL_COMMIT_MARKER_ID_BYTES: usize = 16;
const JOURNAL_BATCH_FIXED_HEADER_BYTES: usize = JOURNAL_BATCH_HEADER_BYTES
    + JOURNAL_BATCH_ID_BYTES
    + JOURNAL_COMMIT_MARKER_ID_BYTES
    + size_of::<u64>()
    + size_of::<u32>();
const JOURNAL_SCHEMA_FINGERPRINT_BYTES: usize = 16;
const JOURNAL_RECORD_ROW_PUT: u8 = 1;
const JOURNAL_RECORD_ROW_DELETE: u8 = 2;
const JOURNAL_RECORD_SCHEMA_PUT: u8 = 3;
const JOURNAL_RECORD_ACCEPTED_SCHEMA_PUBLISH: u8 = 4;
const JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_PUT: u8 = 5;
const JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_DELETE: u8 = 6;
const JOURNAL_RECORD_IDENTITY_RANGE_ADVANCE: u8 = 7;
const JOURNAL_RECORD_CONSTRAINT_VALIDATION_INDEX_PUT: u8 = 8;
#[cfg(any(test, feature = "migration"))]
const JOURNAL_RECORD_SCHEMA_MIGRATION_ROW_PUT: u8 = 9;
#[cfg(any(test, feature = "migration"))]
const JOURNAL_RECORD_SCHEMA_MIGRATION_INDEX_PUT: u8 = 10;
const JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_DELETE: u8 = 11;
const JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_PUT: u8 = 12;
pub(in crate::db) const MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD: usize = 64;

pub(in crate::db) type JournalBatchId = [u8; JOURNAL_BATCH_ID_BYTES];
pub(in crate::db) type JournalCommitMarkerId = [u8; JOURNAL_COMMIT_MARKER_ID_BYTES];

/// Durable replay order for one complete marker-bound journal batch.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct JournalSequence(u64);

impl JournalSequence {
    #[must_use]
    pub(in crate::db) const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub(in crate::db) const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub(in crate::db) const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl Storable for JournalSequence {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.0.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        debug_assert_eq!(
            bytes.len(),
            size_of::<u64>(),
            "JournalSequence::from_bytes received unexpected byte length",
        );

        let mut out = [0u8; size_of::<u64>()];
        if bytes.len() == size_of::<u64>() {
            out.copy_from_slice(bytes.as_ref());
        }

        Self(u64::from_be_bytes(out))
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: 8,
        is_fixed_size: true,
    };
}

/// Logical journal record. Ordinary index entries remain derived materialized
/// state. Migration-private entries are retained only while predecessor
/// authority is gated, so their exact marker-bound effects are explicit.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum JournalRecord {
    /// Persisted row upsert for one entity and schema fingerprint.
    RowPut {
        entity_path: String,
        primary_key: RawDataStoreKey,
        row_bytes: Vec<u8>,
        schema_fingerprint: CommitSchemaFingerprint,
    },
    /// Persisted row delete for one entity and schema fingerprint.
    RowDelete {
        entity_path: String,
        primary_key: RawDataStoreKey,
        schema_fingerprint: CommitSchemaFingerprint,
    },
    /// Persisted schema snapshot update for one store.
    SchemaPut {
        store_path: String,
        schema_snapshot_bytes: Vec<u8>,
    },
    /// Atomic accepted-schema bundle and root publication for one store.
    AcceptedSchemaPublish {
        store_path: String,
        expected_revision: AcceptedSchemaRevision,
        schema_bundle_bytes: Vec<u8>,
        schema_root_bytes: Vec<u8>,
    },
    /// One bounded chunk removed from the user-index domain paired with the
    /// leading accepted-schema publication in this batch.
    AcceptedSchemaIndexDelete {
        store_path: String,
        entity_tag: EntityTag,
        accepted_after_fingerprint: CommitSchemaFingerprint,
        keys: Vec<RawIndexStoreKey>,
    },
    /// One bounded chunk inserted into the user-index domain paired with the
    /// leading accepted-schema publication in this batch.
    AcceptedSchemaIndexPut {
        store_path: String,
        entity_tag: EntityTag,
        accepted_after_fingerprint: CommitSchemaFingerprint,
        keys: Vec<RawIndexStoreKey>,
    },
    /// Schema-owned validation-job replacement for one live activation.
    ConstraintValidationJobPut {
        store_path: String,
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
        job_bytes: Vec<u8>,
    },
    /// Schema-owned validation-job removal for one promoted or aborted activation.
    ConstraintValidationJobDelete {
        store_path: String,
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
    },
    /// One isolated candidate-index entry owned by the paired validation job.
    ConstraintValidationIndexPut {
        store_path: String,
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
        key: RawIndexStoreKey,
    },
    /// One contiguous marker-owned advance for an accepted Identity owner.
    IdentityRangeAdvance { range: IdentityRangeAdvance },
    /// Candidate-layout row replacement owned by one exact migration plan.
    #[cfg(any(test, feature = "migration"))]
    SchemaMigrationRowPut {
        store_path: String,
        primary_key: RawDataStoreKey,
        row_bytes: Vec<u8>,
        schema_fingerprint: CommitSchemaFingerprint,
        plan_digest: SchemaMigrationPlanDigest,
    },
    /// Planner-invisible candidate index entry owned by one exact migration plan.
    #[cfg(any(test, feature = "migration"))]
    SchemaMigrationIndexPut {
        store_path: String,
        key: crate::db::index::RawIndexStoreKey,
        plan_digest: SchemaMigrationPlanDigest,
    },
}

impl JournalRecord {
    pub(in crate::db) fn row_put(
        entity_path: impl Into<String>,
        primary_key: RawDataStoreKey,
        row_bytes: Vec<u8>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        let record = Self::RowPut {
            entity_path: entity_path.into(),
            primary_key,
            row_bytes,
            schema_fingerprint,
        };
        validate_journal_record(&record)?;

        Ok(record)
    }

    pub(in crate::db) fn row_delete(
        entity_path: impl Into<String>,
        primary_key: RawDataStoreKey,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        let record = Self::RowDelete {
            entity_path: entity_path.into(),
            primary_key,
            schema_fingerprint,
        };
        validate_journal_record(&record)?;

        Ok(record)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn schema_migration_row_put(
        store_path: impl Into<String>,
        primary_key: RawDataStoreKey,
        row_bytes: Vec<u8>,
        schema_fingerprint: CommitSchemaFingerprint,
        plan_digest: SchemaMigrationPlanDigest,
    ) -> Result<Self, InternalError> {
        let record = Self::SchemaMigrationRowPut {
            store_path: store_path.into(),
            primary_key,
            row_bytes,
            schema_fingerprint,
            plan_digest,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn schema_migration_index_put(
        store_path: impl Into<String>,
        key: crate::db::index::RawIndexStoreKey,
        plan_digest: SchemaMigrationPlanDigest,
    ) -> Result<Self, InternalError> {
        let record = Self::SchemaMigrationIndexPut {
            store_path: store_path.into(),
            key,
            plan_digest,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn schema_put(
        store_path: impl Into<String>,
        schema_snapshot_bytes: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let record = Self::SchemaPut {
            store_path: store_path.into(),
            schema_snapshot_bytes,
        };
        validate_journal_record(&record)?;

        Ok(record)
    }

    pub(in crate::db) fn accepted_schema_publish(
        store_path: impl Into<String>,
        expected_revision: AcceptedSchemaRevision,
        schema_bundle_bytes: Vec<u8>,
        schema_root_bytes: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let record = Self::AcceptedSchemaPublish {
            store_path: store_path.into(),
            expected_revision,
            schema_bundle_bytes,
            schema_root_bytes,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn accepted_schema_index_delete(
        store_path: impl Into<String>,
        entity_tag: EntityTag,
        accepted_after_fingerprint: CommitSchemaFingerprint,
        keys: Vec<RawIndexStoreKey>,
    ) -> Result<Self, InternalError> {
        let record = Self::AcceptedSchemaIndexDelete {
            store_path: store_path.into(),
            entity_tag,
            accepted_after_fingerprint,
            keys,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn accepted_schema_index_put(
        store_path: impl Into<String>,
        entity_tag: EntityTag,
        accepted_after_fingerprint: CommitSchemaFingerprint,
        keys: Vec<RawIndexStoreKey>,
    ) -> Result<Self, InternalError> {
        let record = Self::AcceptedSchemaIndexPut {
            store_path: store_path.into(),
            entity_tag,
            accepted_after_fingerprint,
            keys,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn constraint_validation_job_put(
        store_path: impl Into<String>,
        job: &ConstraintValidationJob,
    ) -> Result<Self, InternalError> {
        let record = Self::ConstraintValidationJobPut {
            store_path: store_path.into(),
            entity_tag: job.entity_tag(),
            constraint_id: job.constraint_id(),
            job_bytes: encode_constraint_validation_job(job)?,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn constraint_validation_job_delete(
        store_path: impl Into<String>,
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
    ) -> Result<Self, InternalError> {
        let record = Self::ConstraintValidationJobDelete {
            store_path: store_path.into(),
            entity_tag,
            constraint_id,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn constraint_validation_index_put(
        store_path: impl Into<String>,
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
        key: RawIndexStoreKey,
    ) -> Result<Self, InternalError> {
        let record = Self::ConstraintValidationIndexPut {
            store_path: store_path.into(),
            entity_tag,
            constraint_id,
            key,
        };
        validate_journal_record(&record)?;
        Ok(record)
    }

    pub(in crate::db) fn identity_range_advance(
        range: IdentityRangeAdvance,
    ) -> Result<Self, InternalError> {
        let record = Self::IdentityRangeAdvance { range };
        validate_journal_record(&record)?;
        Ok(record)
    }
}

/// One complete marker-bound journal batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalBatch {
    batch_id: JournalBatchId,
    commit_marker_id: JournalCommitMarkerId,
    journal_sequence: JournalSequence,
    records: Vec<JournalRecord>,
}

impl JournalBatch {
    pub(in crate::db) fn new(
        batch_id: JournalBatchId,
        commit_marker_id: JournalCommitMarkerId,
        journal_sequence: JournalSequence,
        records: Vec<JournalRecord>,
    ) -> Result<Self, InternalError> {
        let _ = journal_sequence
            .next()
            .ok_or_else(InternalError::journal_mutation_revision_exhausted)?;
        let batch = Self {
            batch_id,
            commit_marker_id,
            journal_sequence,
            records,
        };
        validate_journal_batch_shape(&batch)?;

        Ok(batch)
    }

    #[must_use]
    pub(in crate::db) const fn batch_id(&self) -> JournalBatchId {
        self.batch_id
    }

    #[must_use]
    pub(in crate::db) const fn commit_marker_id(&self) -> JournalCommitMarkerId {
        self.commit_marker_id
    }

    #[must_use]
    pub(in crate::db) const fn journal_sequence(&self) -> JournalSequence {
        self.journal_sequence
    }

    #[must_use]
    pub(in crate::db) fn records(&self) -> &[JournalRecord] {
        &self.records
    }
}

/// Raw encoded journal batch bytes stored in the journal tail.
///
/// Owns the persisted byte envelope and validates only when decoded through the
/// journal codec boundary.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::journal) struct RawJournalBatch(Vec<u8>);

impl RawJournalBatch {
    pub(in crate::db::journal) fn from_batch(batch: &JournalBatch) -> Result<Self, InternalError> {
        encode_journal_batch(batch).map(Self)
    }

    pub(in crate::db::journal) const fn from_control_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub(in crate::db::journal) fn decode(&self) -> Result<JournalBatch, InternalError> {
        decode_journal_batch(self.as_bytes())
    }

    #[must_use]
    pub(in crate::db::journal) const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

pub(in crate::db) fn encode_journal_batch(batch: &JournalBatch) -> Result<Vec<u8>, InternalError> {
    validate_journal_batch_shape(batch)?;

    let payload_len = journal_batch_payload_len(batch);
    let total_len = JOURNAL_BATCH_HEADER_BYTES.saturating_add(payload_len);
    if total_len > MAX_JOURNAL_BATCH_BYTES as usize {
        return Err(InternalError::store_unsupported());
    }

    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&JOURNAL_BATCH_MAGIC);
    encoded.push(JOURNAL_BATCH_FORMAT_VERSION_CURRENT);
    write_len_u32(&mut encoded, payload_len, "journal batch payload")?;
    write_journal_batch_payload(&mut encoded, batch)?;

    Ok(encoded)
}

pub(in crate::db) fn decode_journal_batch(bytes: &[u8]) -> Result<JournalBatch, InternalError> {
    if bytes.len() > MAX_JOURNAL_BATCH_BYTES as usize {
        return Err(journal_batch_corruption());
    }
    let fixed_header = inspect_raw_journal_batch_fixed_header(bytes)?;
    if fixed_header.total_len() != bytes.len() {
        return Err(journal_batch_corruption());
    }

    let mut cursor = JOURNAL_BATCH_FIXED_HEADER_BYTES;
    let payload_end = fixed_header.total_len();
    let record_count = fixed_header.record_count() as usize;

    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(read_journal_record(bytes, &mut cursor)?);
    }

    if cursor != payload_end {
        return Err(journal_batch_corruption());
    }

    JournalBatch::new(
        fixed_header.batch_id(),
        fixed_header.commit_marker_id(),
        fixed_header.journal_sequence(),
        records,
    )
}

/// Current fixed journal-batch envelope facts available before payload decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::journal) struct RawJournalBatchHeader {
    payload_len: usize,
    total_len: usize,
}

impl RawJournalBatchHeader {
    /// Return the encoded payload bytes advertised by the current header.
    #[must_use]
    pub(in crate::db::journal) const fn payload_len(self) -> usize {
        self.payload_len
    }

    /// Return the exact encoded batch length including the fixed header.
    #[must_use]
    pub(in crate::db::journal) const fn total_len(self) -> usize {
        self.total_len
    }
}

/// Inspect the fixed current-form journal-batch header without decoding records.
pub(in crate::db::journal) fn inspect_raw_journal_batch_header(
    bytes: &[u8],
) -> Result<RawJournalBatchHeader, InternalError> {
    if bytes.len() < JOURNAL_BATCH_HEADER_BYTES {
        return Err(journal_batch_corruption());
    }

    let mut cursor = 0usize;
    let magic = read_fixed_array::<4>(bytes, &mut cursor, "journal batch magic")?;
    if magic != JOURNAL_BATCH_MAGIC {
        return Err(journal_batch_corruption());
    }

    let format_version = *bytes.get(cursor).ok_or_else(journal_batch_corruption)?;
    cursor = cursor.saturating_add(1);
    validate_journal_batch_format_version(format_version)?;

    let payload_len = read_len_u32(bytes, &mut cursor, "journal batch payload")? as usize;
    let total_len = JOURNAL_BATCH_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(journal_batch_corruption)?;
    if total_len > MAX_JOURNAL_BATCH_BYTES as usize {
        return Err(journal_batch_corruption());
    }

    Ok(RawJournalBatchHeader {
        payload_len,
        total_len,
    })
}

/// Current fixed journal-batch facts available before record decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::journal) struct RawJournalBatchFixedHeader {
    header: RawJournalBatchHeader,
    batch_id: JournalBatchId,
    commit_marker_id: JournalCommitMarkerId,
    journal_sequence: JournalSequence,
    record_count: u32,
}

impl RawJournalBatchFixedHeader {
    /// Return the exact encoded batch length including the fixed header.
    #[must_use]
    pub(in crate::db::journal) const fn total_len(self) -> usize {
        self.header.total_len()
    }

    /// Return the decoded marker-bound batch identity.
    #[must_use]
    pub(in crate::db::journal) const fn batch_id(self) -> JournalBatchId {
        self.batch_id
    }

    /// Return the decoded commit marker identity.
    #[must_use]
    pub(in crate::db::journal) const fn commit_marker_id(self) -> JournalCommitMarkerId {
        self.commit_marker_id
    }

    /// Return the decoded durable journal sequence.
    #[must_use]
    pub(in crate::db::journal) const fn journal_sequence(self) -> JournalSequence {
        self.journal_sequence
    }

    /// Return the current-envelope record count.
    #[must_use]
    pub(in crate::db::journal) const fn record_count(self) -> u32 {
        self.record_count
    }
}

/// Inspect all fixed current-form journal-batch facts before decoding records.
pub(in crate::db::journal) fn inspect_raw_journal_batch_fixed_header(
    bytes: &[u8],
) -> Result<RawJournalBatchFixedHeader, InternalError> {
    let header = inspect_raw_journal_batch_header(bytes)?;
    if header.total_len() < JOURNAL_BATCH_FIXED_HEADER_BYTES
        || bytes.len() < JOURNAL_BATCH_FIXED_HEADER_BYTES
    {
        return Err(journal_batch_corruption());
    }

    let mut cursor = JOURNAL_BATCH_HEADER_BYTES;
    let batch_id = read_fixed_array::<JOURNAL_BATCH_ID_BYTES>(bytes, &mut cursor, "batch id")?;
    if batch_id == [0; JOURNAL_BATCH_ID_BYTES] {
        return Err(journal_batch_corruption());
    }
    let commit_marker_id =
        read_fixed_array::<JOURNAL_COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    if commit_marker_id == [0; JOURNAL_COMMIT_MARKER_ID_BYTES] {
        return Err(journal_batch_corruption());
    }
    let journal_sequence = JournalSequence::new(read_u64_le(bytes, &mut cursor, "sequence")?);
    let record_count = read_len_u32(bytes, &mut cursor, "journal batch record count")?;
    if record_count as usize > MAX_JOURNAL_BATCH_RECORDS {
        return Err(journal_batch_corruption());
    }

    Ok(RawJournalBatchFixedHeader {
        header,
        batch_id,
        commit_marker_id,
        journal_sequence,
        record_count,
    })
}

#[must_use]
pub(in crate::db) fn journal_batch_encoded_len(batch: &JournalBatch) -> usize {
    JOURNAL_BATCH_HEADER_BYTES.saturating_add(journal_batch_payload_len(batch))
}

/// Return the canonical encoded length of one journal batch from already-sized
/// record payloads, or `None` when the record count is outside the format.
#[must_use]
pub(in crate::db) const fn journal_batch_encoded_len_for_record_payloads(
    record_count: usize,
    record_payload_bytes: usize,
) -> Option<usize> {
    if record_count > MAX_JOURNAL_BATCH_RECORDS {
        return None;
    }

    Some(
        JOURNAL_BATCH_HEADER_BYTES
            .saturating_add(JOURNAL_BATCH_ID_BYTES)
            .saturating_add(JOURNAL_COMMIT_MARKER_ID_BYTES)
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>())
            .saturating_add(record_payload_bytes),
    )
}

/// Return the canonical payload length of one row-put journal record.
#[must_use]
pub(in crate::db) const fn journal_row_put_record_payload_len(
    entity_path_bytes: usize,
    primary_key_bytes: usize,
    row_bytes: usize,
) -> usize {
    1usize
        .saturating_add(size_of::<u32>() + entity_path_bytes)
        .saturating_add(size_of::<u32>() + primary_key_bytes)
        .saturating_add(size_of::<u32>() + row_bytes)
        .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES)
}

/// Return the canonical payload length of one row-delete journal record.
#[must_use]
pub(in crate::db) const fn journal_row_delete_record_payload_len(
    entity_path_bytes: usize,
    primary_key_bytes: usize,
) -> usize {
    1usize
        .saturating_add(size_of::<u32>() + entity_path_bytes)
        .saturating_add(size_of::<u32>() + primary_key_bytes)
        .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES)
}

fn write_journal_batch_payload(
    out: &mut Vec<u8>,
    batch: &JournalBatch,
) -> Result<(), InternalError> {
    out.extend_from_slice(&batch.batch_id);
    out.extend_from_slice(&batch.commit_marker_id);
    out.extend_from_slice(&batch.journal_sequence.get().to_le_bytes());
    write_len_u32(out, batch.records.len(), "journal batch record count")?;
    for record in &batch.records {
        write_journal_record(out, record)?;
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the current persisted journal format is encoded by one exhaustive record-kind match"
)]
fn write_journal_record(out: &mut Vec<u8>, record: &JournalRecord) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } => {
            out.push(JOURNAL_RECORD_ROW_PUT);
            write_len_prefixed_bytes(out, entity_path.as_bytes(), "journal row entity_path")?;
            write_len_prefixed_bytes(out, primary_key.as_bytes(), "journal row primary_key")?;
            write_len_prefixed_bytes(out, row_bytes, "journal row payload")?;
            out.extend_from_slice(schema_fingerprint);
        }
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            schema_fingerprint,
        } => {
            out.push(JOURNAL_RECORD_ROW_DELETE);
            write_len_prefixed_bytes(out, entity_path.as_bytes(), "journal row entity_path")?;
            write_len_prefixed_bytes(out, primary_key.as_bytes(), "journal row primary_key")?;
            out.extend_from_slice(schema_fingerprint);
        }
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => {
            out.push(JOURNAL_RECORD_SCHEMA_PUT);
            write_len_prefixed_bytes(out, store_path.as_bytes(), "journal schema store_path")?;
            write_len_prefixed_bytes(
                out,
                schema_snapshot_bytes,
                "journal schema snapshot payload",
            )?;
        }
        JournalRecord::AcceptedSchemaPublish {
            store_path,
            expected_revision,
            schema_bundle_bytes,
            schema_root_bytes,
        } => {
            out.push(JOURNAL_RECORD_ACCEPTED_SCHEMA_PUBLISH);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal accepted schema store_path",
            )?;
            out.extend_from_slice(&expected_revision.get().to_le_bytes());
            write_len_prefixed_bytes(out, schema_bundle_bytes, "journal accepted schema bundle")?;
            write_len_prefixed_bytes(out, schema_root_bytes, "journal accepted schema root")?;
        }
        JournalRecord::AcceptedSchemaIndexDelete {
            store_path,
            entity_tag,
            accepted_after_fingerprint,
            keys,
        } => {
            out.push(JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_DELETE);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal accepted schema index store_path",
            )?;
            out.extend_from_slice(&entity_tag.value().to_le_bytes());
            out.extend_from_slice(accepted_after_fingerprint);
            write_index_key_chunk(out, keys)?;
        }
        JournalRecord::AcceptedSchemaIndexPut {
            store_path,
            entity_tag,
            accepted_after_fingerprint,
            keys,
        } => {
            out.push(JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_PUT);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal accepted schema index store_path",
            )?;
            out.extend_from_slice(&entity_tag.value().to_le_bytes());
            out.extend_from_slice(accepted_after_fingerprint);
            write_index_key_chunk(out, keys)?;
        }
        JournalRecord::ConstraintValidationJobPut {
            store_path,
            entity_tag,
            constraint_id,
            job_bytes,
        } => {
            out.push(JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_PUT);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal validation job store_path",
            )?;
            out.extend_from_slice(&entity_tag.value().to_le_bytes());
            out.extend_from_slice(&constraint_id.get().to_le_bytes());
            write_len_prefixed_bytes(out, job_bytes, "journal validation job payload")?;
        }
        JournalRecord::ConstraintValidationJobDelete {
            store_path,
            entity_tag,
            constraint_id,
        } => {
            out.push(JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_DELETE);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal validation job store_path",
            )?;
            out.extend_from_slice(&entity_tag.value().to_le_bytes());
            out.extend_from_slice(&constraint_id.get().to_le_bytes());
        }
        JournalRecord::ConstraintValidationIndexPut {
            store_path,
            entity_tag,
            constraint_id,
            key,
        } => {
            out.push(JOURNAL_RECORD_CONSTRAINT_VALIDATION_INDEX_PUT);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal validation index store_path",
            )?;
            out.extend_from_slice(&entity_tag.value().to_le_bytes());
            out.extend_from_slice(&constraint_id.get().to_le_bytes());
            write_len_prefixed_bytes(out, key.as_bytes(), "journal validation index key")?;
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            let owner = range.owner();
            out.push(JOURNAL_RECORD_IDENTITY_RANGE_ADVANCE);
            out.extend_from_slice(&owner.database_incarnation_id().to_bytes());
            out.extend_from_slice(&owner.entity_tag().value().to_le_bytes());
            out.extend_from_slice(&owner.field_id().get().to_le_bytes());
            out.extend_from_slice(&range.expected_high_water().to_le_bytes());
            out.extend_from_slice(&range.new_high_water().to_le_bytes());
            out.extend_from_slice(&range.allocation_count().to_le_bytes());
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
            plan_digest,
        } => {
            out.push(JOURNAL_RECORD_SCHEMA_MIGRATION_ROW_PUT);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal migration row store_path",
            )?;
            write_len_prefixed_bytes(
                out,
                primary_key.as_bytes(),
                "journal migration row primary_key",
            )?;
            write_len_prefixed_bytes(out, row_bytes, "journal migration row payload")?;
            out.extend_from_slice(schema_fingerprint);
            out.extend_from_slice(&plan_digest.to_bytes());
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path,
            key,
            plan_digest,
        } => {
            out.push(JOURNAL_RECORD_SCHEMA_MIGRATION_INDEX_PUT);
            write_len_prefixed_bytes(
                out,
                store_path.as_bytes(),
                "journal migration index store_path",
            )?;
            write_len_prefixed_bytes(out, key.as_bytes(), "journal migration index key")?;
            out.extend_from_slice(&plan_digest.to_bytes());
        }
    }

    Ok(())
}

fn write_index_key_chunk(
    out: &mut Vec<u8>,
    keys: &[RawIndexStoreKey],
) -> Result<(), InternalError> {
    write_len_u32(out, keys.len(), "journal accepted schema index key count")?;
    for key in keys {
        write_len_prefixed_bytes(out, key.as_bytes(), "journal accepted schema index key")?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive decoder keeps every current journal record tag on the same bounded cursor authority"
)]
fn read_journal_record(bytes: &[u8], cursor: &mut usize) -> Result<JournalRecord, InternalError> {
    let tag = *bytes.get(*cursor).ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(1);

    match tag {
        JOURNAL_RECORD_ROW_PUT => {
            let entity_path = read_utf8_path(bytes, cursor, "journal row entity_path")?;
            let primary_key = read_primary_key(bytes, cursor)?;
            let row_bytes = read_len_prefixed_bytes(bytes, cursor, "journal row payload")?.to_vec();
            let schema_fingerprint = read_fixed_array::<JOURNAL_SCHEMA_FINGERPRINT_BYTES>(
                bytes,
                cursor,
                "schema fingerprint",
            )?;

            JournalRecord::row_put(entity_path, primary_key, row_bytes, schema_fingerprint)
        }
        JOURNAL_RECORD_ROW_DELETE => {
            let entity_path = read_utf8_path(bytes, cursor, "journal row entity_path")?;
            let primary_key = read_primary_key(bytes, cursor)?;
            let schema_fingerprint = read_fixed_array::<JOURNAL_SCHEMA_FINGERPRINT_BYTES>(
                bytes,
                cursor,
                "schema fingerprint",
            )?;

            JournalRecord::row_delete(entity_path, primary_key, schema_fingerprint)
        }
        JOURNAL_RECORD_SCHEMA_PUT => {
            let store_path = read_utf8_path(bytes, cursor, "journal schema store_path")?;
            let schema_snapshot_bytes =
                read_len_prefixed_bytes(bytes, cursor, "journal schema snapshot payload")?.to_vec();

            JournalRecord::schema_put(store_path, schema_snapshot_bytes)
        }
        JOURNAL_RECORD_ACCEPTED_SCHEMA_PUBLISH => {
            let store_path = read_utf8_path(bytes, cursor, "journal accepted schema store_path")?;
            let expected_revision = AcceptedSchemaRevision::new(read_u64_le(
                bytes,
                cursor,
                "journal accepted schema expected revision",
            )?);
            let schema_bundle_bytes =
                read_len_prefixed_bytes(bytes, cursor, "journal accepted schema bundle")?.to_vec();
            let schema_root_bytes =
                read_len_prefixed_bytes(bytes, cursor, "journal accepted schema root")?.to_vec();
            JournalRecord::accepted_schema_publish(
                store_path,
                expected_revision,
                schema_bundle_bytes,
                schema_root_bytes,
            )
        }
        JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_DELETE => {
            let (store_path, entity_tag, accepted_after_fingerprint, keys) =
                read_accepted_schema_index_chunk(bytes, cursor)?;
            JournalRecord::accepted_schema_index_delete(
                store_path,
                entity_tag,
                accepted_after_fingerprint,
                keys,
            )
        }
        JOURNAL_RECORD_ACCEPTED_SCHEMA_INDEX_PUT => {
            let (store_path, entity_tag, accepted_after_fingerprint, keys) =
                read_accepted_schema_index_chunk(bytes, cursor)?;
            JournalRecord::accepted_schema_index_put(
                store_path,
                entity_tag,
                accepted_after_fingerprint,
                keys,
            )
        }
        JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_PUT => {
            let store_path = read_utf8_path(bytes, cursor, "journal validation job store_path")?;
            let entity_tag = EntityTag::new(read_u64_le(
                bytes,
                cursor,
                "journal validation job entity tag",
            )?);
            let constraint_id = ConstraintId::new(read_u32_le(
                bytes,
                cursor,
                "journal validation job constraint id",
            )?)
            .ok_or_else(journal_batch_corruption)?;
            let job_bytes =
                read_len_prefixed_bytes(bytes, cursor, "journal validation job payload")?.to_vec();
            let job = decode_constraint_validation_job(&job_bytes)
                .map_err(|_| journal_batch_corruption())?;
            if job.entity_tag() != entity_tag || job.constraint_id() != constraint_id {
                return Err(journal_batch_corruption());
            }
            JournalRecord::constraint_validation_job_put(store_path, &job)
        }
        JOURNAL_RECORD_CONSTRAINT_VALIDATION_JOB_DELETE => {
            let store_path = read_utf8_path(bytes, cursor, "journal validation job store_path")?;
            let entity_tag = EntityTag::new(read_u64_le(
                bytes,
                cursor,
                "journal validation job entity tag",
            )?);
            let constraint_id = ConstraintId::new(read_u32_le(
                bytes,
                cursor,
                "journal validation job constraint id",
            )?)
            .ok_or_else(journal_batch_corruption)?;
            JournalRecord::constraint_validation_job_delete(store_path, entity_tag, constraint_id)
        }
        JOURNAL_RECORD_CONSTRAINT_VALIDATION_INDEX_PUT => {
            let store_path = read_utf8_path(bytes, cursor, "journal validation index store_path")?;
            let entity_tag = EntityTag::new(read_u64_le(
                bytes,
                cursor,
                "journal validation index entity tag",
            )?);
            let constraint_id = ConstraintId::new(read_u32_le(
                bytes,
                cursor,
                "journal validation index constraint id",
            )?)
            .ok_or_else(journal_batch_corruption)?;
            let key_bytes = read_len_prefixed_bytes(bytes, cursor, "journal validation index key")?;
            if key_bytes.len() > crate::db::index::IndexKey::MAX_STORED_SIZE_USIZE {
                return Err(journal_batch_corruption());
            }
            let key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Borrowed(key_bytes));
            JournalRecord::constraint_validation_index_put(
                store_path,
                entity_tag,
                constraint_id,
                key,
            )
        }
        JOURNAL_RECORD_IDENTITY_RANGE_ADVANCE => {
            let database_incarnation_id = DatabaseIncarnationId::try_from_bytes(
                read_fixed_array::<16>(bytes, cursor, "journal identity database incarnation")?,
            )
            .map_err(|_| journal_batch_corruption())?;
            let entity_tag =
                EntityTag::new(read_u64_le(bytes, cursor, "journal identity entity tag")?);
            let field_id = FieldId::new(read_u32_le(bytes, cursor, "journal identity field id")?);
            let expected_high_water =
                read_u128_le(bytes, cursor, "journal identity expected high-water")?;
            let new_high_water = read_u128_le(bytes, cursor, "journal identity new high-water")?;
            let allocation_count = read_u32_le(bytes, cursor, "journal identity allocation count")?;
            let owner = IdentityStateOwner::try_new(database_incarnation_id, entity_tag, field_id)
                .map_err(|_| journal_batch_corruption())?;
            let range = IdentityRangeAdvance::try_new(
                owner,
                expected_high_water,
                new_high_water,
                allocation_count,
            )
            .map_err(|_| journal_batch_corruption())?;
            JournalRecord::identity_range_advance(range)
        }
        #[cfg(any(test, feature = "migration"))]
        JOURNAL_RECORD_SCHEMA_MIGRATION_ROW_PUT => {
            let store_path = read_utf8_path(bytes, cursor, "journal migration row store_path")?;
            let primary_key = read_primary_key(bytes, cursor)?;
            let row_bytes =
                read_len_prefixed_bytes(bytes, cursor, "journal migration row payload")?.to_vec();
            let schema_fingerprint = read_fixed_array::<JOURNAL_SCHEMA_FINGERPRINT_BYTES>(
                bytes,
                cursor,
                "migration row schema fingerprint",
            )?;
            let plan_digest = SchemaMigrationPlanDigest::from_bytes(read_fixed_array::<32>(
                bytes,
                cursor,
                "migration row plan digest",
            )?);
            JournalRecord::schema_migration_row_put(
                store_path,
                primary_key,
                row_bytes,
                schema_fingerprint,
                plan_digest,
            )
        }
        #[cfg(any(test, feature = "migration"))]
        JOURNAL_RECORD_SCHEMA_MIGRATION_INDEX_PUT => {
            let store_path = read_utf8_path(bytes, cursor, "journal migration index store_path")?;
            let key_bytes = read_len_prefixed_bytes(bytes, cursor, "journal migration index key")?;
            if key_bytes.len() > crate::db::index::IndexKey::MAX_STORED_SIZE_USIZE {
                return Err(journal_batch_corruption());
            }
            let key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Borrowed(key_bytes));
            let plan_digest = SchemaMigrationPlanDigest::from_bytes(read_fixed_array::<32>(
                bytes,
                cursor,
                "migration index plan digest",
            )?);
            JournalRecord::schema_migration_index_put(store_path, key, plan_digest)
        }
        _ => Err(journal_batch_corruption()),
    }
}

fn read_accepted_schema_index_chunk(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<
    (
        String,
        EntityTag,
        CommitSchemaFingerprint,
        Vec<RawIndexStoreKey>,
    ),
    InternalError,
> {
    let store_path = read_utf8_path(bytes, cursor, "journal accepted schema index store_path")?;
    let entity_tag = EntityTag::new(read_u64_le(
        bytes,
        cursor,
        "journal accepted schema index entity tag",
    )?);
    let accepted_after_fingerprint = read_fixed_array::<JOURNAL_SCHEMA_FINGERPRINT_BYTES>(
        bytes,
        cursor,
        "journal accepted schema index fingerprint",
    )?;
    let key_count =
        read_len_u32(bytes, cursor, "journal accepted schema index key count")? as usize;
    if !(1..=MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD).contains(&key_count) {
        return Err(journal_batch_corruption());
    }
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let key_bytes =
            read_len_prefixed_bytes(bytes, cursor, "journal accepted schema index key")?;
        if key_bytes.len() > IndexKey::MAX_STORED_SIZE_USIZE {
            return Err(journal_batch_corruption());
        }
        keys.push(<RawIndexStoreKey as Storable>::from_bytes(Cow::Borrowed(
            key_bytes,
        )));
    }
    Ok((store_path, entity_tag, accepted_after_fingerprint, keys))
}

fn read_primary_key(bytes: &[u8], cursor: &mut usize) -> Result<RawDataStoreKey, InternalError> {
    let primary_key = read_len_prefixed_bytes(bytes, cursor, "journal row primary_key")?;
    if primary_key.len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE {
        return Err(journal_batch_corruption());
    }

    Ok(<RawDataStoreKey as Storable>::from_bytes(Cow::Borrowed(
        primary_key,
    )))
}

fn journal_batch_payload_len(batch: &JournalBatch) -> usize {
    let record_payload_bytes = batch.records.iter().fold(0usize, |len, record| {
        len.saturating_add(journal_record_payload_len(record))
    });
    journal_batch_encoded_len_for_record_payloads(batch.records.len(), record_payload_bytes)
        .unwrap_or(usize::MAX)
        .saturating_sub(JOURNAL_BATCH_HEADER_BYTES)
}

pub(in crate::db) fn journal_record_payload_len(record: &JournalRecord) -> usize {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            ..
        } => journal_row_put_record_payload_len(
            entity_path.len(),
            primary_key.as_bytes().len(),
            row_bytes.len(),
        ),
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            ..
        } => journal_row_delete_record_payload_len(entity_path.len(), primary_key.as_bytes().len()),
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u32>() + schema_snapshot_bytes.len()),
        JournalRecord::AcceptedSchemaPublish {
            store_path,
            schema_bundle_bytes,
            schema_root_bytes,
            ..
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>() + schema_bundle_bytes.len())
            .saturating_add(size_of::<u32>() + schema_root_bytes.len()),
        JournalRecord::AcceptedSchemaIndexDelete {
            store_path, keys, ..
        }
        | JournalRecord::AcceptedSchemaIndexPut {
            store_path, keys, ..
        } => accepted_schema_index_chunk_payload_len(store_path.len(), keys),
        JournalRecord::ConstraintValidationJobPut {
            store_path,
            job_bytes,
            ..
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>())
            .saturating_add(size_of::<u32>() + job_bytes.len()),
        JournalRecord::ConstraintValidationJobDelete { store_path, .. } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>()),
        JournalRecord::ConstraintValidationIndexPut {
            store_path, key, ..
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>())
            .saturating_add(size_of::<u32>() + key.as_bytes().len()),
        JournalRecord::IdentityRangeAdvance { .. } => 1usize
            .saturating_add(16)
            .saturating_add(size_of::<u64>())
            .saturating_add(size_of::<u32>())
            .saturating_add(size_of::<u128>() * 2)
            .saturating_add(size_of::<u32>()),
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            ..
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u32>() + primary_key.as_bytes().len())
            .saturating_add(size_of::<u32>() + row_bytes.len())
            .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES)
            .saturating_add(32),
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path, key, ..
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u32>() + key.as_bytes().len())
            .saturating_add(32),
    }
}

fn accepted_schema_index_chunk_payload_len(
    store_path_bytes: usize,
    keys: &[RawIndexStoreKey],
) -> usize {
    keys.iter().fold(
        1usize
            .saturating_add(size_of::<u32>() + store_path_bytes)
            .saturating_add(size_of::<u64>())
            .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES)
            .saturating_add(size_of::<u32>()),
        |bytes, key| bytes.saturating_add(size_of::<u32>() + key.as_bytes().len()),
    )
}

fn validate_journal_batch_shape(batch: &JournalBatch) -> Result<(), InternalError> {
    if batch.batch_id == [0; JOURNAL_BATCH_ID_BYTES] {
        return Err(journal_batch_corruption());
    }
    if batch.commit_marker_id == [0; JOURNAL_COMMIT_MARKER_ID_BYTES] {
        return Err(journal_batch_corruption());
    }
    if batch.records.len() > MAX_JOURNAL_BATCH_RECORDS {
        return Err(journal_batch_corruption());
    }
    for record in &batch.records {
        validate_journal_record(record)?;
    }
    validate_row_record_targets(batch)?;
    #[cfg(any(test, feature = "migration"))]
    validate_schema_migration_record_targets(batch)?;
    validate_identity_range_row_sets(batch)?;
    validate_constraint_validation_job_transition(batch)?;
    validate_constraint_validation_index_set(batch)?;
    validate_accepted_schema_index_chunks(batch)?;

    Ok(())
}

fn validate_row_record_targets(batch: &JournalBatch) -> Result<(), InternalError> {
    let mut targets = BTreeSet::new();
    for record in &batch.records {
        match record {
            JournalRecord::RowPut { primary_key, .. }
            | JournalRecord::RowDelete { primary_key, .. } => {
                if !targets.insert(primary_key.as_bytes()) {
                    return Err(journal_batch_corruption());
                }
            }
            JournalRecord::SchemaPut { .. }
            | JournalRecord::AcceptedSchemaPublish { .. }
            | JournalRecord::AcceptedSchemaIndexDelete { .. }
            | JournalRecord::AcceptedSchemaIndexPut { .. }
            | JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. }
            | JournalRecord::ConstraintValidationIndexPut { .. }
            | JournalRecord::IdentityRangeAdvance { .. } => {}
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationRowPut { .. }
            | JournalRecord::SchemaMigrationIndexPut { .. } => {}
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "migration"))]
fn validate_schema_migration_record_targets(batch: &JournalBatch) -> Result<(), InternalError> {
    let mut plan = None;
    let mut row_targets = BTreeSet::new();
    let mut index_targets = BTreeSet::new();
    for record in &batch.records {
        let Some(record_plan) = journal_record_migration_plan(record) else {
            continue;
        };
        if plan.is_some_and(|existing| existing != record_plan) {
            return Err(journal_batch_corruption());
        }
        plan = Some(record_plan);
        match record {
            JournalRecord::SchemaMigrationRowPut {
                store_path,
                primary_key,
                ..
            } => {
                if !row_targets.insert((store_path.as_str(), primary_key.as_bytes())) {
                    return Err(journal_batch_corruption());
                }
            }
            JournalRecord::SchemaMigrationIndexPut {
                store_path, key, ..
            } => {
                if !index_targets.insert((store_path.as_str(), key.as_bytes())) {
                    return Err(journal_batch_corruption());
                }
            }
            JournalRecord::RowPut { .. }
            | JournalRecord::RowDelete { .. }
            | JournalRecord::SchemaPut { .. }
            | JournalRecord::AcceptedSchemaPublish { .. }
            | JournalRecord::AcceptedSchemaIndexDelete { .. }
            | JournalRecord::AcceptedSchemaIndexPut { .. }
            | JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. }
            | JournalRecord::ConstraintValidationIndexPut { .. }
            | JournalRecord::IdentityRangeAdvance { .. } => {}
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "migration"))]
const fn journal_record_migration_plan(
    record: &JournalRecord,
) -> Option<SchemaMigrationPlanDigest> {
    match record {
        JournalRecord::SchemaMigrationRowPut { plan_digest, .. }
        | JournalRecord::SchemaMigrationIndexPut { plan_digest, .. } => Some(*plan_digest),
        _ => None,
    }
}

fn validate_accepted_schema_index_chunks(batch: &JournalBatch) -> Result<(), InternalError> {
    let has_chunks = batch.records.iter().any(|record| {
        matches!(
            record,
            JournalRecord::AcceptedSchemaIndexDelete { .. }
                | JournalRecord::AcceptedSchemaIndexPut { .. }
        )
    });
    if !has_chunks {
        return Ok(());
    }
    let Some(JournalRecord::AcceptedSchemaPublish {
        store_path: publish_store_path,
        ..
    }) = batch.records.first()
    else {
        return Err(journal_batch_corruption());
    };

    let mut previous_entity = None;
    let mut previous_fingerprint = None;
    let mut previous_kind = 0_u8;
    let mut previous_key = None;
    for record in &batch.records[1..] {
        let (store_path, entity_tag, fingerprint, kind, keys) = match record {
            JournalRecord::AcceptedSchemaIndexDelete {
                store_path,
                entity_tag,
                accepted_after_fingerprint,
                keys,
            } => (
                store_path,
                *entity_tag,
                *accepted_after_fingerprint,
                1_u8,
                keys,
            ),
            JournalRecord::AcceptedSchemaIndexPut {
                store_path,
                entity_tag,
                accepted_after_fingerprint,
                keys,
            } => (
                store_path,
                *entity_tag,
                *accepted_after_fingerprint,
                2_u8,
                keys,
            ),
            _ => return Err(journal_batch_corruption()),
        };
        if store_path != publish_store_path
            || previous_entity.is_some_and(|previous| previous > entity_tag)
            || (previous_entity == Some(entity_tag)
                && (previous_fingerprint != Some(fingerprint) || previous_kind > kind))
        {
            return Err(journal_batch_corruption());
        }
        if previous_entity != Some(entity_tag) || previous_kind != kind {
            previous_key = None;
        }
        for key in keys {
            if previous_key.is_some_and(|previous: &RawIndexStoreKey| previous >= key) {
                return Err(journal_batch_corruption());
            }
            previous_key = Some(key);
        }
        previous_entity = Some(entity_tag);
        previous_fingerprint = Some(fingerprint);
        previous_kind = kind;
    }
    Ok(())
}

fn validate_constraint_validation_job_transition(
    batch: &JournalBatch,
) -> Result<(), InternalError> {
    let mut has_job_transition = false;
    for record in &batch.records {
        match record {
            JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. } => {
                if has_job_transition {
                    return Err(journal_batch_corruption());
                }
                has_job_transition = true;
            }
            JournalRecord::RowPut { .. }
            | JournalRecord::RowDelete { .. }
            | JournalRecord::SchemaPut { .. }
            | JournalRecord::AcceptedSchemaPublish { .. }
            | JournalRecord::AcceptedSchemaIndexDelete { .. }
            | JournalRecord::AcceptedSchemaIndexPut { .. }
            | JournalRecord::ConstraintValidationIndexPut { .. }
            | JournalRecord::IdentityRangeAdvance { .. } => {}
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationRowPut { .. }
            | JournalRecord::SchemaMigrationIndexPut { .. } => {}
        }
    }
    Ok(())
}

fn validate_constraint_validation_index_set(batch: &JournalBatch) -> Result<(), InternalError> {
    let has_index_entries = batch
        .records
        .iter()
        .any(|record| matches!(record, JournalRecord::ConstraintValidationIndexPut { .. }));
    if !has_index_entries {
        return Ok(());
    }
    let Some(JournalRecord::ConstraintValidationJobPut {
        store_path,
        entity_tag,
        constraint_id,
        ..
    }) = batch.records.first()
    else {
        return Err(journal_batch_corruption());
    };
    let mut previous_key = None;
    for record in &batch.records[1..] {
        let JournalRecord::ConstraintValidationIndexPut {
            store_path: entry_store_path,
            entity_tag: entry_entity_tag,
            constraint_id: entry_constraint_id,
            key,
        } = record
        else {
            return Err(journal_batch_corruption());
        };
        if entry_store_path != store_path
            || entry_entity_tag != entity_tag
            || entry_constraint_id != constraint_id
            || previous_key.is_some_and(|previous| previous >= key)
        {
            return Err(journal_batch_corruption());
        }
        previous_key = Some(key);
    }
    Ok(())
}

fn validate_identity_range_row_sets(batch: &JournalBatch) -> Result<(), InternalError> {
    let ranges = batch
        .records
        .iter()
        .filter_map(|record| match record {
            JournalRecord::IdentityRangeAdvance { range } => Some(*range),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (index, range) in ranges.iter().copied().enumerate() {
        if ranges[..index]
            .iter()
            .any(|existing| existing.owner() == range.owner())
        {
            return Err(journal_batch_corruption());
        }
        let mut expected_allocation = range.expected_high_water();
        let mut count = 0u32;
        for record in &batch.records {
            match record {
                JournalRecord::RowPut { primary_key, .. } => {
                    let key = DecodedDataStoreKey::try_from_raw(primary_key)
                        .map_err(|_| journal_batch_corruption())?;
                    if key.entity_tag() != range.owner().entity_tag() {
                        continue;
                    }
                    let value = match key.primary_key_value() {
                        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)) => {
                            u128::from(value)
                        }
                        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat128(value)) => value,
                        _ => return Err(journal_batch_corruption()),
                    };
                    if value <= range.expected_high_water() {
                        continue;
                    }
                    if value > range.new_high_water() {
                        return Err(journal_batch_corruption());
                    }
                    expected_allocation = expected_allocation
                        .checked_add(1)
                        .ok_or_else(journal_batch_corruption)?;
                    count = count.checked_add(1).ok_or_else(journal_batch_corruption)?;
                    if value != expected_allocation {
                        return Err(journal_batch_corruption());
                    }
                }
                JournalRecord::RowDelete { primary_key, .. } => {
                    let key = DecodedDataStoreKey::try_from_raw(primary_key)
                        .map_err(|_| journal_batch_corruption())?;
                    if key.entity_tag() != range.owner().entity_tag() {
                        continue;
                    }
                    let value = match key.primary_key_value() {
                        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)) => {
                            u128::from(value)
                        }
                        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat128(value)) => value,
                        _ => return Err(journal_batch_corruption()),
                    };
                    if value > range.expected_high_water() {
                        return Err(journal_batch_corruption());
                    }
                }
                JournalRecord::SchemaPut { .. }
                | JournalRecord::AcceptedSchemaPublish { .. }
                | JournalRecord::AcceptedSchemaIndexDelete { .. }
                | JournalRecord::AcceptedSchemaIndexPut { .. }
                | JournalRecord::ConstraintValidationJobPut { .. }
                | JournalRecord::ConstraintValidationJobDelete { .. }
                | JournalRecord::ConstraintValidationIndexPut { .. }
                | JournalRecord::IdentityRangeAdvance { .. } => {}
                #[cfg(any(test, feature = "migration"))]
                JournalRecord::SchemaMigrationRowPut { .. }
                | JournalRecord::SchemaMigrationIndexPut { .. } => {}
            }
        }
        if count != range.allocation_count() || expected_allocation != range.new_high_water() {
            return Err(journal_batch_corruption());
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the current persisted journal format is validated by one exhaustive record-kind match"
)]
fn validate_journal_record(record: &JournalRecord) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            ..
        } => {
            validate_path(entity_path, "journal row entity_path")?;
            validate_primary_key_shape(primary_key)?;
            validate_row_payload(row_bytes)?;
        }
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            ..
        } => {
            validate_path(entity_path, "journal row entity_path")?;
            validate_primary_key_shape(primary_key)?;
        }
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => {
            validate_path(store_path, "journal schema store_path")?;
            if schema_snapshot_bytes.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
                return Err(journal_batch_corruption());
            }
        }
        JournalRecord::AcceptedSchemaPublish {
            store_path,
            expected_revision,
            schema_bundle_bytes,
            schema_root_bytes,
        } => {
            validate_path(store_path, "journal accepted schema store_path")?;
            let candidate = CandidateSchemaRevision::from_encoded(
                schema_bundle_bytes.clone(),
                schema_root_bytes.clone(),
            )
            .map_err(|_| journal_batch_corruption())?;
            if candidate.store_path() != store_path
                || expected_revision.checked_next() != Some(candidate.revision())
            {
                return Err(journal_batch_corruption());
            }
        }
        JournalRecord::AcceptedSchemaIndexDelete {
            store_path,
            entity_tag,
            keys,
            ..
        }
        | JournalRecord::AcceptedSchemaIndexPut {
            store_path,
            entity_tag,
            keys,
            ..
        } => {
            validate_path(store_path, "journal accepted schema index store_path")?;
            if !(1..=MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD).contains(&keys.len()) {
                return Err(journal_batch_corruption());
            }
            let mut previous = None;
            for key in keys {
                let decoded =
                    IndexKey::try_from_raw(key).map_err(|_| journal_batch_corruption())?;
                if decoded.key_kind() != IndexKeyKind::User
                    || decoded.index_id().entity_tag() != *entity_tag
                    || previous.is_some_and(|prior: &RawIndexStoreKey| prior >= key)
                {
                    return Err(journal_batch_corruption());
                }
                previous = Some(key);
            }
        }
        JournalRecord::ConstraintValidationJobPut {
            store_path,
            entity_tag,
            constraint_id,
            job_bytes,
        } => {
            validate_path(store_path, "journal validation job store_path")?;
            if job_bytes.len() > MAX_CONSTRAINT_VALIDATION_JOB_BYTES {
                return Err(journal_batch_corruption());
            }
            let job = decode_constraint_validation_job(job_bytes)
                .map_err(|_| journal_batch_corruption())?;
            if job.entity_tag() != *entity_tag || job.constraint_id() != *constraint_id {
                return Err(journal_batch_corruption());
            }
        }
        JournalRecord::ConstraintValidationJobDelete { store_path, .. } => {
            validate_path(store_path, "journal validation job store_path")?;
        }
        JournalRecord::ConstraintValidationIndexPut {
            store_path, key, ..
        } => {
            validate_path(store_path, "journal validation index store_path")?;
            if key.as_bytes().is_empty()
                || key.as_bytes().len() > crate::db::index::IndexKey::MAX_STORED_SIZE_USIZE
                || crate::db::index::IndexKey::try_from_raw(key).is_err()
            {
                return Err(journal_batch_corruption());
            }
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            IdentityRangeAdvance::try_new(
                range.owner(),
                range.expected_high_water(),
                range.new_high_water(),
                range.allocation_count(),
            )
            .map_err(|_| journal_batch_corruption())?;
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            plan_digest,
            ..
        } => {
            validate_path(store_path, "journal migration row store_path")?;
            validate_primary_key_shape(primary_key)?;
            validate_row_payload(row_bytes)?;
            if plan_digest.to_bytes() == [0; 32] {
                return Err(journal_batch_corruption());
            }
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path,
            key,
            plan_digest,
        } => {
            validate_path(store_path, "journal migration index store_path")?;
            if key.as_bytes().is_empty()
                || key.as_bytes().len() > crate::db::index::IndexKey::MAX_STORED_SIZE_USIZE
                || crate::db::index::IndexKey::try_from_raw(key).is_err()
                || plan_digest.to_bytes() == [0; 32]
            {
                return Err(journal_batch_corruption());
            }
        }
    }

    Ok(())
}

fn validate_path(path: &str, _label: &'static str) -> Result<(), InternalError> {
    if path.is_empty() {
        return Err(journal_batch_corruption());
    }
    if path.len() > MAX_JOURNAL_PATH_BYTES {
        return Err(journal_batch_corruption());
    }

    Ok(())
}

fn validate_primary_key_shape(primary_key: &RawDataStoreKey) -> Result<(), InternalError> {
    if primary_key.as_bytes().len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE {
        return Err(journal_batch_corruption());
    }

    Ok(())
}

fn validate_row_payload(row_bytes: &[u8]) -> Result<(), InternalError> {
    if row_bytes.len() > MAX_ROW_BYTES as usize {
        return Err(journal_batch_corruption());
    }

    Ok(())
}

fn validate_journal_batch_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == JOURNAL_BATCH_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    let _ = format_version;

    Err(InternalError::serialize_incompatible_persisted_format())
}

fn write_len_u32(out: &mut Vec<u8>, len: usize, _label: &'static str) -> Result<(), InternalError> {
    let len = u32::try_from(len).map_err(|_| InternalError::store_unsupported())?;
    out.extend_from_slice(&len.to_le_bytes());

    Ok(())
}

fn write_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    label: &'static str,
) -> Result<(), InternalError> {
    write_len_u32(out, bytes.len(), label)?;
    out.extend_from_slice(bytes);

    Ok(())
}

fn read_len_u32(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<u32, InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(size_of::<u32>()))
        .ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(size_of::<u32>());

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

fn read_u64_le(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<u64, InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(size_of::<u64>()))
        .ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(size_of::<u64>());

    Ok(u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]))
}

fn read_u128_le(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<u128, InternalError> {
    let payload = read_fixed_array::<16>(bytes, cursor, "journal u128")?;
    Ok(u128::from_le_bytes(payload))
}

fn read_u32_le(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<u32, InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(size_of::<u32>()))
        .ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(size_of::<u32>());

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

fn read_fixed_array<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
    _label: &'static str,
) -> Result<[u8; N], InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(N))
        .ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(N);

    payload.try_into().map_err(|_| journal_batch_corruption())
}

fn read_len_prefixed_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<&'a [u8], InternalError> {
    let len = read_len_u32(bytes, cursor, label)? as usize;
    let payload = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(journal_batch_corruption)?;
    *cursor = cursor.saturating_add(len);

    Ok(payload)
}

fn read_utf8_path(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<String, InternalError> {
    let path = read_len_prefixed_bytes(bytes, cursor, label)?;
    let path = std::str::from_utf8(path).map_err(|_| journal_batch_corruption())?;
    validate_path(path, label)?;

    Ok(path.to_owned())
}

fn journal_batch_corruption() -> InternalError {
    InternalError::store_corruption()
}
