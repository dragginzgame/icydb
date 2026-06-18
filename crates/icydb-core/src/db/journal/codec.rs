//! Module: db::journal::codec
//! Responsibility: bounded/fallible journal batch encoding.
//! Does not own: journal-tail storage, commit marker lifecycle, recovery, or fold.
//! Boundary: logical journal records -> stable-memory journal batch bytes.

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::{CommitSchemaFingerprint, MAX_COMMIT_BYTES},
        data::RawDataStoreKey,
        schema::MAX_SCHEMA_SNAPSHOT_BYTES,
    },
    error::InternalError,
    traits::Storable,
};
use ic_memory::stable_structures::storable::Bound;
use std::borrow::Cow;

pub(in crate::db) const JOURNAL_BATCH_FORMAT_VERSION_CURRENT: u8 = 1;
pub(in crate::db) const MAX_JOURNAL_BATCH_BYTES: u32 = MAX_COMMIT_BYTES;
const MAX_JOURNAL_BATCH_RECORDS: usize = 16 * 1024;
const MAX_JOURNAL_PATH_BYTES: usize = 4 * 1024;
const JOURNAL_BATCH_MAGIC: [u8; 4] = *b"IJBT";
const JOURNAL_BATCH_HEADER_BYTES: usize = 9;
const JOURNAL_BATCH_ID_BYTES: usize = 16;
const JOURNAL_COMMIT_MARKER_ID_BYTES: usize = 16;
const JOURNAL_SCHEMA_FINGERPRINT_BYTES: usize = 16;
const JOURNAL_RECORD_ROW_PUT: u8 = 1;
const JOURNAL_RECORD_ROW_DELETE: u8 = 2;
const JOURNAL_RECORD_SCHEMA_PUT: u8 = 3;

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

/// Logical journal record. Index entries are intentionally absent from the
/// first format; indexes are derived materialized state.

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

impl Storable for RawJournalBatch {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_JOURNAL_BATCH_BYTES,
        is_fixed_size: false,
    };
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
    let payload_end = cursor
        .checked_add(payload_len)
        .ok_or_else(journal_batch_corruption)?;
    if payload_end != bytes.len() {
        return Err(journal_batch_corruption());
    }

    let batch_id = read_fixed_array::<JOURNAL_BATCH_ID_BYTES>(bytes, &mut cursor, "batch id")?;
    let commit_marker_id =
        read_fixed_array::<JOURNAL_COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    let journal_sequence = JournalSequence::new(read_u64_le(bytes, &mut cursor, "sequence")?);
    let record_count = read_len_u32(bytes, &mut cursor, "journal batch record count")? as usize;
    if record_count > MAX_JOURNAL_BATCH_RECORDS {
        return Err(journal_batch_corruption());
    }

    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(read_journal_record(bytes, &mut cursor)?);
    }

    if cursor != payload_end {
        return Err(journal_batch_corruption());
    }

    JournalBatch::new(batch_id, commit_marker_id, journal_sequence, records)
}

#[must_use]
pub(in crate::db) fn journal_batch_encoded_len(batch: &JournalBatch) -> usize {
    JOURNAL_BATCH_HEADER_BYTES.saturating_add(journal_batch_payload_len(batch))
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
    }

    Ok(())
}

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
        _ => Err(journal_batch_corruption()),
    }
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
    let mut len = JOURNAL_BATCH_ID_BYTES
        .saturating_add(JOURNAL_COMMIT_MARKER_ID_BYTES)
        .saturating_add(size_of::<u64>())
        .saturating_add(size_of::<u32>());
    for record in &batch.records {
        len = len.saturating_add(journal_record_payload_len(record));
    }

    len
}

fn journal_record_payload_len(record: &JournalRecord) -> usize {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            ..
        } => 1usize
            .saturating_add(size_of::<u32>() + entity_path.len())
            .saturating_add(size_of::<u32>() + primary_key.as_bytes().len())
            .saturating_add(size_of::<u32>() + row_bytes.len())
            .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES),
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            ..
        } => 1usize
            .saturating_add(size_of::<u32>() + entity_path.len())
            .saturating_add(size_of::<u32>() + primary_key.as_bytes().len())
            .saturating_add(JOURNAL_SCHEMA_FINGERPRINT_BYTES),
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => 1usize
            .saturating_add(size_of::<u32>() + store_path.len())
            .saturating_add(size_of::<u32>() + schema_snapshot_bytes.len()),
    }
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

    Ok(())
}

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
