//! Module: db::commit::marker
//! Responsibility: define persisted commit-marker payloads and marker-shape validation.
//! Does not own: marker storage backend, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::{prepare,recovery,store} -> commit::marker (one-way).

#[cfg(test)]
use crate::db::journal::{JournalRecord, JournalSequence};
use crate::{
    db::{
        commit::prepared_op::PreparedIndexDeltaKind,
        data::{DecodedDataStoreKey, RawDataStoreKey},
        index::{IndexEntryValue, IndexStore, RawIndexStoreKey},
        journal::{
            JournalBatch, decode_journal_batch, encode_journal_batch, journal_batch_encoded_len,
        },
    },
    error::InternalError,
    runtime::now_millis,
};
use ic_stable_structures::Storable;
#[cfg(test)]
use std::cell::Cell;
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
    thread::LocalKey,
};

// Commit-marker durability invariant:
// - Persist one marker before any stable mutation.
// - After marker persistence, apply/recovery consume only marker payloads.
// - Recovery publishes marker-bound journal batches deterministically.
// This makes partial mutations deterministic without a WAL.

/// Stored commit-id byte width shared by marker and guard paths.
pub(in crate::db) const COMMIT_ID_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_BYTES: usize = 16;
pub(in crate::db) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 1;

pub(in crate::db) type CommitSchemaFingerprint = [u8; COMMIT_SCHEMA_FINGERPRINT_BYTES];

static COMMIT_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
thread_local! {
    static TEST_JOURNAL_SEQUENCE: Cell<u64> = const { Cell::new(1) };
}

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub(crate) const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitRowOp
///
/// Row-level mutation recorded in a commit marker.
/// Store identity is derived from `entity_path` at apply/recovery time.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CommitRowOp {
    pub(crate) entity_path: Cow<'static, str>,
    pub(crate) key: RawDataStoreKey,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) schema_fingerprint: CommitSchemaFingerprint,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub(crate) fn new(
        entity_path: impl Into<Cow<'static, str>>,
        key: RawDataStoreKey,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
            schema_fingerprint,
        }
    }

    /// Construct one row-level commit operation from raw key bytes.
    ///
    /// This is the raw-key decode boundary for callers that still own opaque
    /// key bytes rather than a typed `RawDataStoreKey`.
    pub(crate) fn try_new_bytes(
        entity_path: impl Into<Cow<'static, str>>,
        key: &[u8],
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        let (raw_key, _) = decode_data_key(key)?;

        Ok(Self::new(
            entity_path,
            raw_key,
            before,
            after,
            schema_fingerprint,
        ))
    }
}

///
/// CommitIndexOp
///
/// Internal index mutation used during row-op preparation/apply.
/// Not persisted in commit markers.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitIndexOp {
    pub(crate) index_store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexStoreKey,
    pub(crate) value: Option<IndexEntryValue>,
    pub(crate) delta_kind: PreparedIndexDeltaKind,
}

impl CommitIndexOp {
    /// Build one index commit op without delta counter attribution.
    pub(crate) const fn unchanged(
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
    ) -> Self {
        Self {
            index_store,
            key,
            value,
            delta_kind: PreparedIndexDeltaKind::None,
        }
    }

    /// Build one index commit op that contributes to insert counters.
    pub(crate) const fn index_insert(
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
    ) -> Self {
        Self {
            index_store,
            key,
            value,
            delta_kind: PreparedIndexDeltaKind::IndexInsert,
        }
    }

    /// Build one index commit op that contributes to remove counters.
    pub(crate) const fn index_remove(
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
    ) -> Self {
        Self {
            index_store,
            key,
            value,
            delta_kind: PreparedIndexDeltaKind::IndexRemove,
        }
    }
}

///
/// CommitMarker
///
/// Persisted mutation plan covering journal publication.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption inside one marker payload version.
/// Persisted format-version rejection is owned by the marker envelope in `commit::store`.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(in crate::db) journal_batches: Vec<JournalBatch>,
}

impl CommitMarker {
    /// Construct a new commit marker with a deterministic marker id.
    #[cfg(test)]
    pub(crate) fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = generate_commit_id()?;
        if row_ops.is_empty() {
            return Self::from_parts(id, Vec::new());
        }

        let records = row_ops
            .iter()
            .map(journal_record_from_row_op_for_test)
            .collect::<Result<Vec<_>, _>>()?;
        let batch = JournalBatch::new(id, id, next_test_journal_sequence()?, records)?;

        Self::from_parts(id, vec![batch])
    }

    /// Construct one marker from already-derived durable payload parts.
    ///
    /// Journal batches are embedded in the marker so recovery can repair or
    /// verify marker-bound journal publication before replay.
    pub(in crate::db) fn from_parts(
        id: [u8; COMMIT_ID_BYTES],
        journal_batches: Vec<JournalBatch>,
    ) -> Result<Self, InternalError> {
        let marker = Self {
            id,
            journal_batches,
        };
        validate_commit_marker_shape(&marker)?;

        Ok(marker)
    }

    /// Borrow marker-bound journal batches embedded in this commit marker.
    #[must_use]
    pub(in crate::db) fn journal_batches(&self) -> &[JournalBatch] {
        &self.journal_batches
    }

    // Build the canonical payload corruption for truncated variable-length fields.
    fn payload_truncated_length(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }

    // Build the canonical payload corruption for truncated byte payloads.
    fn payload_truncated_bytes(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }

    // Build the canonical payload corruption for invalid fixed-size payloads.
    fn payload_invalid_fixed_size(_label: &'static str) -> InternalError {
        InternalError::commit_corruption()
    }
}

#[cfg(test)]
pub(in crate::db) fn reset_test_journal_sequence() {
    TEST_JOURNAL_SEQUENCE.with(|sequence| sequence.set(1));
}

#[cfg(test)]
fn next_test_journal_sequence() -> Result<JournalSequence, InternalError> {
    TEST_JOURNAL_SEQUENCE.with(|sequence| {
        let value = sequence.get();
        let next = value
            .checked_add(1)
            .ok_or_else(InternalError::commit_id_generation_failed)?;
        sequence.set(next);

        Ok(JournalSequence::new(value))
    })
}

#[cfg(test)]
fn journal_record_from_row_op_for_test(
    row_op: &CommitRowOp,
) -> Result<JournalRecord, InternalError> {
    match row_op.after.as_ref() {
        Some(after) => JournalRecord::row_put(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
            after.clone(),
            row_op.schema_fingerprint,
        ),
        None => JournalRecord::row_delete(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
            row_op.schema_fingerprint,
        ),
    }
}

const COMMIT_MARKER_ID_BYTES: usize = COMMIT_ID_BYTES;
const COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES: usize = 4;

/// Generate one deterministic commit id for marker persistence.
///
/// This id is persisted for marker identity and diagnostics; it is not a source
/// of user-visible randomness or durable commit ordering authority.
pub(in crate::db) fn generate_commit_id() -> Result<[u8; COMMIT_ID_BYTES], InternalError> {
    let sequence = COMMIT_ID_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| InternalError::commit_id_generation_failed())?;

    let mut id = [0u8; COMMIT_ID_BYTES];
    id[..8].copy_from_slice(&now_millis().to_be_bytes());
    id[8..].copy_from_slice(&sequence.to_be_bytes());

    Ok(id)
}

/// Encode one commit-marker payload in the canonical binary format.
#[cfg(test)]
pub(in crate::db) fn encode_commit_marker_payload(
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: size the output once so commit persistence writes one compact frame.
    let capacity = commit_marker_payload_capacity(marker);
    if capacity > u32::MAX as usize {
        return Err(InternalError::commit_marker_payload_exceeds_u32_length_limit());
    }

    // Phase 2: emit one length-delimited frame for deterministic recovery replay.
    let mut encoded = Vec::with_capacity(capacity);
    write_commit_marker_payload(&mut encoded, marker)?;

    Ok(encoded)
}

/// Return the canonical marker payload size without allocating it.
pub(in crate::db) fn commit_marker_payload_capacity(marker: &CommitMarker) -> usize {
    let mut capacity = COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES;
    for batch in &marker.journal_batches {
        capacity = capacity.saturating_add(4 + journal_batch_encoded_len(batch));
    }

    capacity
}

// Write the canonical marker payload into an existing output buffer.
pub(in crate::db) fn write_commit_marker_payload(
    out: &mut Vec<u8>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    out.extend_from_slice(&marker.id);
    write_len_u32(
        out,
        marker.journal_batches.len(),
        "commit marker journal batch count",
    )?;
    for batch in &marker.journal_batches {
        let encoded = encode_journal_batch(batch)?;
        write_len_prefixed_bytes(out, &encoded, "commit marker journal batch")?;
    }

    Ok(())
}

/// Decode one commit-marker payload from the canonical binary format.
pub(in crate::db) fn decode_commit_marker_payload(
    bytes: &[u8],
) -> Result<CommitMarker, InternalError> {
    // Phase 1: parse the fixed marker header before touching batch bytes.
    if bytes.len() < COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES {
        return Err(InternalError::commit_corruption());
    }

    let mut cursor = 0;
    let id = read_fixed_array::<COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    let journal_batch_count =
        read_len_u32(bytes, &mut cursor, "commit marker journal batch count")? as usize;
    let mut journal_batches = Vec::new();
    for _ in 0..journal_batch_count {
        journal_batches
            .try_reserve(1)
            .map_err(|_| InternalError::commit_corruption())?;
        let encoded = read_len_prefixed_bytes(bytes, &mut cursor, "commit marker journal batch")?;
        journal_batches.push(decode_journal_batch(encoded)?);
    }

    // Phase 3: reject trailing bytes so malformed payloads fail closed.
    if cursor != bytes.len() {
        return Err(InternalError::commit_corruption());
    }

    Ok(CommitMarker {
        id,
        journal_batches,
    })
}

// Write one bounded little-endian u32 length field.
fn write_len_u32(out: &mut Vec<u8>, len: usize, _label: &'static str) -> Result<(), InternalError> {
    let len = u32::try_from(len)
        .map_err(|_| InternalError::commit_marker_payload_exceeds_u32_length_limit())?;
    out.extend_from_slice(&len.to_le_bytes());

    Ok(())
}

// Write one length-delimited byte slice into the marker payload.
fn write_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    label: &'static str,
) -> Result<(), InternalError> {
    write_len_u32(out, bytes.len(), label)?;
    out.extend_from_slice(bytes);

    Ok(())
}

// Read one little-endian u32 length from the marker payload.
fn read_len_u32(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<u32, InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(4))
        .ok_or_else(|| CommitMarker::payload_truncated_length(label))?;
    *cursor = cursor.saturating_add(4);

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

// Read one fixed-size byte array from the marker payload.
fn read_fixed_array<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<[u8; N], InternalError> {
    let payload = bytes
        .get(*cursor..cursor.saturating_add(N))
        .ok_or_else(|| CommitMarker::payload_truncated_bytes(label))?;
    *cursor = cursor.saturating_add(N);

    payload
        .try_into()
        .map_err(|_| CommitMarker::payload_invalid_fixed_size(label))
}

// Read one length-delimited byte slice from the marker payload.
fn read_len_prefixed_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<&'a [u8], InternalError> {
    let len = read_len_u32(bytes, cursor, label)? as usize;
    let payload = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(|| CommitMarker::payload_truncated_bytes(label))?;
    *cursor = cursor.saturating_add(len);

    Ok(payload)
}

/// Decode a raw data-store key and validate its structural invariants.
pub(in crate::db) fn decode_data_key(
    bytes: &[u8],
) -> Result<(RawDataStoreKey, DecodedDataStoreKey), InternalError> {
    // Commit markers store the current data-key wire bytes length-prefixed.
    // The current data-key format is variable-width, so this gate is a bounded
    // maximum check; structural validation belongs to `DecodedDataStoreKey::try_from_raw`.
    let len = bytes.len();
    let max = RawDataStoreKey::MAX_STORED_SIZE_USIZE;
    if len > max {
        return Err(InternalError::commit_component_length_invalid());
    }

    let raw = <RawDataStoreKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    let data_key = DecodedDataStoreKey::try_from_raw(&raw)
        .map_err(|_| InternalError::commit_component_corruption())?;

    Ok((raw, data_key))
}

/// Validate commit-marker row-op shape invariants.
///
/// Every row op must represent a concrete mutation:
/// - insert (`before=None`, `after=Some`)
/// - update (`before=Some`, `after=Some`)
/// - delete (`before=Some`, `after=None`)
///
/// The empty shape (`before=None`, `after=None`) is corruption.
pub(crate) fn validate_commit_marker_shape(marker: &CommitMarker) -> Result<(), InternalError> {
    // Validate every embedded journal batch is bound to this marker
    // and has a unique batch identity and replay sequence.
    let mut batch_ids = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for batch in &marker.journal_batches {
        if batch.commit_marker_id() != marker.id {
            return Err(InternalError::commit_corruption());
        }
        if !batch_ids.insert(batch.batch_id()) {
            return Err(InternalError::commit_corruption());
        }
        if !sequences.insert(batch.journal_sequence()) {
            return Err(InternalError::commit_corruption());
        }
    }

    Ok(())
}
