//! Module: db::commit::marker
//! Responsibility: define persisted commit-marker payloads and marker-shape validation.
//! Does not own: marker storage backend, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::{prepare,recovery,store} -> commit::marker (one-way).

#[cfg(test)]
use crate::db::journal::{JournalRecord, JournalSequence};
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
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
use ic_memory::stable_structures::Storable;
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
// - Recovery replays marker row ops deterministically.
// This makes partial mutations deterministic without a WAL.

/// Stored commit-id byte width shared by marker and guard paths.
pub(in crate::db) const COMMIT_ID_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_BYTES: usize = 16;
pub(in crate::db) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 2;

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
/// Persisted mutation plan covering row-level operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption inside one marker payload version.
/// Persisted format-version rejection is owned by the marker envelope in `commit::store`.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(crate) row_ops: Vec<CommitRowOp>,
    pub(in crate::db) journal_batches: Vec<JournalBatch>,
}

impl CommitMarker {
    /// Construct a new commit marker with a deterministic marker id.
    #[cfg(test)]
    pub(crate) fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = generate_commit_id()?;
        if row_ops.is_empty() {
            return Self::from_parts(id, row_ops, Vec::new());
        }

        let records = row_ops
            .iter()
            .map(journal_record_from_row_op_for_test)
            .collect::<Result<Vec<_>, _>>()?;
        let batch = JournalBatch::new(id, id, next_test_journal_sequence()?, records)?;

        Self::from_parts(id, Vec::new(), vec![batch])
    }

    /// Construct one marker from already-derived durable payload parts.
    ///
    /// Journal batches are embedded in the marker so recovery can repair or
    /// verify marker-bound journal publication before replay.
    pub(in crate::db) fn from_parts(
        id: [u8; COMMIT_ID_BYTES],
        row_ops: Vec<CommitRowOp>,
        journal_batches: Vec<JournalBatch>,
    ) -> Result<Self, InternalError> {
        let marker = Self {
            id,
            row_ops,
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

    // Build the canonical row-op corruption for oversized row payloads.
    fn row_op_payload_too_large(_label: &str, _len: usize) -> InternalError {
        InternalError::commit_corruption()
    }

    // Build the canonical row-op corruption for key decode failures.
    fn row_op_key_decode_failed(_err: impl Sized) -> InternalError {
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
const COMMIT_MARKER_SCHEMA_FINGERPRINT_BYTES: usize = COMMIT_SCHEMA_FINGERPRINT_BYTES;
const COMMIT_MARKER_FLAG_BEFORE: u8 = 0b0000_0001;
const COMMIT_MARKER_FLAG_AFTER: u8 = 0b0000_0010;
const COMMIT_MARKER_FLAG_MASK: u8 = COMMIT_MARKER_FLAG_BEFORE | COMMIT_MARKER_FLAG_AFTER;
const COMMIT_MARKER_ROW_COUNT_BYTES: usize = 4;
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

/// Encode one single-row commit-marker payload for hot write-lane persistence.
#[cfg(test)]
pub(in crate::db) fn encode_single_row_commit_marker_payload(
    marker_id: [u8; COMMIT_ID_BYTES],
    row_op: &CommitRowOp,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: compute the exact one-row frame capacity up front.
    let capacity = single_row_commit_marker_payload_capacity(row_op);
    if capacity > u32::MAX as usize {
        return Err(InternalError::commit_marker_payload_exceeds_u32_length_limit());
    }

    // Phase 2: encode the single row op directly without the outer row-op loop.
    let mut encoded = Vec::with_capacity(capacity);
    write_single_row_commit_marker_payload(&mut encoded, marker_id, row_op)?;

    Ok(encoded)
}

/// Return the canonical one-row marker payload size without allocating it.
pub(in crate::db) fn single_row_commit_marker_payload_capacity(row_op: &CommitRowOp) -> usize {
    COMMIT_MARKER_ID_BYTES
        .saturating_add(COMMIT_MARKER_ROW_COUNT_BYTES)
        .saturating_add(commit_row_op_payload_capacity(row_op))
        .saturating_add(COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES)
}

/// Return the canonical multi-row marker payload size without allocating it.
pub(in crate::db) fn commit_marker_payload_capacity(marker: &CommitMarker) -> usize {
    let mut capacity = COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_ROW_COUNT_BYTES;
    for row_op in &marker.row_ops {
        capacity = capacity.saturating_add(commit_row_op_payload_capacity(row_op));
    }
    capacity = capacity.saturating_add(COMMIT_MARKER_JOURNAL_BATCH_COUNT_BYTES);
    for batch in &marker.journal_batches {
        capacity = capacity.saturating_add(4 + journal_batch_encoded_len(batch));
    }

    capacity
}

// Write the canonical one-row marker payload into an existing output buffer.
pub(in crate::db) fn write_single_row_commit_marker_payload(
    out: &mut Vec<u8>,
    marker_id: [u8; COMMIT_ID_BYTES],
    row_op: &CommitRowOp,
) -> Result<(), InternalError> {
    out.extend_from_slice(&marker_id);
    write_len_u32(out, 1, "commit marker row count")?;
    write_commit_row_op(out, row_op)?;
    write_len_u32(out, 0, "commit marker journal batch count")?;

    Ok(())
}

// Write the canonical multi-row marker payload into an existing output buffer.
pub(in crate::db) fn write_commit_marker_payload(
    out: &mut Vec<u8>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    out.extend_from_slice(&marker.id);
    write_len_u32(out, marker.row_ops.len(), "commit marker row count")?;
    for row_op in &marker.row_ops {
        write_commit_row_op(out, row_op)?;
    }
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

// Return the canonical encoded payload size contribution for one row op.
fn commit_row_op_payload_capacity(row_op: &CommitRowOp) -> usize {
    let mut capacity = 4 + row_op.entity_path.len();
    capacity = capacity
        .saturating_add(4 + row_op.key.as_bytes().len())
        .saturating_add(1)
        .saturating_add(COMMIT_MARKER_SCHEMA_FINGERPRINT_BYTES);
    if let Some(bytes) = &row_op.before {
        capacity = capacity.saturating_add(4 + bytes.len());
    }
    if let Some(bytes) = &row_op.after {
        capacity = capacity.saturating_add(4 + bytes.len());
    }

    capacity
}

// Encode one row op under the canonical marker payload framing.
fn write_commit_row_op(out: &mut Vec<u8>, row_op: &CommitRowOp) -> Result<(), InternalError> {
    write_len_prefixed_bytes(
        out,
        row_op.entity_path.as_bytes(),
        "commit marker entity_path",
    )?;
    write_len_prefixed_bytes(out, row_op.key.as_bytes(), "commit marker key")?;

    let mut flags = 0_u8;
    if row_op.before.is_some() {
        flags |= COMMIT_MARKER_FLAG_BEFORE;
    }
    if row_op.after.is_some() {
        flags |= COMMIT_MARKER_FLAG_AFTER;
    }
    out.push(flags);

    if let Some(bytes) = &row_op.before {
        write_len_prefixed_bytes(out, bytes, "commit marker before payload")?;
    }
    if let Some(bytes) = &row_op.after {
        write_len_prefixed_bytes(out, bytes, "commit marker after payload")?;
    }

    out.extend_from_slice(&row_op.schema_fingerprint);

    Ok(())
}

/// Decode one commit-marker payload from the canonical binary format.
pub(in crate::db) fn decode_commit_marker_payload(
    bytes: &[u8],
) -> Result<CommitMarker, InternalError> {
    // Phase 1: parse the fixed marker header before touching any row-op bytes.
    if bytes.len() < COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_ROW_COUNT_BYTES {
        return Err(InternalError::commit_corruption());
    }

    let mut cursor = 0;
    let id = read_fixed_array::<COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    let row_op_count = read_len_u32(bytes, &mut cursor, "commit marker row count")? as usize;
    let mut row_ops = Vec::new();

    // Phase 2: parse each length-delimited row op without routing through generic decode.
    for _ in 0..row_op_count {
        row_ops
            .try_reserve(1)
            .map_err(|_| InternalError::commit_corruption())?;
        let entity_path_bytes =
            read_len_prefixed_bytes(bytes, &mut cursor, "commit marker entity_path")?;
        let entity_path = std::str::from_utf8(entity_path_bytes)
            .map_err(|_| InternalError::commit_corruption())?;
        let key = read_len_prefixed_bytes(bytes, &mut cursor, "commit marker key")?;
        let flags = *bytes
            .get(cursor)
            .ok_or_else(InternalError::commit_corruption)?;
        cursor = cursor.saturating_add(1);
        if flags & !COMMIT_MARKER_FLAG_MASK != 0 {
            return Err(InternalError::commit_corruption());
        }

        let before = if flags & COMMIT_MARKER_FLAG_BEFORE != 0 {
            Some(
                read_len_prefixed_bytes(bytes, &mut cursor, "commit marker before payload")?
                    .to_vec(),
            )
        } else {
            None
        };
        let after = if flags & COMMIT_MARKER_FLAG_AFTER != 0 {
            Some(
                read_len_prefixed_bytes(bytes, &mut cursor, "commit marker after payload")?
                    .to_vec(),
            )
        } else {
            None
        };
        let schema_fingerprint = read_fixed_array::<COMMIT_MARKER_SCHEMA_FINGERPRINT_BYTES>(
            bytes,
            &mut cursor,
            "commit marker schema fingerprint",
        )?;

        row_ops.push(CommitRowOp::try_new_bytes(
            entity_path.to_owned(),
            key,
            before,
            after,
            schema_fingerprint,
        )?);
    }

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
        row_ops,
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
    // The 0.159 data-key format is variable-width, so this gate is a bounded
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
    // Phase 1: validate every row op under the shared marker invariant gate.
    for row_op in &marker.row_ops {
        validate_commit_row_op_shape(row_op)?;
    }

    // Phase 2: validate every embedded journal batch is bound to this marker
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

/// Validate one commit-marker row-op shape invariant.
///
/// This is the shared validation gate for both normal marker persistence and
/// the single-row hot path, so direct single-row encoding cannot bypass marker
/// row semantics.
pub(crate) fn validate_commit_row_op_shape(row_op: &CommitRowOp) -> Result<(), InternalError> {
    // Phase 1: reject row ops that cannot encode any mutation semantics.
    if row_op.entity_path.is_empty() {
        return Err(InternalError::commit_corruption());
    }
    if row_op.before.is_none() && row_op.after.is_none() {
        return Err(InternalError::commit_corruption());
    }

    // Phase 2: guard row payload size before durable persistence/recovery
    // preparation can classify oversized persisted bytes elsewhere.
    for (label, payload) in [
        ("before", row_op.before.as_ref()),
        ("after", row_op.after.as_ref()),
    ] {
        if let Some(bytes) = payload
            && bytes.len() > MAX_ROW_BYTES as usize
        {
            return Err(CommitMarker::row_op_payload_too_large(label, bytes.len()));
        }
    }

    // Phase 3: enforce data-key byte shape and semantic decode.
    DecodedDataStoreKey::try_from_raw(&row_op.key)
        .map_err(CommitMarker::row_op_key_decode_failed)?;

    Ok(())
}
