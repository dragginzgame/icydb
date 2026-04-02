//! Module: commit::marker
//! Responsibility: define persisted commit-marker payloads and marker-shape validation.
//! Does not own: marker storage backend, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::{prepare,recovery,store} -> commit::marker (one-way).

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::prepared_op::PreparedIndexDeltaKind,
        data::{DataKey, RawDataKey},
        index::{IndexStore, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    types::Ulid,
};
use canic_cdk::structures::Storable;
use std::{borrow::Cow, cell::RefCell, thread::LocalKey};

// Commit-marker durability invariant:
// - Persist one marker before any stable mutation.
// - After marker persistence, apply/recovery consume only marker payloads.
// - Recovery replays marker row ops deterministically.
// This makes partial mutations deterministic without a WAL.

pub(crate) const COMMIT_LABEL: &str = "CommitMarker";
/// Stored commit-id byte width shared by marker and guard paths.
pub(in crate::db) const COMMIT_ID_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_BYTES: usize = 16;
pub(in crate::db) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 1;

pub(in crate::db) type CommitSchemaFingerprint = [u8; COMMIT_SCHEMA_FINGERPRINT_BYTES];

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
    pub(crate) key: RawDataKey,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) schema_fingerprint: CommitSchemaFingerprint,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub(crate) fn new(
        entity_path: impl Into<Cow<'static, str>>,
        key: RawDataKey,
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
    /// This is the decode and migration boundary for callers that still own
    /// opaque key bytes rather than a typed `RawDataKey`.
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
    pub(crate) store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexKey,
    pub(crate) value: Option<RawIndexEntry>,
    pub(crate) delta_kind: PreparedIndexDeltaKind,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering row-level operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption inside one marker payload version.
/// Cross-version compatibility is owned by the versioned marker envelope in `commit::store`.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(crate) row_ops: Vec<CommitRowOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub(crate) fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = generate_commit_id()?;

        Ok(Self { id, row_ops })
    }

    // Build the canonical payload corruption for truncated variable-length fields.
    fn payload_truncated_length(label: &'static str) -> InternalError {
        InternalError::commit_corruption(format!("{label} decode failed: truncated length",))
    }

    // Build the canonical payload corruption for truncated byte payloads.
    fn payload_truncated_bytes(label: &'static str) -> InternalError {
        InternalError::commit_corruption(format!("{label} decode failed: truncated bytes",))
    }

    // Build the canonical payload corruption for invalid fixed-size payloads.
    fn payload_invalid_fixed_size(label: &'static str) -> InternalError {
        InternalError::commit_corruption(format!(
            "{label} decode failed: invalid fixed-size payload",
        ))
    }

    // Build the canonical row-op corruption for oversized row payloads.
    fn row_op_payload_too_large(label: &str, len: usize) -> InternalError {
        InternalError::commit_corruption(format!(
            "row op {label} payload exceeds max size: {len} bytes (limit {MAX_ROW_BYTES})",
        ))
    }

    // Build the canonical row-op corruption for key decode failures.
    fn row_op_key_decode_failed(err: impl std::fmt::Display) -> InternalError {
        InternalError::commit_corruption(format!("row op key decode: {err}"))
    }
}

const COMMIT_MARKER_ID_BYTES: usize = COMMIT_ID_BYTES;
const COMMIT_MARKER_SCHEMA_FINGERPRINT_BYTES: usize = COMMIT_SCHEMA_FINGERPRINT_BYTES;
const COMMIT_MARKER_FLAG_BEFORE: u8 = 0b0000_0001;
const COMMIT_MARKER_FLAG_AFTER: u8 = 0b0000_0010;
const COMMIT_MARKER_FLAG_MASK: u8 = COMMIT_MARKER_FLAG_BEFORE | COMMIT_MARKER_FLAG_AFTER;
const COMMIT_MARKER_ROW_COUNT_BYTES: usize = 4;

/// Generate one fresh commit id for marker persistence.
pub(in crate::db) fn generate_commit_id() -> Result<[u8; COMMIT_ID_BYTES], InternalError> {
    Ulid::try_generate()
        .map_err(InternalError::commit_id_generation_failed)
        .map(|ulid| ulid.to_bytes())
}

/// Encode one commit-marker payload in the canonical binary format.
#[cfg(test)]
pub(in crate::db) fn encode_commit_marker_payload(
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: size the output once so commit persistence writes one compact frame.
    let capacity = commit_marker_payload_capacity(marker);
    if capacity > u32::MAX as usize {
        return Err(
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                capacity,
            ),
        );
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
        return Err(
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                capacity,
            ),
        );
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
}

/// Return the canonical multi-row marker payload size without allocating it.
pub(in crate::db) fn commit_marker_payload_capacity(marker: &CommitMarker) -> usize {
    let mut capacity = COMMIT_MARKER_ID_BYTES + COMMIT_MARKER_ROW_COUNT_BYTES;
    for row_op in &marker.row_ops {
        capacity = capacity.saturating_add(commit_row_op_payload_capacity(row_op));
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

    Ok(())
}

// Return the canonical encoded payload size contribution for one row op.
fn commit_row_op_payload_capacity(row_op: &CommitRowOp) -> usize {
    let mut capacity = 4 + row_op.entity_path.len();
    capacity = capacity
        .saturating_add(4 + DataKey::STORED_SIZE_USIZE)
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
        return Err(InternalError::commit_corruption(
            "commit marker payload decode: truncated header",
        ));
    }

    let mut cursor = 0;
    let id = read_fixed_array::<COMMIT_MARKER_ID_BYTES>(bytes, &mut cursor, "commit marker id")?;
    let row_op_count = read_len_u32(bytes, &mut cursor, "commit marker row count")? as usize;
    let mut row_ops = Vec::with_capacity(row_op_count);

    // Phase 2: parse each length-delimited row op without routing through generic decode.
    for _ in 0..row_op_count {
        let entity_path_bytes =
            read_len_prefixed_bytes(bytes, &mut cursor, "commit marker entity_path")?;
        let entity_path = std::str::from_utf8(entity_path_bytes).map_err(|_| {
            InternalError::commit_corruption("commit marker payload decode: entity_path not utf-8")
        })?;
        let key = read_len_prefixed_bytes(bytes, &mut cursor, "commit marker key")?;
        let flags = *bytes.get(cursor).ok_or_else(|| {
            InternalError::commit_corruption("commit marker payload decode: truncated row-op flags")
        })?;
        cursor = cursor.saturating_add(1);
        if flags & !COMMIT_MARKER_FLAG_MASK != 0 {
            return Err(InternalError::commit_corruption(
                "commit marker payload decode: invalid row-op flags",
            ));
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

    // Phase 3: reject trailing bytes so malformed payloads fail closed.
    if cursor != bytes.len() {
        return Err(InternalError::commit_corruption(
            "commit marker payload decode: trailing bytes after payload",
        ));
    }

    Ok(CommitMarker { id, row_ops })
}

// Write one bounded little-endian u32 length field.
fn write_len_u32(out: &mut Vec<u8>, len: usize, label: &'static str) -> Result<(), InternalError> {
    let len = u32::try_from(len)
        .map_err(|_| InternalError::commit_marker_payload_exceeds_u32_length_limit(label, len))?;
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

/// Decode a raw data key and validate its structural invariants.
pub(in crate::db) fn decode_data_key(bytes: &[u8]) -> Result<(RawDataKey, DataKey), InternalError> {
    // Phase 1: enforce fixed-size contract for data keys.
    let len = bytes.len();
    let expected = DataKey::STORED_SIZE_USIZE;
    if len != expected {
        return Err(InternalError::commit_component_length_invalid(
            "data key", len, expected,
        ));
    }

    // Phase 2: decode and validate key shape.
    let raw = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    let data_key = DataKey::try_from_raw(&raw)
        .map_err(|err| InternalError::commit_component_corruption("data key", err))?;

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
    // Phase 1: reject row ops that cannot encode any mutation semantics.
    for row_op in &marker.row_ops {
        if row_op.entity_path.is_empty() {
            return Err(InternalError::commit_corruption(
                "row op has empty entity_path",
            ));
        }
        if row_op.before.is_none() && row_op.after.is_none() {
            return Err(InternalError::commit_corruption(
                "row op has neither before nor after payload",
            ));
        }

        // Phase 2: guard row payload size at marker-decode boundary so recovery
        // does not classify oversized persisted bytes during apply preparation.
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
        DataKey::try_from_raw(&row_op.key).map_err(CommitMarker::row_op_key_decode_failed)?;
    }

    Ok(())
}
