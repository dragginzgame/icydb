//! Module: db::commit::store::control_slot
//! Responsibility: encode/decode the durable commit control-slot envelope.
//! Does not own: stable-cell lifecycle, marker semantics, or recovery orchestration.
//! Boundary: commit store lifecycle -> control-slot bytes -> marker payload.

use crate::{
    db::{
        commit::{
            marker::{
                CommitMarker, MAX_COMMIT_BYTES, commit_marker_payload_capacity,
                validate_commit_marker_shape, write_commit_marker_payload,
            },
            store::{bytes::read_u32_le, marker_envelope::write_commit_marker_envelope_header},
        },
        integrity::DatabaseIncarnationId,
    },
    error::InternalError,
};

///
/// CommitControlSlotRef
///
/// Borrowed view of one decoded commit control-slot envelope.
/// This keeps hot-path marker checks allocation-free while preserving the
/// same strict control-slot validation contract as the owned decode helper.
///

pub(super) struct CommitControlSlotRef<'a> {
    pub(super) database_incarnation_id: DatabaseIncarnationId,
    pub(super) marker_bytes: &'a [u8],
}

///
/// ControlSlotLengths
///
/// Checked control-slot length fields shared by direct marker encoders.
/// This keeps the multi-row and single-row fast paths on separate payload
/// writers while centralizing the persisted envelope size checks.
///

struct ControlSlotLengths {
    payload_size: usize,
    marker_length: u32,
    capacity: usize,
}

pub(super) const COMMIT_CONTROL_HEADER_BYTES: usize = 25;
const COMMIT_CONTROL_MAGIC: [u8; 4] = *b"ICCS";
const COMMIT_CONTROL_STATE_VERSION_CURRENT: u8 = 1;
const DATABASE_INCARNATION_BYTES: usize = 16;
const COMMIT_MARKER_HEADER_BYTES: usize = 5;

// Build the canonical max-size corruption error for raw commit control bytes.
fn control_slot_exceeds_max_size() -> InternalError {
    InternalError::commit_marker_exceeds_max_size()
}

// Build the canonical control-slot canonical-envelope corruption error.
fn control_slot_canonical_envelope_required() -> InternalError {
    InternalError::commit_corruption()
}

// Decode commit control-slot bytes into marker payload bytes.
//
// Compatibility contract:
// - only the canonical control-slot envelope is accepted
pub(super) fn decode_commit_control_slot(bytes: &[u8]) -> Result<Vec<u8>, InternalError> {
    let slot = inspect_commit_control_slot(bytes)?;

    Ok(slot.marker_bytes.to_vec())
}

// Inspect commit control-slot bytes under the canonical envelope without
// allocating an owned marker payload.
pub(super) fn inspect_commit_control_slot(
    bytes: &[u8],
) -> Result<CommitControlSlotRef<'_>, InternalError> {
    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_exceeds_max_size());
    }
    let encoded_len = commit_control_slot_encoded_len(bytes)?;
    if bytes.len() != encoded_len {
        return Err(control_slot_canonical_envelope_required());
    }

    let incarnation_start = COMMIT_CONTROL_MAGIC.len() + 1;
    let incarnation_end = incarnation_start + DATABASE_INCARNATION_BYTES;
    let incarnation_bytes: [u8; DATABASE_INCARNATION_BYTES] = bytes
        .get(incarnation_start..incarnation_end)
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    let database_incarnation_id = DatabaseIncarnationId::try_from_bytes(incarnation_bytes)?;
    let marker_bytes = bytes
        .get(COMMIT_CONTROL_HEADER_BYTES..encoded_len)
        .ok_or_else(control_slot_canonical_envelope_required)?;

    Ok(CommitControlSlotRef {
        database_incarnation_id,
        marker_bytes,
    })
}

/// Return the total encoded control-slot length from a bounded header prefix.
pub(super) fn commit_control_slot_encoded_len(bytes: &[u8]) -> Result<usize, InternalError> {
    if bytes.len() < COMMIT_CONTROL_HEADER_BYTES {
        return Err(control_slot_canonical_envelope_required());
    }

    let magic: [u8; 4] = bytes
        .get(..COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    if magic != COMMIT_CONTROL_MAGIC {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let control_version = *bytes
        .get(COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(control_slot_canonical_envelope_required)?;
    if control_version != COMMIT_CONTROL_STATE_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let mut cursor = COMMIT_CONTROL_MAGIC.len() + 1 + DATABASE_INCARNATION_BYTES;
    let marker_len = read_u32_le(bytes, &mut cursor, "commit control-slot")? as usize;
    let encoded_len = cursor.saturating_add(marker_len);
    if encoded_len > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_exceeds_max_size());
    }

    Ok(encoded_len)
}

/// Encode the canonical empty commit-control slot.
pub(super) fn encode_empty_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(COMMIT_CONTROL_HEADER_BYTES);
    write_commit_control_slot_header(&mut encoded, database_incarnation_id, 0);
    encoded
}

// Encode marker payload bytes into the persisted control-slot format.
#[cfg(test)]
pub(super) fn encode_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
    marker_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let encoded = encode_commit_control_slot_bytes(database_incarnation_id, marker_bytes)?;

    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size());
    }

    Ok(encoded)
}

// Encode the full control slot for a multi-row marker directly so atomic batch
// opens do not allocate intermediate marker payload and marker-envelope buffers.
pub(super) fn encode_commit_control_slot_from_marker(
    database_incarnation_id: DatabaseIncarnationId,
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    validate_commit_marker_shape(marker)?;

    let marker_payload_len = commit_marker_payload_capacity(marker);
    let lengths = checked_control_slot_lengths(marker_payload_len)?;

    let mut encoded = Vec::with_capacity(lengths.capacity);
    write_commit_control_slot_header(&mut encoded, database_incarnation_id, lengths.marker_length);
    write_commit_marker_envelope_header(&mut encoded, lengths.payload_size)?;
    write_commit_marker_payload(&mut encoded, marker)?;

    Ok(encoded)
}

// Validate and materialize the shared control-slot lengths used by the direct
// marker encoder.
fn checked_control_slot_lengths(
    marker_payload_len: usize,
) -> Result<ControlSlotLengths, InternalError> {
    let marker_bytes_len = COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload_len);
    let marker_len = u32::try_from(marker_bytes_len)
        .map_err(|_| InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit())?;
    let total_len = COMMIT_CONTROL_HEADER_BYTES.saturating_add(marker_bytes_len);
    if total_len > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size());
    }

    Ok(ControlSlotLengths {
        payload_size: marker_payload_len,
        marker_length: marker_len,
        capacity: total_len,
    })
}

/// Return the exact current control-slot length for a marker payload when it fits.
#[must_use]
#[cfg(feature = "sql")]
pub(in crate::db::commit) fn commit_control_slot_encoded_len_for_marker_payload(
    marker_payload_len: usize,
) -> Option<usize> {
    checked_control_slot_lengths(marker_payload_len)
        .ok()
        .map(|lengths| lengths.capacity)
}

// Encode the stable control-slot frame directly so recovery only reads one
// bounded binary envelope before marker decode.
#[cfg(test)]
fn encode_commit_control_slot_bytes(
    database_incarnation_id: DatabaseIncarnationId,
    marker_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let mut encoded =
        Vec::with_capacity(COMMIT_CONTROL_HEADER_BYTES.saturating_add(marker_bytes.len()));
    let marker_len = u32::try_from(marker_bytes.len())
        .map_err(|_| InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit())?;
    write_commit_control_slot_header(&mut encoded, database_incarnation_id, marker_len);
    encoded.extend_from_slice(marker_bytes);

    Ok(encoded)
}

// Write the shared commit control-slot header used by all marker write paths.
fn write_commit_control_slot_header(
    out: &mut Vec<u8>,
    database_incarnation_id: DatabaseIncarnationId,
    marker_len: u32,
) {
    out.extend_from_slice(&COMMIT_CONTROL_MAGIC);
    out.push(COMMIT_CONTROL_STATE_VERSION_CURRENT);
    out.extend_from_slice(&database_incarnation_id.to_bytes());
    out.extend_from_slice(&marker_len.to_le_bytes());
}
