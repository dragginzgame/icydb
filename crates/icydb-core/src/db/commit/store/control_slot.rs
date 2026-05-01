//! Module: commit::store::control_slot
//! Responsibility: encode/decode the durable commit control-slot envelope.
//! Does not own: stable-cell lifecycle, marker semantics, or recovery orchestration.
//! Boundary: commit store lifecycle -> control-slot bytes -> marker payload.

use crate::{
    db::commit::{
        marker::{
            CommitMarker, CommitRowOp, MAX_COMMIT_BYTES, commit_marker_payload_capacity,
            single_row_commit_marker_payload_capacity, validate_commit_marker_shape,
            validate_commit_row_op_shape, write_commit_marker_payload,
            write_single_row_commit_marker_payload,
        },
        store::{bytes::read_u32_le, marker_envelope::write_commit_marker_envelope_header},
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

pub(super) const COMMIT_CONTROL_HEADER_BYTES: usize = 9;
const COMMIT_CONTROL_MAGIC: [u8; 4] = *b"CMCS";
const COMMIT_CONTROL_STATE_VERSION_CURRENT: u8 = 1;
const COMMIT_MARKER_HEADER_BYTES: usize = 5;

// Build the canonical max-size corruption error for raw commit control bytes.
fn control_slot_exceeds_max_size(size: usize) -> InternalError {
    InternalError::commit_marker_exceeds_max_size(size, MAX_COMMIT_BYTES)
}

// Build the canonical control-slot canonical-envelope corruption error.
fn control_slot_canonical_envelope_required() -> InternalError {
    InternalError::commit_corruption("commit control-slot decode: expected envelope")
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
    if bytes.is_empty() {
        return Ok(CommitControlSlotRef { marker_bytes: &[] });
    }

    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_exceeds_max_size(bytes.len()));
    }
    if bytes.len() < COMMIT_CONTROL_HEADER_BYTES {
        return Err(control_slot_canonical_envelope_required());
    }

    let magic: [u8; 4] = bytes
        .get(..COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    if magic != COMMIT_CONTROL_MAGIC {
        return Err(InternalError::serialize_incompatible_persisted_format(
            "commit control-slot magic mismatch".to_string(),
        ));
    }

    let control_version = *bytes
        .get(COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(control_slot_canonical_envelope_required)?;
    if control_version != COMMIT_CONTROL_STATE_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format(
            format!(
                "commit control-slot version {control_version} is incompatible with runtime version {COMMIT_CONTROL_STATE_VERSION_CURRENT}",
            ),
        ));
    }

    let mut cursor = COMMIT_CONTROL_MAGIC.len() + 1;
    let marker_len = read_u32_le(bytes, &mut cursor, "commit control-slot")? as usize;
    let remaining = bytes.len().saturating_sub(cursor);
    if remaining != marker_len {
        return Err(control_slot_canonical_envelope_required());
    }

    let marker_end = cursor.saturating_add(marker_len);
    let marker_bytes = bytes
        .get(cursor..marker_end)
        .ok_or_else(control_slot_canonical_envelope_required)?;

    Ok(CommitControlSlotRef { marker_bytes })
}

// Encode marker payload bytes into the persisted control-slot format.
#[cfg(test)]
pub(super) fn encode_commit_control_slot(marker_bytes: &[u8]) -> Result<Vec<u8>, InternalError> {
    let encoded = encode_commit_control_slot_bytes(marker_bytes)?;

    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size(
            encoded.len(),
            MAX_COMMIT_BYTES,
        ));
    }

    Ok(encoded)
}

// Encode the full control slot for a multi-row marker directly so atomic batch
// opens do not allocate intermediate marker payload and marker-envelope buffers.
pub(super) fn encode_commit_control_slot_from_marker(
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    validate_commit_marker_shape(marker)?;

    let marker_payload_len = commit_marker_payload_capacity(marker);
    let lengths = checked_control_slot_lengths(marker_payload_len)?;

    let mut encoded = Vec::with_capacity(lengths.capacity);
    write_commit_control_slot_header(&mut encoded, lengths.marker_length);
    write_commit_marker_envelope_header(&mut encoded, lengths.payload_size)?;
    write_commit_marker_payload(&mut encoded, marker)?;

    Ok(encoded)
}

// Encode the full control slot for a single-row marker directly so hot
// save/delete opens do not allocate intermediate marker payload vectors.
pub(super) fn encode_single_row_commit_control_slot(
    marker_id: [u8; 16],
    row_op: &CommitRowOp,
) -> Result<Vec<u8>, InternalError> {
    validate_commit_row_op_shape(row_op)?;

    let marker_payload_len = single_row_commit_marker_payload_capacity(row_op);
    let lengths = checked_control_slot_lengths(marker_payload_len)?;

    let mut encoded = Vec::with_capacity(lengths.capacity);
    write_commit_control_slot_header(&mut encoded, lengths.marker_length);
    write_commit_marker_envelope_header(&mut encoded, lengths.payload_size)?;
    write_single_row_commit_marker_payload(&mut encoded, marker_id, row_op)?;

    Ok(encoded)
}

// Validate and materialize the shared control-slot lengths used by the direct
// multi-row and single-row marker encoders.
fn checked_control_slot_lengths(
    marker_payload_len: usize,
) -> Result<ControlSlotLengths, InternalError> {
    let marker_bytes_len = COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload_len);
    let marker_len = u32::try_from(marker_bytes_len).map_err(|_| {
        InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit(marker_bytes_len)
    })?;
    let total_len = COMMIT_CONTROL_HEADER_BYTES.saturating_add(marker_bytes_len);
    if total_len > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size(
            total_len,
            MAX_COMMIT_BYTES,
        ));
    }

    Ok(ControlSlotLengths {
        payload_size: marker_payload_len,
        marker_length: marker_len,
        capacity: total_len,
    })
}

// Encode the stable control-slot frame directly so recovery only reads one
// bounded binary envelope before marker decode.
#[cfg(test)]
fn encode_commit_control_slot_bytes(marker_bytes: &[u8]) -> Result<Vec<u8>, InternalError> {
    let mut encoded =
        Vec::with_capacity(COMMIT_CONTROL_HEADER_BYTES.saturating_add(marker_bytes.len()));
    let marker_len = u32::try_from(marker_bytes.len()).map_err(|_| {
        InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit(marker_bytes.len())
    })?;
    write_commit_control_slot_header(&mut encoded, marker_len);
    encoded.extend_from_slice(marker_bytes);

    Ok(encoded)
}

// Write the shared commit control-slot header used by all marker write paths.
fn write_commit_control_slot_header(out: &mut Vec<u8>, marker_len: u32) {
    out.extend_from_slice(&COMMIT_CONTROL_MAGIC);
    out.push(COMMIT_CONTROL_STATE_VERSION_CURRENT);
    out.extend_from_slice(&marker_len.to_le_bytes());
}
