//! Module: commit::store::marker_envelope
//! Responsibility: encode/decode the versioned commit-marker envelope.
//! Does not own: control-slot framing, stable-cell lifecycle, or marker semantics.
//! Boundary: control-slot marker bytes -> marker envelope -> marker payload codec.

use crate::{
    db::commit::{
        marker::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, MAX_COMMIT_BYTES,
            decode_commit_marker_payload,
        },
        store::bytes::read_u32_le,
    },
    error::InternalError,
};

const COMMIT_MARKER_HEADER_BYTES: usize = 5;

// Build the canonical max-size corruption error for raw commit marker bytes.
fn marker_exceeds_max_size(size: usize) -> InternalError {
    InternalError::commit_marker_exceeds_max_size(size, MAX_COMMIT_BYTES)
}

// Build the canonical marker-envelope canonical-envelope corruption error.
fn marker_canonical_envelope_required() -> InternalError {
    InternalError::commit_corruption("commit marker decode: expected envelope")
}

// Decode one commit marker with strict envelope semantics.
pub(super) fn decode_commit_marker(bytes: &[u8]) -> Result<CommitMarker, InternalError> {
    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(marker_exceeds_max_size(bytes.len()));
    }

    let (format_version, marker_payload) = decode_commit_marker_bytes(bytes)?;
    validate_commit_marker_format_version(format_version)?;

    decode_commit_marker_payload(&marker_payload)
}

// Validate marker envelope version against the single supported format.
fn validate_commit_marker_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == COMMIT_MARKER_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    Err(InternalError::serialize_incompatible_persisted_format(
        format!(
            "commit marker format version {format_version} is unsupported by runtime version {COMMIT_MARKER_FORMAT_VERSION_CURRENT}",
        ),
    ))
}

// Write the shared versioned marker-envelope header.
pub(super) fn write_commit_marker_envelope_header(
    out: &mut Vec<u8>,
    marker_payload_len: usize,
) -> Result<(), InternalError> {
    out.push(COMMIT_MARKER_FORMAT_VERSION_CURRENT);
    out.extend_from_slice(
        &(u32::try_from(marker_payload_len).map_err(|_| {
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                marker_payload_len,
            )
        })?)
        .to_le_bytes(),
    );

    Ok(())
}

// Encode the versioned marker envelope directly so only the marker payload
// itself still uses persisted-payload decode.
#[cfg(test)]
pub(super) fn encode_commit_marker_bytes(
    format_version: u8,
    marker_payload: &[u8],
) -> Result<Vec<u8>, InternalError> {
    if marker_payload.len() > u32::MAX as usize {
        return Err(
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                marker_payload.len(),
            ),
        );
    }

    let payload_len = u32::try_from(marker_payload.len()).map_err(|_| {
        InternalError::commit_marker_payload_exceeds_u32_length_limit(
            "commit marker payload",
            marker_payload.len(),
        )
    })?;
    let mut encoded =
        Vec::with_capacity(COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload.len()));
    // Tests intentionally vary the format version, so this helper cannot reuse
    // `write_commit_marker_envelope_header`, which always emits the live version.
    encoded.push(format_version);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(marker_payload);

    Ok(encoded)
}

// Decode the marker envelope without routing through generic tuple deserialization.
fn decode_commit_marker_bytes(bytes: &[u8]) -> Result<(u8, Vec<u8>), InternalError> {
    if bytes.len() < COMMIT_MARKER_HEADER_BYTES {
        return Err(marker_canonical_envelope_required());
    }

    let format_version = bytes[0];
    let mut cursor = 1;
    let payload_len = read_u32_le(bytes, &mut cursor, "commit marker")? as usize;
    let payload = bytes
        .get(cursor..)
        .ok_or_else(marker_canonical_envelope_required)?;
    if payload.len() != payload_len {
        return Err(marker_canonical_envelope_required());
    }

    Ok((format_version, payload.to_vec()))
}
