//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db::commit::store) fn encode_commit_marker_bytes(
    format_version: u8,
    marker_payload: &[u8],
) -> Result<Vec<u8>, InternalError> {
    if marker_payload.len() > u32::MAX as usize {
        return Err(InternalError::commit_marker_payload_exceeds_u32_length_limit());
    }

    let payload_len = u32::try_from(marker_payload.len())
        .map_err(|_| InternalError::commit_marker_payload_exceeds_u32_length_limit())?;
    let mut encoded =
        Vec::with_capacity(COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload.len()));
    // Tests intentionally vary the format version, so this helper cannot reuse
    // `write_commit_marker_envelope_header`, which always emits the live version.
    encoded.push(format_version);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(marker_payload);

    Ok(encoded)
}
