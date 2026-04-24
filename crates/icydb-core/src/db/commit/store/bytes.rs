//! Module: commit::store::bytes
//! Responsibility: local byte-cursor helpers for commit store envelopes.
//! Does not own: marker semantics, control-slot semantics, or persisted format policy.
//! Boundary: commit store envelope decoders -> bounded byte primitives.

use crate::error::InternalError;

// Read one little-endian u32 length from a bounded binary envelope.
pub(super) fn read_u32_le(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<u32, InternalError> {
    let next = cursor.saturating_add(4);
    let payload = bytes.get(*cursor..next).ok_or_else(|| {
        InternalError::commit_corruption(format!(
            "{label} decode failed: expected canonical envelope"
        ))
    })?;
    *cursor = next;

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}
