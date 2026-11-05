//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db::commit::store) fn encode_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    marker_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let marker_len = u32::try_from(marker_bytes.len())
        .map_err(|_| InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit())?;
    let mut encoded = Vec::new();
    write_current_control_prefix(
        &mut encoded,
        database_incarnation_id,
        cursor_authentication_key,
        0,
        &[],
    )?;
    encoded.extend_from_slice(&marker_len.to_le_bytes());
    encoded.extend_from_slice(marker_bytes);
    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size());
    }
    Ok(encoded)
}
