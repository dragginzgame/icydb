//! Test-only helpers for the parent module.

use super::*;

/// Encode one grouped continuation token as an external cursor token string.
pub(in crate::db) fn encode_grouped_cursor_token(
    token: &GroupedContinuationToken,
    mac_key: &[u8; 32],
) -> Result<String, TokenWireError> {
    token
        .encode(mac_key)
        .map(|encoded| encode_cursor(encoded.as_slice()))
}
