//! Module: db::cursor::signature
//! Responsibility: deterministic continuation signature payload ownership.
//! Does not own: cursor token decode policy or planner continuation validation.
//! Boundary: carries and displays the fixed-width semantic signature used by cursor checks.

use crate::db::codec::hex::encode_hex_lower;
use std::fmt;

///
/// ContinuationSignature
///
/// Stable, deterministic hash of continuation-relevant plan semantics.
/// Excludes windowing state (`limit`, `offset`) and cursor boundaries.
/// Hex/display formatting is signature-owned and delegates only the primitive
/// byte formatting to `db::codec`.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContinuationSignature([u8; 32]);

impl ContinuationSignature {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Encode this signature as lowercase hexadecimal text.
    #[must_use]
    pub fn as_hex(&self) -> String {
        encode_hex_lower(&self.0)
    }
}

impl fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_hex())
    }
}
