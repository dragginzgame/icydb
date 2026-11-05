//! Test-only constructors and adapters for the parent module.

use super::*;

impl RawRow {
    /// Construct a raw row from serialized bytes.
    pub(in crate::db) fn try_new(bytes: Vec<u8>) -> Result<Self, InternalError> {
        Self::from_untrusted_bytes(bytes)
    }
}
