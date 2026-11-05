//! Test-only constants for the parent module.

use super::*;

impl RawDataStoreKey {
    pub(in crate::db) const ENTITY_TAG_SIZE_USIZE: usize = Self::ENTITY_TAG_SIZE_BYTES as usize;
}
