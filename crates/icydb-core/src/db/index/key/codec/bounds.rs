//! Module: index::key::codec::bounds
//! Responsibility: byte-size bounds/constants for index-key framing.
//! Does not own: parsing logic or semantic key construction.
//! Boundary: consumed by codec encode/decode helpers.

use crate::{
    MAX_INDEX_FIELDS,
    db::{data::StorageKey, identity::IndexName, index::key::codec::IndexKey},
};

pub(super) const KEY_KIND_TAG_SIZE: usize = 1;
pub(super) const COMPONENT_COUNT_SIZE: usize = 1;
pub(super) const SEGMENT_LEN_SIZE: usize = 2;
pub(super) const INDEX_ID_SIZE: usize = IndexName::STORED_SIZE_USIZE;
pub(super) const KEY_PREFIX_SIZE: usize = KEY_KIND_TAG_SIZE + INDEX_ID_SIZE + COMPONENT_COUNT_SIZE;

#[expect(clippy::cast_possible_truncation)]
impl IndexKey {
    pub(crate) const MAX_COMPONENT_SIZE: usize = 4 * 1024;
    pub(crate) const MAX_PK_SIZE: usize = StorageKey::STORED_SIZE_USIZE;

    const MIN_SEGMENT_SIZE: usize = 1;

    /// Maximum on-disk size in bytes (stable, protocol-level bound)
    pub(crate) const MAX_INDEX_KEY_BYTES: u64 = (KEY_PREFIX_SIZE
        + (MAX_INDEX_FIELDS * (SEGMENT_LEN_SIZE + Self::MAX_COMPONENT_SIZE))
        + (SEGMENT_LEN_SIZE + Self::MAX_PK_SIZE))
        as u64;

    /// Maximum on-disk size in bytes (stable, protocol-level bound)
    pub(crate) const STORED_SIZE_BYTES: u64 = Self::MAX_INDEX_KEY_BYTES;

    /// Maximum in-memory size (for bounds checks)
    pub(crate) const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    /// Minimum encoded size for an empty index key.
    pub(crate) const MIN_STORED_SIZE_BYTES: u64 =
        (KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + Self::MIN_SEGMENT_SIZE) as u64;

    /// Minimum encoded size for an empty index key.
    pub(crate) const MIN_STORED_SIZE_USIZE: usize = Self::MIN_STORED_SIZE_BYTES as usize;

    #[must_use]
    pub(crate) fn wildcard_low_component() -> Vec<u8> {
        vec![0]
    }

    #[must_use]
    pub(crate) fn wildcard_high_component() -> Vec<u8> {
        vec![0xFF; Self::MAX_COMPONENT_SIZE]
    }

    #[must_use]
    pub(crate) fn wildcard_low_pk() -> Vec<u8> {
        vec![0]
    }

    #[must_use]
    pub(crate) fn wildcard_high_pk() -> Vec<u8> {
        vec![0xFF; Self::MAX_PK_SIZE]
    }
}
