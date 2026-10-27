//! Module: index::entry
//! Responsibility: index-entry payload encode/decode and structural validation.
//! Does not own: commit ordering or unique-policy decisions.
//! Boundary: commit/index-store consume raw entries after prevalidation.

use crate::db::{
    index::{IndexKey, RawIndexStoreKey},
    key_taxonomy::{IndexEntryValue, PrimaryKeyValue},
};
use ic_storage::adapter::{Storable, storable::Bound};
use std::borrow::Cow;

///
/// Constants
///

const INDEX_ENTRY_WITNESS_BYTES: usize = 1;
const INDEX_ENTRY_WITNESS_PRESENT: u8 = 0;
pub(crate) const MAX_INDEX_ENTRY_BYTES: u32 = 1;

///
/// IndexEntryCorruption
///

#[derive(Debug)]
pub(crate) enum IndexEntryCorruption {
    TooLarge,

    LengthMismatch,

    InvalidKey,

    InvalidWitness,

    EmptyEntry,
}

///
/// IndexEntryRowWitness
///
/// One decoded row identity with a validated presence marker. The raw index key
/// owns the identity. Primary-row existence is checked independently by integrity
/// and row-fetch paths; the marker does not prove a primary row is readable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexEntryRowWitness {
    primary_key_value: PrimaryKeyValue,
}

impl IndexEntryRowWitness {
    const fn new(primary_key_value: &PrimaryKeyValue) -> Self {
        Self {
            primary_key_value: *primary_key_value,
        }
    }

    #[must_use]
    pub(in crate::db) const fn primary_key_value(&self) -> &PrimaryKeyValue {
        &self.primary_key_value
    }
}

///
/// IndexEntryValue
///

impl IndexEntryValue {
    #[must_use]
    pub(crate) fn presence() -> Self {
        Self::from_persisted_bytes(vec![INDEX_ENTRY_WITNESS_PRESENT])
    }

    /// Decode this key-owned raw entry and append its scalar-or-composite
    /// primary-key value if `limit` has not been reached.
    pub(in crate::db) fn push_row_identity_primary_key_values_limited<E>(
        &self,
        raw_key: &RawIndexStoreKey,
        out: &mut Vec<PrimaryKeyValue>,
        limit: usize,
        map_corruption: impl FnOnce(IndexEntryCorruption) -> E,
    ) -> Result<bool, E> {
        let row_witness = self.decode_row_witness(raw_key).map_err(map_corruption)?;
        out.push(*row_witness.primary_key_value());
        if out.len() >= limit {
            return Ok(true);
        }

        Ok(false)
    }

    // Validate the complete key frame without materialising unused components,
    // then validate the storage-owned existence witness.
    pub(in crate::db) fn decode_row_witness(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<IndexEntryRowWitness, IndexEntryCorruption> {
        let (primary_key_value, _) = IndexKey::primary_key_value_and_bytes_from_raw(raw_key)
            .map_err(|_| IndexEntryCorruption::InvalidKey)?;

        self.decode_row_witness_from_primary_key_value(&primary_key_value)
    }

    // Decode row identity from a caller-owned decoded index key. Index scans
    // often need the decoded key for predicate/components already, so this
    // avoids reparsing the same raw index key on the hot traversal path.
    pub(in crate::db) fn decode_row_witness_from_index_key(
        &self,
        key: &IndexKey,
    ) -> Result<IndexEntryRowWitness, IndexEntryCorruption> {
        let primary_key_value = key
            .primary_key_value()
            .map_err(|_| IndexEntryCorruption::InvalidKey)?;

        self.decode_row_witness_from_primary_key_value(&primary_key_value)
    }

    pub(in crate::db) fn decode_row_witness_from_primary_key_value(
        &self,
        primary_key_value: &PrimaryKeyValue,
    ) -> Result<IndexEntryRowWitness, IndexEntryCorruption> {
        self.validate()?;

        Ok(IndexEntryRowWitness::new(primary_key_value))
    }

    /// Validate the sole current presence marker without binding to an entity.
    pub(crate) fn validate(&self) -> Result<(), IndexEntryCorruption> {
        let bytes = self.as_bytes();
        if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
            return Err(IndexEntryCorruption::TooLarge);
        }
        if bytes.is_empty() {
            return Err(IndexEntryCorruption::EmptyEntry);
        }
        if bytes.len() != INDEX_ENTRY_WITNESS_BYTES {
            return Err(IndexEntryCorruption::LengthMismatch);
        }

        if bytes[0] != INDEX_ENTRY_WITNESS_PRESENT {
            return Err(IndexEntryCorruption::InvalidWitness);
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

impl Storable for IndexEntryValue {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self::from_persisted_bytes(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_INDEX_ENTRY_BYTES,
        is_fixed_size: false,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
