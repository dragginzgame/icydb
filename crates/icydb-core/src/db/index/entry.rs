//! Module: index::entry
//! Responsibility: index-entry payload encode/decode and structural validation.
//! Does not own: commit ordering or unique-policy decisions.
//! Boundary: commit/index-store consume raw entries after prevalidation.

use crate::{
    db::{
        index::{IndexKey, RawIndexStoreKey},
        key_taxonomy::{IndexEntryValue, PrimaryKeyValue},
    },
    traits::Storable,
};
use ic_memory::stable_structures::storable::Bound;
use std::borrow::Cow;

///
/// Constants
///

const INDEX_ENTRY_WITNESS_BYTES: usize = 1;
const INDEX_ENTRY_WITNESS_PRESENT: u8 = 0;
const INDEX_ENTRY_WITNESS_MISSING: u8 = 1;
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
/// IndexRowIdentity
///

#[derive(Clone, Debug)]
pub(crate) struct IndexRowIdentity {
    primary_key_value: PrimaryKeyValue,
}

///
/// IndexEntryExistenceWitness
///
/// Narrow storage-owned row-existence witness carried per raw index entry.
/// `Present` is the normal encoded state; `Missing` exists so the stale-entry
/// repair path can preserve the secondary entry while still exposing one
/// explicit storage-level missing-row witness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexEntryExistenceWitness {
    Present,
    Missing,
}

impl IndexEntryExistenceWitness {
    const fn to_stored_byte(self) -> u8 {
        match self {
            Self::Present => INDEX_ENTRY_WITNESS_PRESENT,
            Self::Missing => INDEX_ENTRY_WITNESS_MISSING,
        }
    }

    const fn try_from_stored_byte(byte: u8) -> Result<Self, IndexEntryCorruption> {
        match byte {
            INDEX_ENTRY_WITNESS_PRESENT => Ok(Self::Present),
            INDEX_ENTRY_WITNESS_MISSING => Ok(Self::Missing),
            _ => Err(IndexEntryCorruption::InvalidWitness),
        }
    }
}

///
/// IndexEntryRowWitness
///
/// One decoded raw index-entry row identity plus its storage-owned existence
/// witness. The raw index key owns the identity; the raw entry value only
/// proves whether that key currently has a row.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexEntryRowWitness {
    primary_key_value: PrimaryKeyValue,
    existence_witness: IndexEntryExistenceWitness,
}

impl IndexEntryRowWitness {
    const fn new(
        primary_key_value: &PrimaryKeyValue,
        existence_witness: IndexEntryExistenceWitness,
    ) -> Self {
        Self {
            primary_key_value: *primary_key_value,
            existence_witness,
        }
    }

    #[must_use]
    pub(in crate::db) const fn primary_key_value(&self) -> &PrimaryKeyValue {
        &self.primary_key_value
    }

    #[must_use]
    pub(in crate::db) const fn existence_witness(self) -> IndexEntryExistenceWitness {
        self.existence_witness
    }
}

impl IndexRowIdentity {
    #[must_use]
    pub(crate) const fn new(primary_key_value: &PrimaryKeyValue) -> Self {
        Self {
            primary_key_value: *primary_key_value,
        }
    }

    #[must_use]
    pub(crate) fn contains(&self, primary_key_value: &PrimaryKeyValue) -> bool {
        &self.primary_key_value == primary_key_value
    }

    #[must_use]
    pub(crate) const fn primary_key_value(&self) -> &PrimaryKeyValue {
        &self.primary_key_value
    }
}

///
/// IndexEntryValue
///

impl IndexEntryValue {
    #[must_use]
    pub(crate) fn presence() -> Self {
        Self::from_persisted_bytes(vec![IndexEntryExistenceWitness::Present.to_stored_byte()])
    }

    pub(crate) fn decode_row_identity(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<IndexRowIdentity, IndexEntryCorruption> {
        self.decode_row_witness(raw_key)
            .map(|witness| IndexRowIdentity::new(witness.primary_key_value()))
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

    // Decode the key-owned raw entry row identity plus its storage-owned
    // existence witness without allocating a temporary vector.
    pub(in crate::db) fn decode_row_witness(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<IndexEntryRowWitness, IndexEntryCorruption> {
        let witness = self.validate_witness()?;
        let primary_key_value = primary_key_value_from_raw_index_store_key(raw_key)?;

        Ok(IndexEntryRowWitness::new(&primary_key_value, witness))
    }

    /// Validate the raw index entry structure without binding to an entity.
    pub(crate) fn validate(&self) -> Result<(), IndexEntryCorruption> {
        self.validate_witness().map(|_| ())
    }

    // Validate the raw index-entry witness payload. Row identity now belongs to
    // `RawIndexStoreKey`; the value carries only a storage-owned existence witness.
    fn validate_witness(&self) -> Result<IndexEntryExistenceWitness, IndexEntryCorruption> {
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

        IndexEntryExistenceWitness::try_from_stored_byte(bytes[0])
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

fn primary_key_value_from_raw_index_store_key(
    raw_key: &RawIndexStoreKey,
) -> Result<PrimaryKeyValue, IndexEntryCorruption> {
    let key = IndexKey::try_from_raw(raw_key).map_err(|_| IndexEntryCorruption::InvalidKey)?;
    key.primary_key_value()
        .map_err(|_| IndexEntryCorruption::InvalidKey)
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
