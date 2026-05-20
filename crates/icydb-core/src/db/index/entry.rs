//! Module: index::entry
//! Responsibility: index-entry payload encode/decode and structural validation.
//! Does not own: commit ordering or unique-policy decisions.
//! Boundary: commit/index-store consume raw entries after prevalidation.

use crate::{
    db::{
        data::StorageKey,
        index::{IndexKey, RawIndexStoreKey},
        key_taxonomy::IndexEntryValue,
    },
    traits::Storable,
    value::{Value, storage_key_as_runtime_value},
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;
use thiserror::Error as ThisError;

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

#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryCorruption {
    #[error("index entry exceeds max size")]
    TooLarge { len: usize },

    #[error("index entry length does not match key count")]
    LengthMismatch,

    #[error("index entry contains invalid key bytes")]
    InvalidKey,

    #[error("index entry contains invalid existence witness")]
    InvalidWitness,

    #[error("index entry contains zero keys")]
    EmptyEntry,

    #[error("index entry missing expected entity key: {entity_key:?} (index {index_key:?})")]
    MissingKey {
        index_key: Box<RawIndexStoreKey>,
        entity_key: Value,
    },
}

impl IndexEntryCorruption {
    #[must_use]
    pub(crate) fn missing_key(index_key: RawIndexStoreKey, entity_key: StorageKey) -> Self {
        Self::MissingKey {
            index_key: Box::new(index_key),
            entity_key: storage_key_as_runtime_value(&entity_key),
        }
    }
}

///
/// IndexEntry
///

#[cfg(test)]
#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryEncodeError {
    #[error("index entry test constructor received more than one key: {keys}")]
    TooManyKeys { keys: usize },

    #[error("index entry test constructor received no keys")]
    EmptyEntry,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexEntry {
    id: StorageKey,
}

///
/// IndexEntryExistenceWitness
///
/// Narrow storage-owned row-existence witness carried per raw index-entry
/// membership. `Present` is the normal encoded state; `Missing` exists so the
/// stale-entry repair path can preserve the secondary entry while
/// still exposing one explicit storage-level missing-row witness.
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
/// IndexEntryMembership
///
/// One decoded raw index-entry membership plus its storage-owned existence
/// witness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexEntryMembership {
    storage_key: StorageKey,
    existence_witness: IndexEntryExistenceWitness,
}

impl IndexEntryMembership {
    const fn new(storage_key: StorageKey, existence_witness: IndexEntryExistenceWitness) -> Self {
        Self {
            storage_key,
            existence_witness,
        }
    }

    #[must_use]
    pub(in crate::db) const fn storage_key(self) -> StorageKey {
        self.storage_key
    }

    #[must_use]
    pub(in crate::db) const fn existence_witness(self) -> IndexEntryExistenceWitness {
        self.existence_witness
    }
}

impl IndexEntry {
    #[must_use]
    pub(crate) const fn new(id: StorageKey) -> Self {
        Self { id }
    }

    #[must_use]
    pub(crate) const fn storage_key(&self) -> StorageKey {
        self.id
    }

    #[must_use]
    pub(crate) fn contains(&self, id: StorageKey) -> bool {
        self.id == id
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

    #[must_use]
    pub(crate) fn from_entry(_entry: &IndexEntry) -> Self {
        Self::presence()
    }

    pub(crate) fn try_decode_for_key(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<IndexEntry, IndexEntryCorruption> {
        self.decode_storage_key(raw_key).map(IndexEntry::new)
    }

    /// Decode this key-owned raw entry and append its storage key if `limit`
    /// has not been reached.
    ///
    /// The raw index key owns row identity; the raw value only validates the
    /// existence witness. This helper exists for structural preflight readers
    /// that do not need the witness itself.
    pub(in crate::db) fn push_membership_storage_keys_limited<E>(
        &self,
        raw_key: &RawIndexStoreKey,
        out: &mut Vec<StorageKey>,
        limit: usize,
        map_corruption: impl FnOnce(IndexEntryCorruption) -> E,
    ) -> Result<bool, E> {
        let membership = self
            .decode_single_membership(raw_key)
            .map_err(map_corruption)?;
        out.push(membership.storage_key());
        if out.len() >= limit {
            return Ok(true);
        }

        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn try_from_keys<I>(keys: I) -> Result<Self, IndexEntryEncodeError>
    where
        I: IntoIterator<Item = StorageKey>,
    {
        // Phase 1: bound-check key cardinality.
        let count = keys.into_iter().count();

        if count == 0 {
            return Err(IndexEntryEncodeError::EmptyEntry);
        }
        if count > 1 {
            return Err(IndexEntryEncodeError::TooManyKeys { keys: count });
        }

        Ok(Self::presence())
    }

    pub(crate) fn decode_keys(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<Vec<StorageKey>, IndexEntryCorruption> {
        self.decode_storage_key(raw_key).map(|key| vec![key])
    }

    // Decode the key-owned raw entry row identity without allocating a
    // temporary membership vector.
    pub(in crate::db) fn decode_storage_key(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<StorageKey, IndexEntryCorruption> {
        Ok(self.decode_single_membership(raw_key)?.storage_key())
    }

    // Decode the key-owned raw entry membership plus its storage-owned
    // existence witness without allocating a temporary membership vector.
    pub(in crate::db) fn decode_single_membership(
        &self,
        raw_key: &RawIndexStoreKey,
    ) -> Result<IndexEntryMembership, IndexEntryCorruption> {
        let witness = self.validate_witness()?;
        let storage_key = storage_key_from_raw_index_store_key(raw_key)?;

        Ok(IndexEntryMembership::new(storage_key, witness))
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
            return Err(IndexEntryCorruption::TooLarge { len: bytes.len() });
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

fn storage_key_from_raw_index_store_key(
    raw_key: &RawIndexStoreKey,
) -> Result<StorageKey, IndexEntryCorruption> {
    IndexKey::try_from_raw(raw_key)
        .and_then(|key| key.primary_storage_key().map_err(|_| "invalid primary key"))
        .map_err(|_| IndexEntryCorruption::InvalidKey)
}

impl From<&IndexEntry> for IndexEntryValue {
    fn from(entry: &IndexEntry) -> Self {
        Self::from_entry(entry)
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
mod tests {
    use super::{
        IndexEntryCorruption, IndexEntryEncodeError, IndexEntryExistenceWitness, IndexEntryValue,
        MAX_INDEX_ENTRY_BYTES,
    };
    use crate::{
        db::{
            data::StorageKey,
            index::{IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        },
        traits::Storable,
        types::EntityTag,
    };
    use std::borrow::Cow;

    fn raw_key_for(key: StorageKey) -> RawIndexStoreKey {
        let component = vec![0x42];
        IndexKey::new_from_components_with_kind(
            &IndexId::new(EntityTag::new(0x159), 1),
            IndexKeyKind::User,
            std::slice::from_ref(&component),
            key,
        )
        .to_raw()
    }

    #[test]
    fn index_entry_value_round_trip() {
        let key = StorageKey::Int(1);
        let raw_key = raw_key_for(key);

        let raw = IndexEntryValue::try_from_keys([key]).expect("encode index entry");
        let decoded = raw.decode_keys(&raw_key).expect("decode index entry");

        assert_eq!(decoded, vec![key]);
        assert_eq!(
            raw.as_bytes(),
            &[IndexEntryExistenceWitness::Present.to_stored_byte()]
        );
    }

    #[test]
    fn index_entry_value_decode_storage_key_recovers_key_owned_row_identity() {
        let key = StorageKey::Int(9);
        let raw_key = raw_key_for(key);
        let raw = IndexEntryValue::try_from_keys([key]).expect("encode index entry");

        assert_eq!(
            raw.decode_storage_key(&raw_key)
                .expect("decode key-owned row identity"),
            key
        );
    }

    #[test]
    fn index_entry_value_membership_is_owned_by_raw_key_not_value_constructor_input() {
        let constructor_key = StorageKey::Int(9);
        let raw_key_key = StorageKey::Nat(42);
        let raw_key = raw_key_for(raw_key_key);
        let raw = IndexEntryValue::try_from_keys([constructor_key]).expect("encode index entry");

        assert_eq!(
            raw.decode_storage_key(&raw_key)
                .expect("decode key-owned membership"),
            raw_key_key
        );
        assert_eq!(
            raw.as_bytes(),
            &[IndexEntryExistenceWitness::Present.to_stored_byte()],
            "raw index-entry values must stay presence-only"
        );
    }

    #[test]
    fn index_entry_value_try_from_keys_rejects_multi_key_entries() {
        let err = IndexEntryValue::try_from_keys([StorageKey::Int(1), StorageKey::Nat(2)])
            .expect_err("presence-only entries reject duplicated membership");

        assert!(matches!(
            err,
            IndexEntryEncodeError::TooManyKeys { keys: 2 }
        ));
    }

    #[test]
    fn index_entry_value_decode_keys_preserves_single_key_compatibility_surface() {
        let key = StorageKey::Int(3);
        let raw_key = raw_key_for(key);
        let raw = IndexEntryValue::try_from_keys([key]).expect("encode index entry");

        assert_eq!(raw.decode_keys(&raw_key).expect("decode keys"), vec![key]);
    }

    #[test]
    fn index_entry_value_decode_single_membership_recovers_present_witness() {
        let key = StorageKey::Int(9);
        let raw_key = raw_key_for(key);
        let raw = IndexEntryValue::try_from_keys([key]).expect("encode index entry");
        let membership = raw
            .decode_single_membership(&raw_key)
            .expect("decode single membership");

        assert_eq!(membership.storage_key(), key);
        assert_eq!(
            membership.existence_witness(),
            IndexEntryExistenceWitness::Present
        );
    }

    #[test]
    fn index_entry_value_roundtrip_via_bytes() {
        let key = StorageKey::Int(9);
        let raw_key = raw_key_for(key);

        let raw = IndexEntryValue::try_from_keys([key]).expect("encode index entry");
        let encoded = Storable::to_bytes(&raw);
        let raw = IndexEntryValue::from_bytes(encoded);
        let decoded = raw.decode_keys(&raw_key).expect("decode index entry");

        assert_eq!(decoded, vec![key]);
    }

    #[test]
    fn index_entry_value_rejects_empty() {
        let raw_key = raw_key_for(StorageKey::Int(1));
        let bytes = vec![];
        let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(&raw_key),
            Err(IndexEntryCorruption::EmptyEntry)
        ));
    }

    #[test]
    fn index_entry_value_rejects_invalid_witness() {
        let raw_key = raw_key_for(StorageKey::Int(1));
        let raw = IndexEntryValue::from_bytes(Cow::Owned(vec![9]));
        assert!(matches!(
            raw.decode_keys(&raw_key),
            Err(IndexEntryCorruption::InvalidWitness)
        ));
    }

    #[test]
    fn index_entry_value_rejects_oversized_payload() {
        let raw_key = raw_key_for(StorageKey::Int(1));
        let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
        let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(&raw_key),
            Err(IndexEntryCorruption::TooLarge { .. })
        ));
    }

    #[test]
    fn index_entry_value_rejects_invalid_raw_key_primary_suffix() {
        let raw = IndexEntryValue::presence();
        let invalid_raw_key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![0]));
        assert!(matches!(
            raw.decode_keys(&invalid_raw_key),
            Err(IndexEntryCorruption::InvalidKey)
        ));
    }

    #[test]
    fn index_entry_value_try_from_keys_rejects_empty_membership() {
        let err = IndexEntryValue::try_from_keys([]).expect_err("encoding should reject no keys");

        assert!(matches!(err, IndexEntryEncodeError::EmptyEntry));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn index_entry_value_decode_fuzz_does_not_panic() {
        const RUNS: u64 = 1_000;
        const MAX_LEN: usize = 256;

        let mut seed = 0xA5A5_5A5A_u64;
        for _ in 0..RUNS {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let len = (seed as usize) % MAX_LEN;

            let mut bytes = vec![0u8; len];
            for byte in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *byte = (seed >> 24) as u8;
            }

            let raw = IndexEntryValue::from_bytes(Cow::Owned(bytes));
            let _ = raw.decode_keys(&raw_key_for(StorageKey::Int(1)));
        }
    }
}
