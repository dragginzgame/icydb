//! Module: index::entry
//! Responsibility: index-entry payload encode/decode and structural validation.
//! Does not own: commit ordering or unique-policy decisions.
//! Boundary: commit/index-store consume raw entries after prevalidation.

use crate::{
    db::{
        data::{StorageKey, StorageKeyEncodeError},
        index::RawIndexKey,
    },
    traits::{EntityKind, FieldValue, Storable},
    value::Value,
};
use canic_cdk::structures::storable::Bound;
use std::{borrow::Cow, collections::BTreeSet};
use thiserror::Error as ThisError;

///
/// Constants
///

const INDEX_ENTRY_LEN_BYTES: usize = 4;
pub(crate) const MAX_INDEX_ENTRY_KEYS: usize = 65_535;

#[expect(clippy::cast_possible_truncation)]
pub(crate) const MAX_INDEX_ENTRY_BYTES: u32 =
    (INDEX_ENTRY_LEN_BYTES + (MAX_INDEX_ENTRY_KEYS * StorageKey::STORED_SIZE_USIZE)) as u32;

///
/// IndexEntryCorruption
///

#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryCorruption {
    #[error("index entry exceeds max size")]
    TooLarge { len: usize },

    #[error("index entry missing key count")]
    MissingLength,

    #[error("index entry key count exceeds limit")]
    TooManyKeys { count: usize },

    #[error("index entry length does not match key count")]
    LengthMismatch,

    #[error("index entry contains invalid key bytes")]
    InvalidKey,

    #[error("index entry contains duplicate key")]
    DuplicateKey,

    #[error("index entry contains zero keys")]
    EmptyEntry,

    #[error("unique index entry contains {keys} keys")]
    NonUniqueEntry { keys: usize },

    #[error("index entry missing expected entity key: {entity_key:?} (index {index_key:?})")]
    MissingKey {
        index_key: Box<RawIndexKey>,
        entity_key: Value,
    },

    #[error("index entry points at key {indexed_key:?} but stored row key is {row_key:?}")]
    RowKeyMismatch {
        indexed_key: Box<Value>,
        row_key: Box<Value>,
    },
}

impl IndexEntryCorruption {
    #[must_use]
    pub(crate) fn missing_key(index_key: RawIndexKey, entity_key: impl FieldValue) -> Self {
        Self::MissingKey {
            index_key: Box::new(index_key),
            entity_key: entity_key.to_value(),
        }
    }
}

///
/// IndexEntryEncodeError
///

#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryEncodeError {
    #[error("index entry exceeds max keys: {keys} (limit {MAX_INDEX_ENTRY_KEYS})")]
    TooManyKeys { keys: usize },

    #[error("index entry contains duplicate key")]
    DuplicateKey,

    #[error("index entry key encoding failed: {0}")]
    KeyEncoding(#[from] StorageKeyEncodeError),
}

///
/// IndexEntry
///

#[derive(Clone, Debug)]
pub(crate) struct IndexEntry<E: EntityKind> {
    ids: BTreeSet<E::Key>,
}

impl<E: EntityKind> IndexEntry<E> {
    #[must_use]
    pub(crate) fn new(id: E::Key) -> Self {
        let mut ids = BTreeSet::new();
        ids.insert(id);
        Self { ids }
    }

    pub(crate) fn insert(&mut self, id: E::Key) {
        self.ids.insert(id);
    }

    pub(crate) fn remove(&mut self, id: E::Key) {
        self.ids.remove(&id);
    }

    #[must_use]
    pub(crate) fn contains(&self, id: E::Key) -> bool {
        self.ids.contains(&id)
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn iter_ids(&self) -> impl Iterator<Item = E::Key> + '_ {
        self.ids.iter().copied()
    }
}

///
/// RawIndexEntry
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawIndexEntry(Vec<u8>);

impl RawIndexEntry {
    pub(crate) fn try_from_entry<E: EntityKind>(
        entry: &IndexEntry<E>,
    ) -> Result<Self, IndexEntryEncodeError> {
        let mut keys = Vec::with_capacity(entry.ids.len());
        for id in &entry.ids {
            let value = id.to_value();
            let key = StorageKey::try_from_value(&value)?;
            keys.push(key);
        }

        Self::try_from_keys(keys)
    }

    pub(crate) fn try_decode<E: EntityKind>(&self) -> Result<IndexEntry<E>, IndexEntryCorruption> {
        let storage_keys = self.decode_keys()?;
        let mut ids = BTreeSet::new();

        for key in storage_keys {
            let value = key.as_value();
            let Some(id) = <E::Key as FieldValue>::from_value(&value) else {
                return Err(IndexEntryCorruption::InvalidKey);
            };
            ids.insert(id);
        }

        if ids.is_empty() {
            return Err(IndexEntryCorruption::EmptyEntry);
        }

        Ok(IndexEntry { ids })
    }

    pub(crate) fn try_from_keys<I>(keys: I) -> Result<Self, IndexEntryEncodeError>
    where
        I: IntoIterator<Item = StorageKey>,
    {
        // Phase 1: collect and bound-check key cardinality.
        let keys: Vec<StorageKey> = keys.into_iter().collect();
        let count = keys.len();

        if count > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryEncodeError::TooManyKeys { keys: count });
        }

        // Enforce encode/decode symmetry: duplicates are rejected at construction,
        // not deferred to decode-time corruption validation.
        let mut unique = BTreeSet::new();
        for key in &keys {
            if !unique.insert(*key) {
                return Err(IndexEntryEncodeError::DuplicateKey);
            }
        }

        // Phase 2: encode canonical length-prefixed payload.
        let mut out =
            Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + count * StorageKey::STORED_SIZE_USIZE);

        let count_u32 =
            u32::try_from(count).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys: count })?;
        out.extend_from_slice(&count_u32.to_be_bytes());

        for sk in keys {
            out.extend_from_slice(&sk.to_bytes()?);
        }

        Ok(Self(out))
    }

    pub(crate) fn decode_keys(&self) -> Result<Vec<StorageKey>, IndexEntryCorruption> {
        // Phase 1: validate frame shape before any key decode.
        self.validate()?;

        let bytes = self.0.as_slice();

        let mut len_buf = [0u8; INDEX_ENTRY_LEN_BYTES];
        len_buf.copy_from_slice(&bytes[..INDEX_ENTRY_LEN_BYTES]);
        let count = u32::from_be_bytes(len_buf) as usize;

        let mut keys = Vec::with_capacity(count);
        let mut offset = INDEX_ENTRY_LEN_BYTES;

        // Phase 2: decode each fixed-width storage key segment.
        for _ in 0..count {
            let end = offset + StorageKey::STORED_SIZE_USIZE;
            let sk = StorageKey::try_from(&bytes[offset..end])
                .map_err(|_| IndexEntryCorruption::InvalidKey)?;

            keys.push(sk);
            offset = end;
        }

        Ok(keys)
    }

    /// Validate the raw index entry structure without binding to an entity.
    pub(crate) fn validate(&self) -> Result<(), IndexEntryCorruption> {
        let bytes = self.0.as_slice();

        // Phase 1: frame-level checks (size, header, declared count).
        if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
            return Err(IndexEntryCorruption::TooLarge { len: bytes.len() });
        }
        if bytes.len() < INDEX_ENTRY_LEN_BYTES {
            return Err(IndexEntryCorruption::MissingLength);
        }

        let mut len_buf = [0u8; INDEX_ENTRY_LEN_BYTES];
        len_buf.copy_from_slice(&bytes[..INDEX_ENTRY_LEN_BYTES]);
        let count = u32::from_be_bytes(len_buf) as usize;

        if count == 0 {
            return Err(IndexEntryCorruption::EmptyEntry);
        }
        if count > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryCorruption::TooManyKeys { count });
        }

        let expected = INDEX_ENTRY_LEN_BYTES
            + count
                .checked_mul(StorageKey::STORED_SIZE_USIZE)
                .ok_or(IndexEntryCorruption::LengthMismatch)?;

        if bytes.len() != expected {
            return Err(IndexEntryCorruption::LengthMismatch);
        }

        // Phase 2: validate each StorageKey and reject duplicates.
        let mut keys = BTreeSet::new();
        let mut offset = INDEX_ENTRY_LEN_BYTES;

        for _ in 0..count {
            let end = offset + StorageKey::STORED_SIZE_USIZE;

            let sk = StorageKey::try_from(&bytes[offset..end])
                .map_err(|_| IndexEntryCorruption::InvalidKey)?;

            if !keys.insert(sk) {
                return Err(IndexEntryCorruption::DuplicateKey);
            }

            offset = end;
        }

        Ok(())
    }

    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.0.len()
    }
}

impl<E: EntityKind> TryFrom<&IndexEntry<E>> for RawIndexEntry {
    type Error = IndexEntryEncodeError;

    fn try_from(entry: &IndexEntry<E>) -> Result<Self, Self::Error> {
        Self::try_from_entry(entry)
    }
}

impl Storable for RawIndexEntry {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
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
        IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES, MAX_INDEX_ENTRY_KEYS,
        RawIndexEntry,
    };
    use crate::{db::data::StorageKey, traits::Storable};
    use std::borrow::Cow;

    #[test]
    fn raw_index_entry_round_trip() {
        let keys = vec![StorageKey::Int(1), StorageKey::Uint(2)];

        let raw = RawIndexEntry::try_from_keys(keys.clone()).expect("encode index entry");
        let decoded = raw.decode_keys().expect("decode index entry");

        assert_eq!(decoded.len(), keys.len());
        assert!(decoded.contains(&StorageKey::Int(1)));
        assert!(decoded.contains(&StorageKey::Uint(2)));
    }

    #[test]
    fn raw_index_entry_roundtrip_via_bytes() {
        let keys = vec![StorageKey::Int(9), StorageKey::Uint(10)];

        let raw = RawIndexEntry::try_from_keys(keys.clone()).expect("encode index entry");
        let encoded = Storable::to_bytes(&raw);
        let raw = RawIndexEntry::from_bytes(encoded);
        let decoded = raw.decode_keys().expect("decode index entry");

        assert_eq!(decoded.len(), keys.len());
        assert!(decoded.contains(&StorageKey::Int(9)));
        assert!(decoded.contains(&StorageKey::Uint(10)));
    }

    #[test]
    fn raw_index_entry_rejects_empty() {
        let bytes = vec![0, 0, 0, 0];
        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::EmptyEntry)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_truncated_payload() {
        let key = StorageKey::Int(1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));
        bytes.truncate(bytes.len() - 1);

        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::LengthMismatch)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_oversized_payload() {
        let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::TooLarge { .. })
        ));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn raw_index_entry_rejects_corrupted_length_field() {
        let count = (MAX_INDEX_ENTRY_KEYS + 1) as u32;
        let raw = RawIndexEntry::from_bytes(Cow::Owned(count.to_be_bytes().to_vec()));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::TooManyKeys { .. })
        ));
    }

    #[test]
    fn raw_index_entry_rejects_duplicate_keys() {
        let key = StorageKey::Int(1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));

        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::DuplicateKey)
        ));
    }

    #[test]
    fn raw_index_entry_try_from_keys_rejects_duplicate_keys() {
        let key = StorageKey::Int(7);
        let err = RawIndexEntry::try_from_keys([key, key]).expect_err(
            "encoding should reject duplicate keys instead of deferring to decode validation",
        );

        assert!(matches!(err, IndexEntryEncodeError::DuplicateKey));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn raw_index_entry_decode_fuzz_does_not_panic() {
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

            let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
            let _ = raw.decode_keys();
        }
    }
}
