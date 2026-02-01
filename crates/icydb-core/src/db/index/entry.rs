use crate::{
    db::{
        index::RawIndexKey,
        store::{StorageKey, StorageKeyEncodeError},
    },
    traits::{EntityKind, FieldValue, Storable},
    types::Ref,
    value::Value,
};
use candid::CandidType;
use canic_cdk::structures::storable::Bound;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeSet};
use thiserror::Error as ThisError;

///
/// Constants
///

const INDEX_ENTRY_LEN_BYTES: usize = 4;
pub const MAX_INDEX_ENTRY_KEYS: usize = 65_535;

#[allow(clippy::cast_possible_truncation)]
pub const MAX_INDEX_ENTRY_BYTES: u32 =
    (INDEX_ENTRY_LEN_BYTES + (MAX_INDEX_ENTRY_KEYS * StorageKey::STORED_SIZE_USIZE)) as u32;

///
/// IndexEntryCorruption
///

#[derive(Debug, ThisError)]
pub enum IndexEntryCorruption {
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
    pub fn missing_key(index_key: RawIndexKey, entity_key: impl FieldValue) -> Self {
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
pub enum IndexEntryEncodeError {
    #[error("index entry exceeds max keys: {keys} (limit {MAX_INDEX_ENTRY_KEYS})")]
    TooManyKeys { keys: usize },

    #[error("index entry key encoding failed: {0}")]
    KeyEncoding(#[from] StorageKeyEncodeError),
}

impl IndexEntryEncodeError {
    #[must_use]
    pub const fn keys(&self) -> usize {
        match self {
            Self::TooManyKeys { keys } => *keys,
            Self::KeyEncoding(_) => 0,
        }
    }
}

///
/// IndexEntry
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    keys: BTreeSet<StorageKey>,
}

impl IndexEntry {
    #[must_use]
    pub fn new<E: EntityKind>(key: Ref<E>) -> Self {
        Self::new_raw(key.raw())
    }

    #[must_use]
    pub(crate) fn new_raw(key: StorageKey) -> Self {
        let mut keys = BTreeSet::new();
        keys.insert(key);
        Self { keys }
    }

    pub fn insert_key<E: EntityKind>(&mut self, key: Ref<E>) {
        self.keys.insert(key.raw());
    }

    pub fn remove_key<E: EntityKind>(&mut self, key: Ref<E>) {
        self.keys.remove(&key.raw());
    }

    #[must_use]
    pub fn contains<E: EntityKind>(&self, key: Ref<E>) -> bool {
        self.keys.contains(&key.raw())
    }

    #[cfg(test)]
    pub(crate) fn insert_raw(&mut self, key: StorageKey) {
        self.keys.insert(key);
    }

    #[cfg(test)]
    pub(crate) fn contains_raw(&self, key: StorageKey) -> bool {
        self.keys.contains(&key)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn iter_keys<E: EntityKind>(&self) -> impl Iterator<Item = Ref<E>> + '_ {
        self.keys.iter().copied().map(Ref::from_raw)
    }

    pub(crate) fn iter_raw_keys(&self) -> impl Iterator<Item = StorageKey> + '_ {
        self.keys.iter().copied()
    }

    #[must_use]
    pub fn single_key<E: EntityKind>(&self) -> Option<Ref<E>> {
        if self.keys.len() == 1 {
            self.keys.iter().copied().map(Ref::from_raw).next()
        } else {
            None
        }
    }
}

///
/// RawIndexEntry
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIndexEntry(Vec<u8>);

impl RawIndexEntry {
    pub fn try_from_entry(entry: &IndexEntry) -> Result<Self, IndexEntryEncodeError> {
        let keys = entry.len();
        if keys > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryEncodeError::TooManyKeys { keys });
        }

        let mut out =
            Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + (keys * StorageKey::STORED_SIZE_USIZE));

        let count = u32::try_from(keys).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys })?;
        out.extend_from_slice(&count.to_be_bytes());

        for key in entry.iter_raw_keys() {
            let bytes = key.to_bytes()?;
            out.extend_from_slice(&bytes);
        }

        Ok(Self(out))
    }

    pub fn try_decode(&self) -> Result<IndexEntry, IndexEntryCorruption> {
        let bytes = self.0.as_slice();

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

        let mut keys = BTreeSet::new();
        let mut offset = INDEX_ENTRY_LEN_BYTES;

        for _ in 0..count {
            let end = offset + StorageKey::STORED_SIZE_USIZE;
            let key = StorageKey::try_from(&bytes[offset..end])
                .map_err(|_| IndexEntryCorruption::InvalidKey)?;

            if !keys.insert(key) {
                return Err(IndexEntryCorruption::DuplicateKey);
            }

            offset = end;
        }

        Ok(IndexEntry { keys })
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
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
