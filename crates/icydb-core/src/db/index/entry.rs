use crate::{
    db::{
        index::RawIndexKey,
        store::{StorageKey, StorageKeyEncodeError},
    },
    traits::{EntityKind, FieldValue, Storable},
    types::Ref,
    value::Value,
};
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

#[derive(Clone, Debug)]
pub struct IndexEntry<E: EntityKind> {
    ids: BTreeSet<E::Id>,
}

impl<E: EntityKind> IndexEntry<E> {
    #[must_use]
    pub fn new(id: E::Id) -> Self {
        let mut ids = BTreeSet::new();
        ids.insert(id);
        Self { ids }
    }

    pub fn insert(&mut self, id: E::Id) {
        self.ids.insert(id);
    }

    pub fn remove(&mut self, id: E::Id) {
        self.ids.remove(&id);
    }

    #[must_use]
    pub fn contains(&self, id: E::Id) -> bool {
        self.ids.contains(&id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = E::Id> + '_ {
        self.ids.iter().copied()
    }

    #[must_use]
    pub fn single_id(&self) -> Option<E::Id> {
        if self.ids.len() == 1 {
            self.ids.iter().copied().next()
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
    pub fn try_from_entry<E: EntityKind>(
        entry: &IndexEntry<E>,
    ) -> Result<Self, IndexEntryEncodeError> {
        let keys = entry.len();
        if keys > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryEncodeError::TooManyKeys { keys });
        }

        let mut out =
            Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + (keys * StorageKey::STORED_SIZE_USIZE));

        let count = u32::try_from(keys).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys })?;
        out.extend_from_slice(&count.to_be_bytes());

        for id in entry.iter_ids() {
            let sk = E::id_to_storage_key(id);
            let bytes = sk.to_bytes()?;
            out.extend_from_slice(&bytes);
        }

        Ok(Self(out))
    }

    pub fn try_decode<E: EntityKind>(&self) -> Result<IndexEntry<E>, IndexEntryCorruption> {
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

        let mut ids = BTreeSet::new();
        let mut offset = INDEX_ENTRY_LEN_BYTES;

        for _ in 0..count {
            let end = offset + StorageKey::STORED_SIZE_USIZE;
            let sk = StorageKey::try_from(&bytes[offset..end])
                .map_err(|_| IndexEntryCorruption::InvalidKey)?;

            let id = E::id_from_storage_key(sk).map_err(|_| IndexEntryCorruption::InvalidKey)?;

            if !ids.insert(id) {
                return Err(IndexEntryCorruption::DuplicateKey);
            }

            offset = end;
        }

        Ok(IndexEntry { ids })
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
