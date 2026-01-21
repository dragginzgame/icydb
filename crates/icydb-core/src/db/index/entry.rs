use crate::{key::Key, traits::Storable};
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
    (INDEX_ENTRY_LEN_BYTES + (MAX_INDEX_ENTRY_KEYS * Key::STORED_SIZE)) as u32;

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
}

impl IndexEntryCorruption {
    #[must_use]
    pub const fn message(&self) -> &'static str {
        match self {
            Self::TooLarge { .. } => "corrupted index entry: exceeds max size",
            Self::MissingLength => "corrupted index entry: missing key count",
            Self::TooManyKeys { .. } => "corrupted index entry: key count exceeds limit",
            Self::LengthMismatch => "corrupted index entry: length mismatch",
            Self::InvalidKey => "corrupted index entry: invalid key bytes",
            Self::DuplicateKey => "corrupted index entry: duplicate key",
            Self::EmptyEntry => "corrupted index entry: empty entry",
            Self::NonUniqueEntry { .. } => "corrupted index entry: non-unique entry",
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
}

impl IndexEntryEncodeError {
    #[must_use]
    pub const fn keys(&self) -> usize {
        match self {
            Self::TooManyKeys { keys } => *keys,
        }
    }
}

///
/// IndexEntry
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    keys: BTreeSet<Key>,
}

impl IndexEntry {
    #[must_use]
    pub fn new(key: Key) -> Self {
        let mut keys = BTreeSet::new();
        keys.insert(key);

        Self { keys }
    }

    pub fn insert_key(&mut self, key: Key) {
        self.keys.insert(key);
    }

    pub fn remove_key(&mut self, key: &Key) {
        self.keys.remove(key);
    }

    #[must_use]
    pub fn contains(&self, key: &Key) -> bool {
        self.keys.contains(key)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.keys.iter().copied()
    }

    #[must_use]
    pub fn single_key(&self) -> Option<Key> {
        if self.keys.len() == 1 {
            self.keys.iter().copied().next()
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

        let mut out = Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + (keys * Key::STORED_SIZE));
        let count = u32::try_from(keys).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys })?;
        out.extend_from_slice(&count.to_be_bytes());
        for key in entry.iter_keys() {
            out.extend_from_slice(&key.to_bytes());
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
            .checked_add(
                count
                    .checked_mul(Key::STORED_SIZE)
                    .ok_or(IndexEntryCorruption::LengthMismatch)?,
            )
            .ok_or(IndexEntryCorruption::LengthMismatch)?;
        if bytes.len() != expected {
            return Err(IndexEntryCorruption::LengthMismatch);
        }

        let mut keys = BTreeSet::new();
        let mut offset = INDEX_ENTRY_LEN_BYTES;
        for _ in 0..count {
            let end = offset + Key::STORED_SIZE;
            let key = Key::try_from_bytes(&bytes[offset..end])
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
