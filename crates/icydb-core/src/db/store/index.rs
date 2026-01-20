use crate::{
    MAX_INDEX_FIELDS,
    db::store::{DataKey, EntityName, IndexName, StoreRegistry},
    prelude::*,
    traits::Storable,
};
use candid::CandidType;
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeSet};
use thiserror::Error as ThisError;

///
/// IndexStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct IndexStoreRegistry(StoreRegistry<IndexStore>);

impl IndexStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

impl Default for IndexStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

///
/// IndexInsertOutcome
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexInsertOutcome {
    Inserted,
    Skipped,
}

///
/// IndexRemoveOutcome
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRemoveOutcome {
    Removed,
    Skipped,
}

///
/// IndexInsertError
///

#[derive(Debug, ThisError)]
pub enum IndexInsertError {
    #[error("unique index violation")]
    UniqueViolation,
    #[error("index entry corrupted: {0}")]
    CorruptedEntry(#[from] IndexEntryCorruption),
    #[error("index entry exceeds max keys: {keys} (limit {MAX_INDEX_ENTRY_KEYS})")]
    EntryTooLarge { keys: usize },
}

///
/// IndexStore
///

#[derive(Deref, DerefMut)]
pub struct IndexStore(BTreeMap<RawIndexKey, RawIndexEntry, VirtualMemory<DefaultMemoryImpl>>);

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    pub fn insert_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> Result<IndexInsertOutcome, IndexInsertError> {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Ok(IndexInsertOutcome::Skipped);
        };
        let raw_key = index_key.to_raw();
        let key = entity.key();

        if let Some(raw_entry) = self.get(&raw_key) {
            let mut entry = raw_entry.try_decode()?;
            if index.unique {
                if entry.len() > 1 {
                    return Err(IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }.into());
                }
                if entry.contains(&key) {
                    return Ok(IndexInsertOutcome::Skipped);
                }
                if !entry.is_empty() {
                    return Err(IndexInsertError::UniqueViolation);
                }
                let entry = IndexEntry::new(key);
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
                self.insert(raw_key, raw_entry);
            } else {
                entry.insert_key(key);
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
                self.insert(raw_key, raw_entry);
            }
        } else {
            let entry = IndexEntry::new(key);
            let raw_entry = RawIndexEntry::try_from_entry(&entry)
                .map_err(|err| IndexInsertError::EntryTooLarge { keys: err.keys() })?;
            self.insert(raw_key, raw_entry);
        }

        Ok(IndexInsertOutcome::Inserted)
    }

    pub fn remove_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> Result<IndexRemoveOutcome, IndexEntryCorruption> {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Ok(IndexRemoveOutcome::Skipped);
        };
        let raw_key = index_key.to_raw();

        if let Some(raw_entry) = self.get(&raw_key) {
            let mut entry = raw_entry.try_decode()?;
            entry.remove_key(&entity.key());
            if entry.is_empty() {
                self.remove(&raw_key);
            } else {
                let raw_entry = RawIndexEntry::try_from_entry(&entry)
                    .map_err(|err| IndexEntryCorruption::TooManyKeys { count: err.keys() })?;
                self.insert(raw_key, raw_entry);
            }
            return Ok(IndexRemoveOutcome::Removed);
        }

        Ok(IndexRemoveOutcome::Skipped)
    }

    pub fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, &'static str> {
        let mut out = Vec::new();
        let index_id = IndexId::new::<E>(index);

        let Some(fps) = prefix
            .iter()
            .map(Value::to_index_fingerprint)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(out);
        };

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &fps);
        let start_raw = start.to_raw();
        let end_raw = end.to_raw();

        for entry in self.range(start_raw..=end_raw) {
            let _ = IndexKey::try_from_raw(entry.key())?;
            let decoded = entry.value().try_decode().map_err(|err| err.message())?;
            out.extend(decoded.iter_keys().map(|k| DataKey::new::<E>(k)));
        }

        Ok(out)
    }

    /// Sum of bytes used by all index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(IndexKey::STORED_SIZE) + entry.value().len() as u64)
            .sum()
    }
}

///
/// IndexId
///

#[derive(Clone, Copy, Debug, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexId(pub IndexName);

impl IndexId {
    #[must_use]
    pub fn new<E: EntityKind>(index: &IndexModel) -> Self {
        let entity = EntityName::from_static(E::ENTITY_NAME);
        Self(IndexName::from_parts(&entity, index.fields))
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self(IndexName::max_storable())
    }
}

///
/// IndexKey
/// (FIXED-SIZE, MANUAL)
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexKey {
    index_id: IndexId,
    len: u8,
    values: [[u8; 16]; MAX_INDEX_FIELDS],
}

impl IndexKey {
    #[allow(clippy::cast_possible_truncation)]
    pub const STORED_SIZE: u32 = IndexName::STORED_SIZE + 1 + (MAX_INDEX_FIELDS as u32 * 16);

    pub fn new<E: EntityKind>(entity: &E, index: &IndexModel) -> Option<Self> {
        if index.fields.len() > MAX_INDEX_FIELDS {
            return None;
        }

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        let mut len = 0usize;

        for field in index.fields {
            let value = entity.get_value(field)?;
            let fp = value.to_index_fingerprint()?;
            values[len] = fp;
            len += 1;
        }

        #[allow(clippy::cast_possible_truncation)]
        Some(Self {
            index_id: IndexId::new::<E>(index),
            len: len as u8,
            values,
        })
    }

    #[must_use]
    pub const fn empty(index_id: IndexId) -> Self {
        Self {
            index_id,
            len: 0,
            values: [[0u8; 16]; MAX_INDEX_FIELDS],
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn bounds_for_prefix(
        index_id: IndexId,
        index_len: usize,
        prefix: &[[u8; 16]],
    ) -> (Self, Self) {
        let mut start = Self::empty(index_id);
        let mut end = Self::empty(index_id);

        for (i, fp) in prefix.iter().enumerate() {
            start.values[i] = *fp;
            end.values[i] = *fp;
        }

        start.len = index_len as u8;
        end.len = start.len;

        for value in end.values.iter_mut().take(index_len).skip(prefix.len()) {
            *value = [0xFF; 16];
        }

        (start, end)
    }

    #[must_use]
    pub fn to_raw(&self) -> RawIndexKey {
        let mut buf = [0u8; Self::STORED_SIZE as usize];

        let name_bytes = self.index_id.0.to_bytes();
        buf[..name_bytes.len()].copy_from_slice(&name_bytes);

        let mut offset = IndexName::STORED_SIZE as usize;
        buf[offset] = self.len;
        offset += 1;

        for value in &self.values {
            buf[offset..offset + 16].copy_from_slice(value);
            offset += 16;
        }

        RawIndexKey(buf)
    }

    pub fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
        let bytes = &raw.0;
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err("corrupted IndexKey: invalid size");
        }

        let mut offset = 0;

        let index_name =
            IndexName::from_bytes(&bytes[offset..offset + IndexName::STORED_SIZE as usize])
                .map_err(|_| "corrupted IndexKey: invalid IndexName bytes")?;
        offset += IndexName::STORED_SIZE as usize;

        let len = bytes[offset];
        offset += 1;

        if len as usize > MAX_INDEX_FIELDS {
            return Err("corrupted IndexKey: invalid index length");
        }

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        for value in &mut values {
            value.copy_from_slice(&bytes[offset..offset + 16]);
            offset += 16;
        }

        let len_usize = len as usize;
        for value in values.iter().skip(len_usize) {
            if value.iter().any(|&b| b != 0) {
                return Err("corrupted IndexKey: non-zero fingerprint padding");
            }
        }

        Ok(Self {
            index_id: IndexId(index_name),
            len,
            values,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawIndexKey([u8; IndexKey::STORED_SIZE as usize]);

impl RawIndexKey {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; IndexKey::STORED_SIZE as usize] {
        &self.0
    }
}

impl Storable for RawIndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut out = [0u8; IndexKey::STORED_SIZE as usize];
        if bytes.len() == out.len() {
            out.copy_from_slice(bytes.as_ref());
        }
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::STORED_SIZE,
        is_fixed_size: true,
    };
}

///
/// IndexEntry (VALUE, RAW + BOUNDED)
///

const INDEX_ENTRY_LEN_BYTES: usize = 4;
pub const MAX_INDEX_ENTRY_KEYS: usize = 65_535;
#[allow(clippy::cast_possible_truncation)]
pub const MAX_INDEX_ENTRY_BYTES: u32 =
    (INDEX_ENTRY_LEN_BYTES + (MAX_INDEX_ENTRY_KEYS * Key::STORED_SIZE)) as u32;

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
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;
    use std::borrow::Cow;

    #[test]
    fn index_key_rejects_undersized_bytes() {
        let buf = vec![0u8; IndexKey::STORED_SIZE as usize - 1];
        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
        let err = IndexKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    fn index_key_rejects_oversized_bytes() {
        let buf = vec![0u8; IndexKey::STORED_SIZE as usize + 1];
        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&buf));
        let err = IndexKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn index_key_rejects_len_over_max() {
        let key = IndexKey::empty(IndexId::max_storable());
        let mut raw = key.to_raw();
        let len_offset = IndexName::STORED_SIZE as usize;
        raw.0[len_offset] = (MAX_INDEX_FIELDS as u8) + 1;
        let err = IndexKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    fn index_key_rejects_invalid_index_name() {
        let key = IndexKey::empty(IndexId::max_storable());
        let mut raw = key.to_raw();
        raw.0[0] = 0;
        raw.0[1] = 0;
        let err = IndexKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    fn index_key_rejects_fingerprint_padding() {
        let key = IndexKey::empty(IndexId::max_storable());
        let mut raw = key.to_raw();
        let values_offset = IndexName::STORED_SIZE as usize + 1;
        raw.0[values_offset] = 1;
        let err = IndexKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    #[expect(clippy::large_types_passed_by_value)]
    fn index_key_ordering_matches_bytes() {
        fn make_key(index_id: IndexId, len: u8, first: u8, second: u8) -> IndexKey {
            let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
            values[0] = [first; 16];
            values[1] = [second; 16];
            IndexKey {
                index_id,
                len,
                values,
            }
        }

        let entity = EntityName::from_static("entity");
        let idx_a = IndexId(IndexName::from_parts(&entity, &["a"]));
        let idx_b = IndexId(IndexName::from_parts(&entity, &["b"]));

        let keys = vec![
            make_key(idx_a, 1, 1, 0),
            make_key(idx_a, 2, 1, 2),
            make_key(idx_a, 1, 2, 0),
            make_key(idx_b, 1, 0, 0),
        ];

        let mut sorted_by_ord = keys.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = keys;
        sorted_by_bytes.sort_by(|a, b| a.to_raw().as_bytes().cmp(b.to_raw().as_bytes()));

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "IndexKey Ord and byte ordering diverged"
        );
    }

    #[test]
    fn raw_index_entry_round_trip() {
        let mut entry = IndexEntry::new(Key::Int(1));
        entry.insert_key(Key::Uint(2));

        let raw = RawIndexEntry::try_from_entry(&entry).expect("encode index entry");
        let decoded = raw.try_decode().expect("decode index entry");

        assert_eq!(decoded.len(), entry.len());
        assert!(decoded.contains(&Key::Int(1)));
        assert!(decoded.contains(&Key::Uint(2)));
    }

    #[test]
    fn raw_index_entry_roundtrip_via_bytes() {
        let mut entry = IndexEntry::new(Key::Int(9));
        entry.insert_key(Key::Uint(10));

        let raw = RawIndexEntry::try_from_entry(&entry).expect("encode index entry");
        let encoded = Storable::to_bytes(&raw);
        let raw = RawIndexEntry::from_bytes(encoded);
        let decoded = raw.try_decode().expect("decode index entry");

        assert_eq!(decoded.len(), entry.len());
        assert!(decoded.contains(&Key::Int(9)));
        assert!(decoded.contains(&Key::Uint(10)));
    }

    #[test]
    fn raw_index_entry_rejects_empty() {
        let raw = RawIndexEntry(vec![0, 0, 0, 0]);
        assert!(matches!(
            raw.try_decode(),
            Err(IndexEntryCorruption::EmptyEntry)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_truncated_payload() {
        let key = Key::Int(1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes());
        bytes.truncate(bytes.len() - 1);
        let raw = RawIndexEntry(bytes);
        assert!(matches!(
            raw.try_decode(),
            Err(IndexEntryCorruption::LengthMismatch)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_oversized_payload() {
        let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
        let raw = RawIndexEntry(bytes);
        assert!(matches!(
            raw.try_decode(),
            Err(IndexEntryCorruption::TooLarge { .. })
        ));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn raw_index_entry_rejects_corrupted_length_field() {
        let count = (MAX_INDEX_ENTRY_KEYS + 1) as u32;
        let raw = RawIndexEntry(count.to_be_bytes().to_vec());
        assert!(matches!(
            raw.try_decode(),
            Err(IndexEntryCorruption::TooManyKeys { .. })
        ));
    }

    #[test]
    fn raw_index_entry_rejects_duplicate_keys() {
        let key = Key::Int(1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes());
        bytes.extend_from_slice(&key.to_bytes());
        let raw = RawIndexEntry(bytes);
        assert!(matches!(
            raw.try_decode(),
            Err(IndexEntryCorruption::DuplicateKey)
        ));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn index_key_decode_fuzz_roundtrip_is_canonical() {
        const RUNS: u64 = 1_000;

        let mut seed = 0xBADC_0FFE_u64;
        for _ in 0..RUNS {
            let mut bytes = [0u8; IndexKey::STORED_SIZE as usize];
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            let raw = RawIndexKey(bytes);
            if let Ok(decoded) = IndexKey::try_from_raw(&raw) {
                let re = decoded.to_raw();
                assert_eq!(
                    raw.as_bytes(),
                    re.as_bytes(),
                    "decoded IndexKey must be canonical"
                );
            }
        }
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
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            let raw = RawIndexEntry(bytes);
            let _ = raw.try_decode();
        }
    }
}
