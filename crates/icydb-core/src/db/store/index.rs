use crate::{
    MAX_INDEX_FIELDS,
    db::store::{DataKey, EntityName, IndexName, StoreRegistry},
    prelude::*,
    traits::Storable,
};
use candid::CandidType;
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use canic_memory::impl_storable_unbounded;
use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashSet};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexInsertError {
    UniqueViolation,
}

///
/// IndexStore
///

#[derive(Deref, DerefMut)]
pub struct IndexStore(BTreeMap<IndexKey, IndexEntry, VirtualMemory<DefaultMemoryImpl>>);

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
        let key = entity.key();

        if let Some(mut entry) = self.get(&index_key) {
            if index.unique {
                if entry.contains(&key) {
                    return Ok(IndexInsertOutcome::Skipped);
                }
                if !entry.is_empty() {
                    return Err(IndexInsertError::UniqueViolation);
                }
                self.insert(index_key, IndexEntry::new(key));
            } else {
                entry.insert_key(key);
                self.insert(index_key, entry);
            }
        } else {
            self.insert(index_key, IndexEntry::new(key));
        }

        Ok(IndexInsertOutcome::Inserted)
    }

    pub fn remove_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexModel,
    ) -> IndexRemoveOutcome {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return IndexRemoveOutcome::Skipped;
        };

        if let Some(mut entry) = self.get(&index_key) {
            entry.remove_key(&entity.key());
            if entry.is_empty() {
                self.remove(&index_key);
            } else {
                self.insert(index_key, entry);
            }
            return IndexRemoveOutcome::Removed;
        }

        IndexRemoveOutcome::Skipped
    }

    pub fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Vec<DataKey> {
        let mut out = Vec::new();

        for (_, entry) in self.iter_with_prefix::<E>(index, prefix) {
            out.extend(entry.keys.iter().map(|&k| DataKey::new::<E>(k)));
        }

        out
    }

    fn iter_with_prefix<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Box<dyn Iterator<Item = (IndexKey, IndexEntry)> + '_> {
        let index_id = IndexId::new::<E>(index);

        let Some(fps) = prefix
            .iter()
            .map(Value::to_index_fingerprint)
            .collect::<Option<Vec<_>>>()
        else {
            let empty = IndexKey::empty(index_id);
            return Box::new(
                self.range(empty.clone()..empty)
                    .map(|e| (e.key().clone(), e.value())),
            );
        };

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &fps);

        Box::new(
            self.range(start..=end)
                .map(|e| (e.key().clone(), e.value())),
        )
    }

    /// Sum of bytes used by all index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(IndexKey::STORED_SIZE) + entry.value().to_bytes().len() as u64)
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
}

impl Storable for IndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        let mut buf = Vec::with_capacity(Self::STORED_SIZE as usize);

        // IMPORTANT: fixed-size IndexName
        buf.extend_from_slice(&self.index_id.0.to_bytes());

        // len
        buf.push(self.len);

        // ALL value slots (fixed-size)
        for value in &self.values {
            buf.extend_from_slice(value);
        }

        debug_assert_eq!(buf.len(), Self::STORED_SIZE as usize);
        buf.into()
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(
            bytes.len(),
            Self::STORED_SIZE as usize,
            "corrupted IndexKey: invalid size"
        );
        let mut offset = 0;

        let index_name =
            IndexName::from_bytes(&bytes[offset..offset + IndexName::STORED_SIZE as usize])
                .expect("corrupted IndexKey: invalid IndexName");
        offset += IndexName::STORED_SIZE as usize;

        let len = bytes[offset];
        offset += 1;

        assert!(
            len as usize <= MAX_INDEX_FIELDS,
            "corrupted IndexKey: len exceeds MAX_INDEX_FIELDS"
        );

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        for value in &mut values {
            value.copy_from_slice(&bytes[offset..offset + 16]);
            offset += 16;
        }

        Self {
            index_id: IndexId(index_name),
            len,
            values,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.to_bytes().into_owned()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: true,
    };
}

///
/// IndexEntry (VALUE, UNBOUNDED)
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    keys: HashSet<Key>,
}

impl IndexEntry {
    #[must_use]
    pub fn new(key: Key) -> Self {
        let mut keys = HashSet::with_capacity(1);
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

    #[must_use]
    pub fn single_key(&self) -> Option<Key> {
        if self.keys.len() == 1 {
            self.keys.iter().copied().next()
        } else {
            None
        }
    }
}

impl_storable_unbounded!(IndexEntry);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;

    #[test]
    #[should_panic(expected = "corrupted IndexKey: invalid size")]
    fn index_key_rejects_undersized_bytes() {
        let buf = vec![0u8; IndexKey::STORED_SIZE as usize - 1];
        let _ = IndexKey::from_bytes(buf.into());
    }

    #[test]
    #[should_panic(expected = "corrupted IndexKey: invalid size")]
    fn index_key_rejects_oversized_bytes() {
        let buf = vec![0u8; IndexKey::STORED_SIZE as usize + 1];
        let _ = IndexKey::from_bytes(buf.into());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    #[should_panic(expected = "corrupted IndexKey: len exceeds MAX_INDEX_FIELDS")]
    fn index_key_rejects_len_over_max() {
        let key = IndexKey::empty(IndexId::max_storable());
        let mut bytes = key.to_bytes().into_owned();
        let len_offset = IndexName::STORED_SIZE as usize;
        bytes[len_offset] = (MAX_INDEX_FIELDS as u8) + 1;
        let _ = IndexKey::from_bytes(bytes.into());
    }

    #[test]
    #[should_panic(expected = "corrupted IndexKey: invalid IndexName")]
    fn index_key_rejects_invalid_index_name() {
        let key = IndexKey::empty(IndexId::max_storable());
        let mut bytes = key.to_bytes().into_owned();
        bytes[0] = 0;
        bytes[1] = 0;
        let _ = IndexKey::from_bytes(bytes.into());
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
        sorted_by_bytes.sort_by(|a, b| a.to_bytes().cmp(&b.to_bytes()));

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "IndexKey Ord and byte ordering diverged"
        );
    }
}
