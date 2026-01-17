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

///
/// IndexStore
///

#[derive(Deref, DerefMut)]
pub struct IndexStore(BTreeMap<IndexKey, IndexEntry, VirtualMemory<DefaultMemoryImpl>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexInsertOutcome {
    Inserted,
    Skipped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRemoveOutcome {
    Removed,
    Skipped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexInsertError {
    UniqueViolation,
}

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

        let (start, end) = IndexKey::bounds_for_prefix(index_id, fps);

        Box::new(self.range(start..end).map(|e| (e.key().clone(), e.value())))
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
    pub const STORED_SIZE: u32 = IndexName::STORED_SIZE as u32 + 1 + (MAX_INDEX_FIELDS as u32 * 16);

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

        Some(Self {
            index_id: IndexId::new::<E>(index),
            len: len as u8,
            values,
        })
    }

    pub fn empty(index_id: IndexId) -> Self {
        Self {
            index_id,
            len: 0,
            values: [[0u8; 16]; MAX_INDEX_FIELDS],
        }
    }

    pub fn bounds_for_prefix(index_id: IndexId, prefix: Vec<[u8; 16]>) -> (Self, Self) {
        let mut start = Self::empty(index_id);
        let mut end = Self::empty(index_id);

        for (i, fp) in prefix.iter().enumerate() {
            start.values[i] = *fp;
            end.values[i] = *fp;
        }

        start.len = prefix.len() as u8;
        end.len = start.len + 1;
        end.values[start.len as usize] = [0xFF; 16];

        (start, end)
    }
}

impl Storable for IndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        let mut buf = Vec::with_capacity(Self::STORED_SIZE as usize);
        buf.extend_from_slice(self.index_id.0.as_bytes());
        buf.push(self.len);
        for i in 0..MAX_INDEX_FIELDS {
            buf.extend_from_slice(&self.values[i]);
        }
        buf.into()
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut offset = 0;

        let index_name =
            IndexName::from_bytes(bytes[offset..offset + IndexName::STORED_SIZE as usize].into())
                .expect("corrupted IndexKey: invalid IndexName bytes");

        offset += IndexName::STORED_SIZE as usize;

        let len = bytes[offset];
        offset += 1;

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        for i in 0..MAX_INDEX_FIELDS {
            values[i].copy_from_slice(&bytes[offset..offset + 16]);
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

    pub fn contains(&self, key: &Key) -> bool {
        self.keys.contains(key)
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn single_key(&self) -> Option<Key> {
        if self.keys.len() == 1 {
            self.keys.iter().copied().next()
        } else {
            None
        }
    }
}

impl_storable_unbounded!(IndexEntry);
