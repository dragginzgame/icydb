use crate::{
    Error, IndexSpec, Key, MAX_INDEX_FIELDS, Value,
    db::{
        executor::ExecutorError,
        store::{DataKey, StoreRegistry},
    },
    obs::metrics,
    traits::EntityKind,
};
use candid::CandidType;
use canic::{
    cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory},
    impl_storable_bounded, impl_storable_unbounded,
    utils::hash::hash_u64,
};
use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt::{self, Display},
};

///
/// IndexStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct IndexStoreRegistry(StoreRegistry<IndexStore>);

impl IndexStoreRegistry {
    #[must_use]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
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

    /// Inserts the given entity into the index defined by `I`.
    /// - If `I::UNIQUE`, insertion will fail if a conflicting entry already exists.
    /// - If the entity is missing required fields for this index, insertion is skipped.
    pub fn insert_index_entry<E: EntityKind>(
        &mut self,
        entity: &E,
        index: &IndexSpec,
    ) -> Result<(), Error> {
        // Skip if index key can't be built (e.g. optional fields missing)
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Ok(());
        };
        let key = entity.key();

        if let Some(mut existing) = self.get(&index_key) {
            if index.unique {
                // Already present → skip redundant write
                if existing.contains(&key) {
                    return Ok(());
                }

                // Any different existing key violates UNIQUE
                if !existing.is_empty() {
                    metrics::with_state_mut(|m| metrics::record_unique_violation_for::<E>(m));

                    return Err(ExecutorError::index_violation(E::PATH, index.fields))?;
                }

                self.insert(index_key.clone(), IndexEntry::new(index.fields, key));
            } else {
                existing.insert_key(key); // <-- add to the set
                self.insert(index_key.clone(), existing);
            }
        } else {
            self.insert(index_key, IndexEntry::new(index.fields, key));
        }
        metrics::with_state_mut(|m| {
            m.ops.index_inserts += 1;
            let entry = m.entities.entry(E::PATH.to_string()).or_default();
            entry.index_inserts = entry.index_inserts.saturating_add(1);
        });

        Ok(())
    }

    // remove_index_entry
    pub fn remove_index_entry<E: EntityKind>(&mut self, entity: &E, index: &IndexSpec) {
        // Skip if index key can't be built (e.g. optional fields missing)
        let Some(index_key) = IndexKey::new(entity, index) else {
            return;
        };

        if let Some(mut existing) = self.get(&index_key) {
            existing.remove_key(&entity.key()); // remove from the set

            if existing.is_empty() {
                self.remove(&index_key);
            } else {
                // Move the updated entry back without cloning
                self.insert(index_key, existing);
            }
            metrics::with_state_mut(|m| {
                m.ops.index_removes += 1;
                let entry = m.entities.entry(E::PATH.to_string()).or_default();
                entry.index_removes = entry.index_removes.saturating_add(1);
            });
        }
    }

    #[must_use]
    pub fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexSpec,
        prefix: &[Value],
    ) -> Vec<DataKey> {
        let mut out = Vec::new();

        for (_, entry) in self.iter_with_hashed_prefix::<E>(index, prefix) {
            out.extend(entry.keys.iter().map(|&k| DataKey::new::<E>(k)));
        }

        out
    }

    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| u64::from(IndexKey::STORABLE_MAX_SIZE) + entry.value().len() as u64)
            .sum()
    }

    /// Internal: iterate entries for this index whose `hashed_values` start with the hashed `prefix`.
    /// Uses a bounded range for efficient scanning.
    fn iter_with_hashed_prefix<E: EntityKind>(
        &self,
        index: &IndexSpec,
        prefix: &[Value],
    ) -> impl Iterator<Item = (IndexKey, IndexEntry)> {
        let index_id = IndexId::new::<E>(index);
        let hashed_prefix_opt = Self::index_fingerprints(prefix); // Option<Vec<[u8;16]>>

        // Compute start..end bounds. If the prefix isn't indexable, construct an empty range
        // (same iterator type) by using identical start==end under the same index_id.
        let (start_key, end_key) = if let Some(hp) = hashed_prefix_opt {
            IndexKey::bounds_for_prefix(index_id, hp)
        } else {
            (
                IndexKey {
                    index_id,
                    hashed_values: Vec::new(),
                },
                IndexKey {
                    index_id,
                    hashed_values: Vec::new(),
                },
            )
        };

        self.range(start_key..end_key)
            .map(|entry| (entry.key().clone(), entry.value()))
    }

    fn index_fingerprints(values: &[Value]) -> Option<Vec<[u8; 16]>> {
        // collects to Option<Vec<_>>: None if any element was non-indexable
        values.iter().map(Value::to_index_fingerprint).collect()
    }
}

///
/// IndexId
///

#[derive(
    Clone,
    Debug,
    Display,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    CandidType,
    Serialize,
    Deserialize,
)]
pub struct IndexId(u64);

impl IndexId {
    #[must_use]
    pub fn new<E: EntityKind>(index: &IndexSpec) -> Self {
        Self::from_path_and_fields(E::PATH, index.fields)
    }

    fn from_path_and_fields(path: &str, fields: &[&str]) -> Self {
        let cap = path.len() + fields.iter().map(|f| f.len() + 1).sum::<usize>();
        let mut buffer = Vec::with_capacity(cap);

        // much more efficient than format
        buffer.extend_from_slice(path.as_bytes());
        for field in fields {
            buffer.extend_from_slice(field.as_bytes());
            buffer.extend_from_slice(b"|");
        }

        Self(hash_u64(&buffer))
    }

    #[must_use]
    pub fn max_storable() -> Self {
        Self::from_path_and_fields(
            "path::to::long::entity::name::Entity",
            &[
                "long_field_one",
                "long_field_two",
                "long_field_three",
                "long_field_four",
            ],
        )
    }
}

///
/// IndexKey
///

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexKey {
    pub index_id: IndexId,
    pub hashed_values: Vec<[u8; 16]>,
}

impl IndexKey {
    // Sized with headroom for worst‑case hashed key payload
    pub const STORABLE_MAX_SIZE: u32 = 180;

    #[must_use]
    pub fn new<E: EntityKind>(entity: &E, index: &IndexSpec) -> Option<Self> {
        let mut hashed_values = Vec::<[u8; 16]>::with_capacity(index.fields.len());

        // get each value and convert to key
        for field in index.fields {
            let value = entity.get_value(field)?;
            let fp = value.to_index_fingerprint()?; // bail if any component is non-indexable

            hashed_values.push(fp);
        }

        Some(Self {
            index_id: IndexId::new::<E>(index),
            hashed_values,
        })
    }

    // max_storable
    #[must_use]
    pub fn max_storable() -> Self {
        Self {
            index_id: IndexId::max_storable(),
            hashed_values: (0..MAX_INDEX_FIELDS).map(|_| [u8::MAX; 16]).collect(),
        }
    }

    /// Compute the bounded start..end keys for a given hashed prefix under an index id.
    /// End is exclusive and created by appending a single 0xFF..0xFF block to the prefix.
    #[must_use]
    pub fn bounds_for_prefix(index_id: IndexId, mut prefix: Vec<[u8; 16]>) -> (Self, Self) {
        let start = Self {
            index_id,
            hashed_values: prefix.clone(),
        };
        prefix.push([0xFF; 16]);
        let end = Self {
            index_id,
            hashed_values: prefix,
        };
        (start, end)
    }
}

impl Display for IndexKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, values: {}",
            self.index_id,
            self.hashed_values.len()
        )
    }
}

impl_storable_bounded!(IndexKey, IndexKey::STORABLE_MAX_SIZE, false);

///
/// IndexEntry
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    fields: Vec<String>,
    keys: HashSet<Key>,
}

impl IndexEntry {
    #[must_use]
    pub fn new(fields: &[&str], key: Key) -> Self {
        let mut key_set = HashSet::with_capacity(1);
        key_set.insert(key);

        Self {
            fields: fields.iter().map(ToString::to_string).collect(),
            keys: key_set,
        }
    }

    pub fn insert_key(&mut self, key: Key) {
        let _ = self.keys.insert(key);
    }

    /// Removes the key from the set.
    pub fn remove_key(&mut self, key: &Key) {
        let _ = self.keys.remove(key);
    }

    /// Checks if the set contains the key.
    #[must_use]
    pub fn contains(&self, key: &Key) -> bool {
        self.keys.contains(key)
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns number of keys in the entry.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns keys as a sorted `Vec<Key>` (useful for serialization/debug).
    #[must_use]
    pub fn to_sorted_vec(&self) -> Vec<Key> {
        let mut keys: Vec<_> = self.keys.iter().copied().collect();
        keys.sort_unstable(); // uses Ord, fast
        keys
    }
}

impl_storable_unbounded!(IndexEntry);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::*;

    #[test]
    fn index_key_max_size_is_bounded() {
        let index_key = IndexKey::max_storable();
        let size = Storable::to_bytes(&index_key).len();

        assert!(
            size <= IndexKey::STORABLE_MAX_SIZE as usize,
            "serialized IndexKey too large: got {size} bytes (limit {})",
            IndexKey::STORABLE_MAX_SIZE
        );
    }

    #[test]
    fn index_entry_round_trip() {
        let original = IndexEntry::new(&["a", "b"], Key::from(1u64));
        let encoded = Storable::to_bytes(&original);
        let decoded = IndexEntry::from_bytes(encoded);

        assert_eq!(original.fields, decoded.fields);
        assert_eq!(original.to_sorted_vec(), decoded.to_sorted_vec());
    }
}
