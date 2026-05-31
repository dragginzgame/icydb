//! Module: data::store
//! Responsibility: stable-or-heap row storage behind the data-store boundary.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::{
    db::{
        data::{CanonicalRow, RawDataStoreKey, RawRow},
        key_taxonomy::RawDataStoreKeyRange,
    },
    types::EntityTag,
};
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
#[cfg(feature = "diagnostics")]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::RangeBounds;

#[cfg(feature = "diagnostics")]
thread_local! {
    static DATA_STORE_GET_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(feature = "diagnostics")]
fn record_data_store_get_call() {
    DATA_STORE_GET_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

///
/// DataStore
///
/// Thin persistence wrapper over one stable or heap BTreeMap.
///
/// Invariant: callers provide already-validated `RawDataStoreKey` and canonical row bytes.
/// This type intentionally does not enforce commit-phase ordering.
///

pub struct DataStore {
    backend: DataStoreBackend,
}

enum DataStoreBackend {
    Stable(StableBTreeMap<RawDataStoreKey, RawRow, VirtualMemory<DefaultMemoryImpl>>),
    Heap(HeapBTreeMap<RawDataStoreKey, RawRow>),
    Journaled {
        canonical: StableBTreeMap<RawDataStoreKey, RawRow, VirtualMemory<DefaultMemoryImpl>>,
        live: HeapBTreeMap<RawDataStoreKey, RawRow>,
        tombstones: BTreeSet<RawDataStoreKey>,
    },
}

/// Control-flow result for store traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum StoreVisit {
    Continue,
    Stop,
}

impl StoreVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl DataStore {
    /// Initialize a data store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: DataStoreBackend::Stable(StableBTreeMap::init(memory)),
        }
    }

    /// Initialize a volatile heap-backed data store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: DataStoreBackend::Heap(HeapBTreeMap::new()),
        }
    }

    /// Initialize a journaled cached-stable data store.
    ///
    /// Normal writes update only the live projection. The canonical stable map
    /// is the future fold target and is not mutated by this wrapper's write
    /// methods.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: DataStoreBackend::Journaled {
                canonical: StableBTreeMap::init(memory),
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
            },
        }
    }

    /// Insert or replace one row by raw key.
    pub(in crate::db) fn insert(
        &mut self,
        key: RawDataStoreKey,
        row: CanonicalRow,
    ) -> Option<RawRow> {
        let row = row.into_raw_row();
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(&key)
        } else {
            None
        };
        match &mut self.backend {
            DataStoreBackend::Stable(map) => map.insert(key, row),
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        }
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(in crate::db) fn insert_raw_for_test(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Option<RawRow> {
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(&key)
        } else {
            None
        };
        match &mut self.backend {
            DataStoreBackend::Stable(map) => map.insert(key, row),
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        }
    }

    /// Remove one row by raw key.
    pub(in crate::db) fn remove(&mut self, key: &RawDataStoreKey) -> Option<RawRow> {
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(key)
        } else {
            None
        };
        match &mut self.backend {
            DataStoreBackend::Stable(map) => map.remove(key),
            DataStoreBackend::Heap(map) => map.remove(key),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(key);
                tombstones.insert(key.clone());
                previous_journaled
            }
        }
    }

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable base.
    pub(in crate::db) fn reset_journaled_live_projection(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            live, tombstones, ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant(
                "journaled live projection reset requires a journaled data store",
            ));
        };

        live.clear();
        tombstones.clear();

        Ok(())
    }

    /// Apply one recovered journal row put into the volatile projection.
    pub(in crate::db) fn apply_recovered_journal_put(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant(
                "journal row replay requires a journaled data store",
            ));
        };

        let previous = if tombstones.contains(&key) {
            None
        } else {
            live.get(&key).cloned().or_else(|| canonical.get(&key))
        };
        tombstones.remove(&key);
        live.insert(key, row);

        Ok(previous)
    }

    /// Apply one recovered journal row delete into the volatile projection.
    pub(in crate::db) fn apply_recovered_journal_delete(
        &mut self,
        key: &RawDataStoreKey,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant(
                "journal row replay requires a journaled data store",
            ));
        };

        let previous = if tombstones.contains(key) {
            None
        } else {
            live.get(key).cloned().or_else(|| canonical.get(key))
        };
        live.remove(key);
        tombstones.insert(key.clone());

        Ok(previous)
    }

    /// Apply one folded journal row put into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_put(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(crate::error::InternalError::store_invariant(
                "journal row fold requires a journaled data store",
            ));
        };

        Ok(canonical.insert(key, row))
    }

    /// Apply one folded journal row delete into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_delete(
        &mut self,
        key: &RawDataStoreKey,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(crate::error::InternalError::store_invariant(
                "journal row fold requires a journaled data store",
            ));
        };

        Ok(canonical.remove(key))
    }

    /// Load one row by raw key.
    pub(in crate::db) fn get(&self, key: &RawDataStoreKey) -> Option<RawRow> {
        #[cfg(feature = "diagnostics")]
        record_data_store_get_call();

        match &self.backend {
            DataStoreBackend::Stable(map) => map.get(key),
            DataStoreBackend::Heap(map) => map.get(key).cloned(),
            DataStoreBackend::Journaled { .. } => Self::journaled_get_raw(&self.backend, key),
        }
    }

    /// Return whether one raw key exists without cloning the row payload.
    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        match &self.backend {
            DataStoreBackend::Stable(map) => map.contains_key(key),
            DataStoreBackend::Heap(map) => map.contains_key(key),
            DataStoreBackend::Journaled { .. } => {
                Self::journaled_get_raw(&self.backend, key).is_some()
            }
        }
    }

    /// Clear all stored rows from the data store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        match &mut self.backend {
            DataStoreBackend::Stable(map) => map.clear_new(),
            DataStoreBackend::Heap(map) => map.clear(),
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                live.clear();
                tombstones.clear();
                for entry in canonical.iter() {
                    tombstones.insert(entry.key().clone());
                }
            }
        }
    }

    /// Return the number of stored rows without exposing the backing map.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        match &self.backend {
            DataStoreBackend::Stable(map) => map.len(),
            DataStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            DataStoreBackend::Journaled { .. } => {
                u64::try_from(Self::journaled_entries_snapshot(&self.backend).len())
                    .unwrap_or(u64::MAX)
            }
        }
    }

    /// Return whether the data store currently contains no rows.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        match &self.backend {
            DataStoreBackend::Stable(map) => map.is_empty(),
            DataStoreBackend::Heap(map) => map.is_empty(),
            DataStoreBackend::Journaled { .. } => {
                Self::journaled_entries_snapshot(&self.backend).is_empty()
            }
        }
    }

    /// Visit raw row entries in canonical storage order.
    pub(in crate::db) fn visit_entries<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Stable(map) => {
                for entry in map.iter() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Heap(map) => {
                for (key, row) in map {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                for (key, row) in Self::journaled_entries_snapshot(&self.backend) {
                    if visitor(&key, &row)?.should_stop() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Visit raw row entries in reverse canonical storage order.
    pub(in crate::db) fn visit_entries_rev<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Stable(map) => {
                for entry in map.iter().rev() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.iter().rev() {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                for (key, row) in Self::journaled_entries_snapshot(&self.backend)
                    .into_iter()
                    .rev()
                {
                    if visitor(&key, &row)?.should_stop() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Visit raw row entries whose keys belong to the provided storage range.
    pub(in crate::db) fn visit_range<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Stable(map) => {
                for entry in map.range(key_range) {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range(key_range) {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                let entries = Self::journaled_entries_snapshot(&self.backend);
                for (key, row) in entries.range(key_range) {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Visit raw row entries in reverse order whose keys belong to the provided storage range.
    pub(in crate::db) fn visit_range_rev<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Stable(map) => {
                for entry in map.range(key_range).rev() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range(key_range).rev() {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                let entries = Self::journaled_entries_snapshot(&self.backend);
                for (key, row) in entries.range(key_range).rev() {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Visit raw row entries for one entity using compact prefix bounds.
    pub(in crate::db) fn visit_entity<E>(
        &self,
        entity: EntityTag,
        visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        self.visit_range(RawDataStoreKey::store_range_bounds(&range), visitor)
    }

    /// Sum of bytes used by all stored rows.
    pub(in crate::db) fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        let mut bytes = 0u64;
        let _: Result<(), Infallible> = self.visit_entries(|key, row| {
            bytes = bytes.saturating_add(key.as_bytes().len() as u64 + row.len() as u64);
            Ok(StoreVisit::Continue)
        });
        bytes
    }

    /// Return the monotonic perf-only count of stable row fetches seen by this process.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_get_call_count() -> u64 {
        DATA_STORE_GET_CALL_COUNT.with(Cell::get)
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            DataStoreBackend::Stable(map) | DataStoreBackend::Journaled { canonical: map, .. } => {
                map.len()
            }
            DataStoreBackend::Heap(_) => 0,
        }
    }

    fn journaled_get_raw(backend: &DataStoreBackend, key: &RawDataStoreKey) -> Option<RawRow> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = backend
        else {
            return None;
        };

        if tombstones.contains(key) {
            return None;
        }
        live.get(key).cloned().or_else(|| canonical.get(key))
    }

    fn journaled_entries_snapshot(
        backend: &DataStoreBackend,
    ) -> HeapBTreeMap<RawDataStoreKey, RawRow> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = backend
        else {
            return HeapBTreeMap::new();
        };

        let mut entries = HeapBTreeMap::new();
        for entry in canonical.iter() {
            let key = entry.key().clone();
            if !tombstones.contains(&key) {
                entries.insert(key, entry.value());
            }
        }
        for (key, row) in live {
            if !tombstones.contains(key) {
                entries.insert(key.clone(), row.clone());
            }
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            data::DecodedDataStoreKey,
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        },
        testing::test_memory,
    };
    use std::ops::Bound;

    fn raw_key(entity: u64, id: u64) -> RawDataStoreKey {
        DecodedDataStoreKey::new(
            EntityTag::new(entity),
            &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
        )
        .to_raw()
        .expect("test data key should encode")
    }

    fn raw_row(value: u8) -> RawRow {
        RawRow::try_new(vec![value]).expect("test raw row should be bounded")
    }

    fn seed_store(memory_id: u8, entries: &[(u64, u64, u8)]) -> DataStore {
        let mut store = DataStore::init(test_memory(memory_id));
        for (entity, id, row) in entries {
            store.insert_raw_for_test(raw_key(*entity, *id), raw_row(*row));
        }
        store
    }

    fn seed_heap_store(entries: &[(u64, u64, u8)]) -> DataStore {
        let mut store = DataStore::init_heap();
        for (entity, id, row) in entries {
            store.insert_raw_for_test(raw_key(*entity, *id), raw_row(*row));
        }
        store
    }

    fn collect_keys(store: &DataStore) -> Vec<RawDataStoreKey> {
        let mut keys = Vec::new();
        let _: Result<(), Infallible> = store.visit_entries(|key, _row| {
            keys.push(key.clone());
            Ok(StoreVisit::Continue)
        });
        keys
    }

    #[test]
    fn data_store_visit_entries_preserves_storage_key_order() {
        let store = seed_store(221, &[(2, 1, 21), (1, 3, 13), (1, 1, 11), (1, 2, 12)]);

        let mut expected = vec![raw_key(2, 1), raw_key(1, 3), raw_key(1, 1), raw_key(1, 2)];
        expected.sort();

        assert_eq!(collect_keys(&store), expected);
    }

    #[test]
    fn data_store_visit_range_preserves_raw_key_bounds() {
        let store = seed_store(222, &[(1, 1, 11), (1, 2, 12), (1, 3, 13), (1, 4, 14)]);

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_range(
            (
                Bound::Included(raw_key(1, 2)),
                Bound::Excluded(raw_key(1, 4)),
            ),
            |key, row| {
                visited.push((key.clone(), row.as_bytes()[0]));
                Ok(StoreVisit::Continue)
            },
        );

        assert_eq!(visited, vec![(raw_key(1, 2), 12), (raw_key(1, 3), 13)]);
    }

    #[test]
    fn data_store_visit_entity_preserves_compact_entity_prefix_bounds() {
        let store = seed_store(223, &[(2, 1, 21), (1, 2, 12), (2, 3, 23), (1, 1, 11)]);

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_entity(EntityTag::new(2), |key, row| {
            visited.push((key.clone(), row.as_bytes()[0]));
            Ok(StoreVisit::Continue)
        });

        assert_eq!(visited, vec![(raw_key(2, 1), 21), (raw_key(2, 3), 23)]);
    }

    #[test]
    fn data_store_visit_entries_can_stop_without_error() {
        let store = seed_store(224, &[(1, 1, 11), (1, 2, 12), (1, 3, 13)]);

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_entries(|key, _row| {
            visited.push(key.clone());
            Ok(if visited.len() == 2 {
                StoreVisit::Stop
            } else {
                StoreVisit::Continue
            })
        });

        assert_eq!(visited, vec![raw_key(1, 1), raw_key(1, 2)]);
    }

    #[test]
    fn heap_data_store_preserves_order_bounds_and_early_stop() {
        let store = seed_heap_store(&[(2, 1, 21), (1, 3, 13), (1, 1, 11), (1, 2, 12)]);

        let mut expected = vec![raw_key(2, 1), raw_key(1, 3), raw_key(1, 1), raw_key(1, 2)];
        expected.sort();
        assert_eq!(collect_keys(&store), expected);

        let mut ranged = Vec::new();
        let _: Result<(), Infallible> = store.visit_range(
            (
                Bound::Included(raw_key(1, 1)),
                Bound::Excluded(raw_key(1, 3)),
            ),
            |key, row| {
                ranged.push((key.clone(), row.as_bytes()[0]));
                Ok(StoreVisit::Continue)
            },
        );
        assert_eq!(ranged, vec![(raw_key(1, 1), 11), (raw_key(1, 2), 12)]);

        let mut entity = Vec::new();
        let _: Result<(), Infallible> = store.visit_entity(EntityTag::new(2), |key, row| {
            entity.push((key.clone(), row.as_bytes()[0]));
            Ok(StoreVisit::Continue)
        });
        assert_eq!(entity, vec![(raw_key(2, 1), 21)]);

        let mut stopped = Vec::new();
        let _: Result<(), Infallible> = store.visit_entries(|key, _| {
            stopped.push(key.clone());
            Ok(if stopped.len() == 2 {
                StoreVisit::Stop
            } else {
                StoreVisit::Continue
            })
        });
        assert_eq!(stopped, vec![raw_key(1, 1), raw_key(1, 2)]);
    }
}
