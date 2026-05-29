//! Module: data::store
//! Responsibility: stable BTreeMap-backed row persistence.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::{
    db::{
        data::{CanonicalRow, RawDataStoreKey, RawRow},
        key_taxonomy::RawDataStoreKeyRange,
    },
    types::EntityTag,
};
use ic_memory::stable_structures::{BTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory};
#[cfg(feature = "diagnostics")]
use std::cell::Cell;
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
/// Thin persistence wrapper over one stable BTreeMap.
///
/// Invariant: callers provide already-validated `RawDataStoreKey` and canonical row bytes.
/// This type intentionally does not enforce commit-phase ordering.
///

pub struct DataStore {
    map: BTreeMap<RawDataStoreKey, RawRow, VirtualMemory<DefaultMemoryImpl>>,
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
            map: BTreeMap::init(memory),
        }
    }

    /// Insert or replace one row by raw key.
    pub(in crate::db) fn insert(
        &mut self,
        key: RawDataStoreKey,
        row: CanonicalRow,
    ) -> Option<RawRow> {
        self.map.insert(key, row.into_raw_row())
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(in crate::db) fn insert_raw_for_test(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Option<RawRow> {
        self.map.insert(key, row)
    }

    /// Remove one row by raw key.
    pub(in crate::db) fn remove(&mut self, key: &RawDataStoreKey) -> Option<RawRow> {
        self.map.remove(key)
    }

    /// Load one row by raw key.
    pub(in crate::db) fn get(&self, key: &RawDataStoreKey) -> Option<RawRow> {
        #[cfg(feature = "diagnostics")]
        record_data_store_get_call();

        self.map.get(key)
    }

    /// Return whether one raw key exists without cloning the row payload.
    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        self.map.contains_key(key)
    }

    /// Clear all stored rows from the data store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        self.map.clear_new();
    }

    /// Return the number of stored rows without exposing the backing map.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        self.map.len()
    }

    /// Return whether the data store currently contains no rows.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Visit raw row entries in canonical storage order.
    pub(in crate::db) fn visit_entries<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        for entry in self.map.iter() {
            if visitor(entry.key(), &entry.value())?.should_stop() {
                break;
            }
        }

        Ok(())
    }

    /// Visit raw row entries in reverse canonical storage order.
    pub(in crate::db) fn visit_entries_rev<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        for entry in self.map.iter().rev() {
            if visitor(entry.key(), &entry.value())?.should_stop() {
                break;
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
        for entry in self.map.range(key_range) {
            if visitor(entry.key(), &entry.value())?.should_stop() {
                break;
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
        for entry in self.map.range(key_range).rev() {
            if visitor(entry.key(), &entry.value())?.should_stop() {
                break;
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
}
