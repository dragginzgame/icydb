//! Module: data::store
//! Responsibility: stable BTreeMap-backed row persistence.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::db::data::{CanonicalRow, DataKey, RawDataKey, RawRow};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
#[cfg(feature = "diagnostics")]
use std::cell::Cell;

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
/// Invariant: callers provide already-validated `RawDataKey` and canonical row bytes.
/// This type intentionally does not enforce commit-phase ordering.
///

pub struct DataStore {
    map: BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>,
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
    pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow> {
        self.map.insert(key, row.into_raw_row())
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(crate) fn insert_raw_for_test(&mut self, key: RawDataKey, row: RawRow) -> Option<RawRow> {
        self.map.insert(key, row)
    }

    /// Remove one row by raw key.
    pub fn remove(&mut self, key: &RawDataKey) -> Option<RawRow> {
        self.map.remove(key)
    }

    /// Load one row by raw key.
    pub fn get(&self, key: &RawDataKey) -> Option<RawRow> {
        #[cfg(feature = "diagnostics")]
        record_data_store_get_call();

        self.map.get(key)
    }

    /// Return whether one raw key exists without cloning the row payload.
    #[must_use]
    pub fn contains(&self, key: &RawDataKey) -> bool {
        self.map.contains_key(key)
    }

    /// Clear all stored rows from the data store.
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        self.iter()
            .map(|entry| DataKey::STORED_SIZE_BYTES + entry.value().len() as u64)
            .sum()
    }

    /// Return the monotonic perf-only count of stable row fetches seen by this process.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_get_call_count() -> u64 {
        DATA_STORE_GET_CALL_COUNT.with(Cell::get)
    }
}

impl std::ops::Deref for DataStore {
    type Target = BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
