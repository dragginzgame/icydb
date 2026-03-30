//! Module: data::store
//! Responsibility: stable BTreeMap-backed row persistence.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::db::data::{CanonicalRow, DataKey, RawDataKey, RawRow};
use canic::cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
use derive_more::Deref;

///
/// DataStore
///
/// Thin persistence wrapper over one stable BTreeMap.
///
/// Invariant: callers provide already-validated `RawDataKey` and canonical row bytes.
/// This type intentionally does not enforce commit-phase ordering.
///

#[derive(Deref)]
pub struct DataStore(BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>);

impl DataStore {
    /// Initialize a data store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    /// Insert or replace one row by raw key.
    pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow> {
        self.0.insert(key, row.into_raw_row())
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(crate) fn insert_raw_for_test(&mut self, key: RawDataKey, row: RawRow) -> Option<RawRow> {
        self.0.insert(key, row)
    }

    /// Remove one row by raw key.
    pub fn remove(&mut self, key: &RawDataKey) -> Option<RawRow> {
        self.0.remove(key)
    }

    /// Load one row by raw key.
    pub fn get(&self, key: &RawDataKey) -> Option<RawRow> {
        self.0.get(key)
    }

    /// Clear all stored rows from the data store.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        self.iter()
            .map(|entry| DataKey::STORED_SIZE_BYTES + entry.value().len() as u64)
            .sum()
    }
}
