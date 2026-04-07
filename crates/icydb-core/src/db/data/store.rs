//! Module: data::store
//! Responsibility: stable BTreeMap-backed row persistence.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::db::data::{CanonicalRow, DataKey, RawDataKey, RawRow};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};

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
    secondary_covering_authoritative: bool,
    secondary_existence_witness_authoritative: bool,
}

impl DataStore {
    /// Initialize a data store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: BTreeMap::init(memory),
            secondary_covering_authoritative: false,
            secondary_existence_witness_authoritative: false,
        }
    }

    /// Insert or replace one row by raw key.
    pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow> {
        let previous = self.map.insert(key, row.into_raw_row());
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();

        previous
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(crate) fn insert_raw_for_test(&mut self, key: RawDataKey, row: RawRow) -> Option<RawRow> {
        let previous = self.map.insert(key, row);
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();

        previous
    }

    /// Remove one row by raw key.
    pub fn remove(&mut self, key: &RawDataKey) -> Option<RawRow> {
        let previous = self.map.remove(key);
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();

        previous
    }

    /// Load one row by raw key.
    pub fn get(&self, key: &RawDataKey) -> Option<RawRow> {
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
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
    }

    /// Return whether this row store currently participates in a synchronized
    /// secondary-covering authority witness.
    #[must_use]
    pub(in crate::db) const fn secondary_covering_authoritative(&self) -> bool {
        self.secondary_covering_authoritative
    }

    /// Mark this row store as synchronized with its paired secondary index
    /// store after successful commit or recovery.
    pub(in crate::db) const fn mark_secondary_covering_authoritative(&mut self) {
        self.secondary_covering_authoritative = true;
    }

    /// Return whether this row store currently participates in an explicit
    /// secondary existence-witness contract.
    #[must_use]
    pub(in crate::db) const fn secondary_existence_witness_authoritative(&self) -> bool {
        self.secondary_existence_witness_authoritative
    }

    /// Mark this row store as synchronized with the paired secondary
    /// existence-witness contract after successful commit, recovery, or a
    /// storage-owned stale-fixture mutation that updates the witness itself.
    pub(in crate::db) const fn mark_secondary_existence_witness_authoritative(&mut self) {
        self.secondary_existence_witness_authoritative = true;
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        self.iter()
            .map(|entry| DataKey::STORED_SIZE_BYTES + entry.value().len() as u64)
            .sum()
    }

    // Any direct row-store mutation invalidates the secondary covering
    // authority witness until commit/recovery re-synchronizes the pair.
    const fn invalidate_secondary_covering_authority(&mut self) {
        self.secondary_covering_authoritative = false;
    }

    // Any direct row-store mutation also invalidates the explicit secondary
    // existence witness until storage or recovery rebuilds it.
    const fn invalidate_secondary_existence_witness_authority(&mut self) {
        self.secondary_existence_witness_authoritative = false;
    }
}

impl std::ops::Deref for DataStore {
    type Target = BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
