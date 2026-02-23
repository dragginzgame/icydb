use crate::db::data::{DataKey, RawDataKey, RawRow};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
use derive_more::Deref;

///
/// DataStore
///
/// Architectural Notes:
///
/// - DataStore is a thin persistence wrapper over a stable BTreeMap.
/// - All key and row validation occurs *before* insertion:
///     - RawDataKey is fixed-size and validated at decode.
///     - RawRow is size-bounded at construction.
/// - This layer does NOT enforce transactional or commit-phase discipline.
///   Higher layers (commit/executor) are responsible for write coordination.
/// - Mutation methods (insert/remove/clear) are intentionally explicit to
///   allow future interception (metrics, invariants, atomic guards).
/// - Read surface is exposed via Deref for ergonomic iteration, but this
///   means DataStore is not a strict policy layer.
///

#[derive(Deref)]
pub struct DataStore(BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>);

impl DataStore {
    #[must_use]
    /// Initialize a data store with the provided backing memory.
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    pub fn insert(&mut self, key: RawDataKey, row: RawRow) -> Option<RawRow> {
        self.0.insert(key, row)
    }

    pub fn remove(&mut self, key: &RawDataKey) -> Option<RawRow> {
        self.0.remove(key)
    }

    pub fn get(&self, key: &RawDataKey) -> Option<RawRow> {
        self.0.get(key)
    }

    /// Clear all stored rows from the data store.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| DataKey::STORED_SIZE_BYTES + entry.value().len() as u64)
            .sum()
    }
}
