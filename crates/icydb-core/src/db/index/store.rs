//! Module: index::store
//! Responsibility: stable index-entry persistence primitives.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::db::index::{entry::RawIndexEntry, key::RawIndexKey};

use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};

///
/// IndexStore
///
/// Thin persistence wrapper over one stable BTreeMap.
///
/// Invariant: callers provide already-validated `RawIndexKey`/`RawIndexEntry`.
///

pub struct IndexStore {
    pub(super) map: BTreeMap<RawIndexKey, RawIndexEntry, VirtualMemory<DefaultMemoryImpl>>,
    generation: u64,
}

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: BTreeMap::init(memory),
            generation: 0,
        }
    }

    /// Snapshot all index entry pairs (diagnostics only).
    #[allow(clippy::redundant_closure_for_method_calls)]
    pub(crate) fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.map.iter().map(|entry| entry.into_pair()).collect()
    }

    pub(in crate::db) fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.map.get(key)
    }

    pub fn len(&self) -> u64 {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[must_use]
    pub(in crate::db) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn insert(
        &mut self,
        key: RawIndexKey,
        entry: RawIndexEntry,
    ) -> Option<RawIndexEntry> {
        let previous = self.map.insert(key, entry);
        self.bump_generation();
        previous
    }

    pub(crate) fn remove(&mut self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let previous = self.map.remove(key);
        self.bump_generation();
        previous
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.bump_generation();
    }

    /// Sum of bytes used by all stored index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.map
            .iter()
            .map(|entry| entry.key().as_bytes().len() as u64 + entry.value().len() as u64)
            .sum()
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }
}
