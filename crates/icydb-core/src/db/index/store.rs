//! Module: index::store
//! Responsibility: stable-or-heap index-entry storage behind the index-store boundary.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::db::index::{IndexEntryValue, key::RawIndexStoreKey};

use candid::CandidType;
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use serde::Deserialize;
use std::collections::BTreeMap as HeapBTreeMap;

//
// IndexState
//
// Explicit lifecycle visibility state for one index store.
// Visibility matters because planner-visible indexes must already be complete:
// the index contents are fully built and query-visible for reads.
//
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum IndexState {
    Building,
    #[default]
    Ready,
    Dropping,
}

impl IndexState {
    /// Return the stable lowercase text label for this lifecycle state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Ready => "ready",
            Self::Dropping => "dropping",
        }
    }
}

///
/// IndexStore
///
/// Thin persistence wrapper over one stable or heap BTreeMap.
///
/// Invariant: callers provide already-validated `RawIndexStoreKey`/`IndexEntryValue`.
///

pub struct IndexStore {
    pub(super) backend: IndexStoreBackend,
    generation: u64,
    state: IndexState,
}

pub(super) enum IndexStoreBackend {
    Stable(StableBTreeMap<RawIndexStoreKey, IndexEntryValue, VirtualMemory<DefaultMemoryImpl>>),
    Heap(HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>),
}

/// Control-flow result for index-store traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexStoreVisit {
    Continue,
    #[allow(
        dead_code,
        reason = "index traversal exposes early-stop semantics for bounded future callers; focused tests cover it before live call sites need it"
    )]
    Stop,
}

impl IndexStoreVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: IndexStoreBackend::Stable(StableBTreeMap::init(memory)),
            generation: 0,
            // Existing stores default to Ready until one explicit build/drop
            // lifecycle is introduced.
            state: IndexState::Ready,
        }
    }

    /// Initialize a volatile heap-backed index store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: IndexStoreBackend::Heap(HeapBTreeMap::new()),
            generation: 0,
            state: IndexState::Ready,
        }
    }

    /// Return whether this index store is heap-backed and volatile.
    #[must_use]
    pub(in crate::db) const fn is_heap_storage(&self) -> bool {
        matches!(self.backend, IndexStoreBackend::Heap(_))
    }

    /// Visit all index entries in canonical store order without exposing the
    /// backing stable-map iterator.
    pub(in crate::db) fn visit_entries<E>(
        &self,
        mut visitor: impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<IndexStoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            IndexStoreBackend::Stable(map) => {
                for entry in map.iter() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        return Ok(());
                    }
                }
            }
            IndexStoreBackend::Heap(map) => {
                for (key, value) in map {
                    if visitor(key, value)?.should_stop() {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    pub(in crate::db) fn get(&self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        match &self.backend {
            IndexStoreBackend::Stable(map) => map.get(key),
            IndexStoreBackend::Heap(map) => map.get(key).cloned(),
        }
    }

    pub fn len(&self) -> u64 {
        match &self.backend {
            IndexStoreBackend::Stable(map) => map.len(),
            IndexStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.backend {
            IndexStoreBackend::Stable(map) => map.is_empty(),
            IndexStoreBackend::Heap(map) => map.is_empty(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Return the explicit lifecycle state for this index store.
    #[must_use]
    pub(in crate::db) const fn state(&self) -> IndexState {
        self.state
    }

    /// Mark this index store as in-progress and therefore ineligible for
    /// planner visibility until a full authoritative rebuild ends.
    pub(in crate::db) const fn mark_building(&mut self) {
        self.state = IndexState::Building;
    }

    /// Mark this index store as fully built and planner-visible again.
    pub(in crate::db) const fn mark_ready(&mut self) {
        self.state = IndexState::Ready;
    }

    /// Mark this index store as dropping and therefore not planner-visible.
    pub(in crate::db) const fn mark_dropping(&mut self) {
        self.state = IndexState::Dropping;
    }

    pub(crate) fn insert(
        &mut self,
        key: RawIndexStoreKey,
        entry: IndexEntryValue,
    ) -> Option<IndexEntryValue> {
        let previous = match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.insert(key, entry),
            IndexStoreBackend::Heap(map) => map.insert(key, entry),
        };
        self.bump_generation();
        previous
    }

    pub(crate) fn remove(&mut self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        let previous = match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.remove(key),
            IndexStoreBackend::Heap(map) => map.remove(key),
        };
        self.bump_generation();
        previous
    }

    pub fn clear(&mut self) {
        match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.clear_new(),
            IndexStoreBackend::Heap(map) => map.clear(),
        }
        self.bump_generation();
    }

    /// Sum of bytes used by all stored index entries.
    pub fn memory_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        let _: Result<(), std::convert::Infallible> = self.visit_entries(|key, value| {
            bytes = bytes.saturating_add(key.as_bytes().len() as u64 + value.len() as u64);
            Ok(IndexStoreVisit::Continue)
        });
        bytes
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }
}
