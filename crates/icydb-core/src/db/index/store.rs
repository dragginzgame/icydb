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
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};

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
    Journaled {
        canonical:
            StableBTreeMap<RawIndexStoreKey, IndexEntryValue, VirtualMemory<DefaultMemoryImpl>>,
        live: HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>,
        tombstones: BTreeSet<RawIndexStoreKey>,
    },
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

    /// Initialize a journaled cached-stable index store.
    ///
    /// Normal writes update only the live materialized projection. The
    /// canonical stable index is updated by future fold/rebuild paths.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: IndexStoreBackend::Journaled {
                canonical: StableBTreeMap::init(memory),
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
            },
            generation: 0,
            state: IndexState::Ready,
        }
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
            IndexStoreBackend::Journaled { .. } => {
                for (key, value) in Self::journaled_entries_snapshot(&self.backend) {
                    if visitor(&key, &value)?.should_stop() {
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
            IndexStoreBackend::Journaled { .. } => Self::journaled_get(&self.backend, key),
        }
    }

    pub fn len(&self) -> u64 {
        match &self.backend {
            IndexStoreBackend::Stable(map) => map.len(),
            IndexStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            IndexStoreBackend::Journaled { .. } => {
                u64::try_from(Self::journaled_entries_snapshot(&self.backend).len())
                    .unwrap_or(u64::MAX)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.backend {
            IndexStoreBackend::Stable(map) => map.is_empty(),
            IndexStoreBackend::Heap(map) => map.is_empty(),
            IndexStoreBackend::Journaled { .. } => {
                Self::journaled_entries_snapshot(&self.backend).is_empty()
            }
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
        let previous_journaled = if matches!(self.backend, IndexStoreBackend::Journaled { .. }) {
            self.get(&key)
        } else {
            None
        };
        let previous = match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.insert(key, entry),
            IndexStoreBackend::Heap(map) => map.insert(key, entry),
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, entry);
                previous_journaled
            }
        };
        self.bump_generation();
        previous
    }

    pub(crate) fn remove(&mut self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        let previous_journaled = if matches!(self.backend, IndexStoreBackend::Journaled { .. }) {
            self.get(key)
        } else {
            None
        };
        let previous = match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.remove(key),
            IndexStoreBackend::Heap(map) => map.remove(key),
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(key);
                tombstones.insert(key.clone());
                previous_journaled
            }
        };
        self.bump_generation();
        previous
    }

    pub fn clear(&mut self) {
        match &mut self.backend {
            IndexStoreBackend::Stable(map) => map.clear_new(),
            IndexStoreBackend::Heap(map) => map.clear(),
            IndexStoreBackend::Journaled {
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            IndexStoreBackend::Stable(map)
            | IndexStoreBackend::Journaled { canonical: map, .. } => map.len(),
            IndexStoreBackend::Heap(_) => 0,
        }
    }

    fn journaled_get(
        backend: &IndexStoreBackend,
        key: &RawIndexStoreKey,
    ) -> Option<IndexEntryValue> {
        let IndexStoreBackend::Journaled {
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

    pub(super) fn journaled_entries_snapshot(
        backend: &IndexStoreBackend,
    ) -> HeapBTreeMap<RawIndexStoreKey, IndexEntryValue> {
        let IndexStoreBackend::Journaled {
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
        for (key, value) in live {
            if !tombstones.contains(key) {
                entries.insert(key.clone(), value.clone());
            }
        }

        entries
    }
}
