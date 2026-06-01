//! Module: index::store
//! Responsibility: stable-or-heap index-entry storage behind the index-store boundary.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::db::{
    direction::Direction,
    index::{IndexEntryValue, key::RawIndexStoreKey},
};

use candid::CandidType;
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use serde::Deserialize;
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::ops::Bound;

#[cfg(test)]
thread_local! {
    static JOURNALED_SNAPSHOT_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_journaled_snapshot_call() {
    JOURNALED_SNAPSHOT_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

#[cfg(test)]
fn reset_journaled_snapshot_call_count_for_tests() {
    JOURNALED_SNAPSHOT_CALL_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn journaled_snapshot_call_count_for_tests() -> u64 {
    JOURNALED_SNAPSHOT_CALL_COUNT.with(Cell::get)
}

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
            IndexStoreBackend::Journaled {
                canonical: _,
                live: _,
                tombstones: _,
            } => self.visit_journaled_entries_in_range(
                (&Bound::Unbounded, &Bound::Unbounded),
                Direction::Asc,
                |key, value| visitor(key, value).map(IndexStoreVisit::should_stop),
            )?,
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

    /// Fold the current journaled materialized index view into the canonical
    /// stable base and clear volatile projection state.
    pub(in crate::db) fn fold_journaled_materialized_view(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let entries = Self::journaled_entries_snapshot(&self.backend);
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant(
                "journal index fold requires a journaled index store",
            ));
        };

        canonical.clear_new();
        for (key, value) in entries {
            canonical.insert(key, value);
        }
        live.clear();
        tombstones.clear();
        self.bump_generation();

        Ok(())
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
        #[cfg(test)]
        record_journaled_snapshot_call();

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

    pub(super) fn visit_journaled_entries_in_range<E>(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        direction: Direction,
        mut visit: impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
    ) -> Result<(), E> {
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &self.backend
        else {
            return Ok(());
        };

        let lower = bounds.0.clone();
        let upper = bounds.1.clone();
        match direction {
            Direction::Asc if canonical.is_empty() => {
                for (key, value) in live.range((lower, upper)) {
                    if visit(key, value)? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc if canonical.is_empty() => {
                for (key, value) in live.range((lower, upper)).rev() {
                    if visit(key, value)? {
                        return Ok(());
                    }
                }
            }
            Direction::Asc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)) {
                    if visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)).rev() {
                    if visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
            }
            Direction::Asc => {
                Self::visit_journaled_merged_entries(
                    canonical,
                    live,
                    tombstones,
                    (lower, upper),
                    visit,
                )?;
            }
            Direction::Desc => {
                Self::visit_journaled_merged_entries_rev(
                    canonical,
                    live,
                    tombstones,
                    (lower, upper),
                    visit,
                )?;
            }
        }

        Ok(())
    }

    #[allow(clippy::redundant_closure_for_method_calls)]
    fn visit_journaled_merged_entries<E>(
        canonical: &StableBTreeMap<
            RawIndexStoreKey,
            IndexEntryValue,
            VirtualMemory<DefaultMemoryImpl>,
        >,
        live: &HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>,
        tombstones: &BTreeSet<RawIndexStoreKey>,
        bounds: (Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>),
        mut visit: impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
    ) -> Result<(), E> {
        enum MergeStep {
            Canonical,
            Live,
            Both,
            Done,
        }

        let mut canonical_iter = canonical
            .range((bounds.0.clone(), bounds.1.clone()))
            .peekable();
        let mut live_iter = live.range((bounds.0, bounds.1)).peekable();

        loop {
            let step = {
                let canonical_key = canonical_iter.peek().map(|entry| entry.key());
                let live_key = live_iter.peek().map(|(key, _)| *key);
                match (canonical_key, live_key) {
                    (None, None) => MergeStep::Done,
                    (Some(_), None) => MergeStep::Canonical,
                    (None, Some(_)) => MergeStep::Live,
                    (Some(canonical_key), Some(live_key)) => match canonical_key.cmp(live_key) {
                        Ordering::Less => MergeStep::Canonical,
                        Ordering::Equal => MergeStep::Both,
                        Ordering::Greater => MergeStep::Live,
                    },
                }
            };

            match step {
                MergeStep::Canonical => {
                    let entry = canonical_iter
                        .next()
                        .expect("peeked canonical journaled index entry should exist");
                    if !tombstones.contains(entry.key()) && visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
                MergeStep::Live => {
                    let (key, value) = live_iter
                        .next()
                        .expect("peeked live journaled index entry should exist");
                    if !tombstones.contains(key) && visit(key, value)? {
                        return Ok(());
                    }
                }
                MergeStep::Both => {
                    let _canonical_entry = canonical_iter
                        .next()
                        .expect("peeked canonical journaled index entry should exist");
                    let (key, value) = live_iter
                        .next()
                        .expect("peeked live journaled index entry should exist");
                    if !tombstones.contains(key) && visit(key, value)? {
                        return Ok(());
                    }
                }
                MergeStep::Done => return Ok(()),
            }
        }
    }

    #[allow(clippy::redundant_closure_for_method_calls)]
    fn visit_journaled_merged_entries_rev<E>(
        canonical: &StableBTreeMap<
            RawIndexStoreKey,
            IndexEntryValue,
            VirtualMemory<DefaultMemoryImpl>,
        >,
        live: &HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>,
        tombstones: &BTreeSet<RawIndexStoreKey>,
        bounds: (Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>),
        mut visit: impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
    ) -> Result<(), E> {
        enum MergeStep {
            Canonical,
            Live,
            Both,
            Done,
        }

        let mut canonical_iter = canonical
            .range((bounds.0.clone(), bounds.1.clone()))
            .rev()
            .peekable();
        let mut live_iter = live.range((bounds.0, bounds.1)).rev().peekable();

        loop {
            let step = {
                let canonical_key = canonical_iter.peek().map(|entry| entry.key());
                let live_key = live_iter.peek().map(|(key, _)| *key);
                match (canonical_key, live_key) {
                    (None, None) => MergeStep::Done,
                    (Some(_), None) => MergeStep::Canonical,
                    (None, Some(_)) => MergeStep::Live,
                    (Some(canonical_key), Some(live_key)) => match canonical_key.cmp(live_key) {
                        Ordering::Less => MergeStep::Live,
                        Ordering::Equal => MergeStep::Both,
                        Ordering::Greater => MergeStep::Canonical,
                    },
                }
            };

            match step {
                MergeStep::Canonical => {
                    let entry = canonical_iter
                        .next()
                        .expect("peeked canonical journaled index entry should exist");
                    if !tombstones.contains(entry.key()) && visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
                MergeStep::Live => {
                    let (key, value) = live_iter
                        .next()
                        .expect("peeked live journaled index entry should exist");
                    if !tombstones.contains(key) && visit(key, value)? {
                        return Ok(());
                    }
                }
                MergeStep::Both => {
                    let _canonical_entry = canonical_iter
                        .next()
                        .expect("peeked canonical journaled index entry should exist");
                    let (key, value) = live_iter
                        .next()
                        .expect("peeked live journaled index entry should exist");
                    if !tombstones.contains(key) && visit(key, value)? {
                        return Ok(());
                    }
                }
                MergeStep::Done => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::direction::Direction, testing::test_memory, traits::Storable};
    use std::{borrow::Cow, convert::Infallible};

    fn raw_key(value: u8) -> RawIndexStoreKey {
        <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]))
    }

    #[test]
    fn journaled_mixed_index_range_traversal_streams_without_snapshot() {
        let mut store = IndexStore::init_journaled(test_memory(93));
        for value in [1_u8, 3, 5] {
            store.insert(raw_key(value), IndexEntryValue::presence());
        }
        store
            .fold_journaled_materialized_view()
            .expect("canonical index seed should fold");

        store.insert(raw_key(0), IndexEntryValue::presence());
        store.insert(raw_key(4), IndexEntryValue::presence());
        store.insert(raw_key(5), IndexEntryValue::presence());
        store.remove(&raw_key(1));

        let lower = Bound::Included(raw_key(0));
        let upper = Bound::Included(raw_key(5));

        reset_journaled_snapshot_call_count_for_tests();
        let mut asc = Vec::new();
        store
            .visit_journaled_entries_in_range((&lower, &upper), Direction::Asc, |key, _value| {
                asc.push(key.as_bytes()[0]);
                Ok::<_, Infallible>(asc.len() == 2)
            })
            .expect("asc journaled index range traversal should succeed");
        assert_eq!(asc, vec![0, 3]);
        assert_eq!(
            journaled_snapshot_call_count_for_tests(),
            0,
            "mixed journaled index range traversal should preserve early stop without materializing a snapshot",
        );

        reset_journaled_snapshot_call_count_for_tests();
        let mut desc = Vec::new();
        store
            .visit_journaled_entries_in_range((&lower, &upper), Direction::Desc, |key, _value| {
                desc.push(key.as_bytes()[0]);
                Ok::<_, Infallible>(desc.len() == 2)
            })
            .expect("desc journaled index range traversal should succeed");
        assert_eq!(desc, vec![5, 4]);
        assert_eq!(
            journaled_snapshot_call_count_for_tests(),
            0,
            "mixed reverse journaled index range traversal should preserve early stop without materializing a snapshot",
        );
    }
}
