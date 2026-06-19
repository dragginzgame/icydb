//! Module: index::store
//! Responsibility: journaled-or-heap index-entry storage behind the index-store boundary.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::db::{
    direction::Direction,
    index::{
        IndexEntryValue, IndexId, IndexKeyKind, cardinality::IndexPrefixCardinality,
        key::RawIndexStoreKey,
    },
    ordered_overlay::{OrderedOverlayEntry, OrderedOverlayVisit, visit_ordered_overlay},
};

use candid::CandidType;
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use serde::Deserialize;
#[cfg(any(test, feature = "diagnostics"))]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::ops::Bound;

#[cfg(test)]
thread_local! {
    static JOURNALED_SNAPSHOT_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(feature = "diagnostics")]
thread_local! {
    static INDEX_STORE_GET_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
    static INDEX_STORE_RANGE_SCAN_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
    static INDEX_STORE_ENTRY_READ_COUNT: Cell<u64> = const { Cell::new(0) };
    static INDEX_STORE_PREFIX_CARDINALITY_LOOKUP_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(feature = "diagnostics")]
fn record_index_store_get_call() {
    INDEX_STORE_GET_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

#[cfg(feature = "diagnostics")]
fn record_index_store_range_scan_call() {
    INDEX_STORE_RANGE_SCAN_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

#[cfg(feature = "diagnostics")]
fn record_index_store_entry_read() {
    INDEX_STORE_ENTRY_READ_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

#[cfg(feature = "diagnostics")]
fn record_index_store_prefix_cardinality_lookup() {
    INDEX_STORE_PREFIX_CARDINALITY_LOOKUP_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

fn visit_index_store_entry<E>(
    key: &RawIndexStoreKey,
    value: &IndexEntryValue,
    visit: &mut impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
) -> Result<bool, E> {
    #[cfg(feature = "diagnostics")]
    record_index_store_entry_read();

    visit(key, value)
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
/// Thin persistence wrapper over one journaled or heap BTreeMap.
///
/// Invariant: callers provide already-validated `RawIndexStoreKey`/`IndexEntryValue`.
///

pub struct IndexStore {
    pub(super) backend: IndexStoreBackend,
    generation: u64,
    state: IndexState,
    prefix_cardinality: IndexPrefixCardinality,
}

pub(super) enum IndexStoreBackend {
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
    Stop,
}

impl IndexStoreVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl IndexStore {
    /// Initialize a volatile heap-backed index store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: IndexStoreBackend::Heap(HeapBTreeMap::new()),
            generation: 0,
            state: IndexState::Ready,
            prefix_cardinality: IndexPrefixCardinality::synchronized_empty(),
        }
    }

    /// Initialize a journaled cached-stable index store.
    ///
    /// Normal writes update only the live materialized projection. The
    /// canonical stable index is updated by future fold/rebuild paths.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let mut store = Self {
            backend: IndexStoreBackend::Journaled {
                canonical: StableBTreeMap::init(memory),
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
            },
            generation: 0,
            state: IndexState::Ready,
            prefix_cardinality: IndexPrefixCardinality::synchronized_empty(),
        };
        store.rebuild_prefix_cardinality_from_entries(Some(0));
        store
    }

    /// Visit all index entries in canonical store order without exposing the
    /// backing stable-map iterator.
    pub(in crate::db) fn visit_entries<E>(
        &self,
        mut visitor: impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<IndexStoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            IndexStoreBackend::Heap(map) => {
                for (key, value) in map {
                    #[cfg(feature = "diagnostics")]
                    record_index_store_entry_read();

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
        #[cfg(feature = "diagnostics")]
        record_index_store_get_call();

        match &self.backend {
            IndexStoreBackend::Heap(map) => map.get(key).cloned(),
            IndexStoreBackend::Journaled { .. } => Self::journaled_get(&self.backend, key),
        }
    }

    pub fn len(&self) -> u64 {
        match &self.backend {
            IndexStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            IndexStoreBackend::Journaled { .. } => {
                let mut count = 0_u64;
                let _: Result<(), std::convert::Infallible> = self.visit_entries(|_key, _value| {
                    count = count.saturating_add(1);
                    Ok(IndexStoreVisit::Continue)
                });
                count
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.backend {
            IndexStoreBackend::Heap(map) => map.is_empty(),
            IndexStoreBackend::Journaled { .. } => {
                let mut empty = true;
                let _: Result<(), std::convert::Infallible> = self.visit_entries(|_key, _value| {
                    empty = false;
                    Ok(IndexStoreVisit::Stop)
                });
                empty
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

    /// Return an exact user-index prefix count when the index metadata is
    /// synchronized with the caller's authoritative row-store generation.
    #[must_use]
    pub(in crate::db) fn exact_prefix_cardinality(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> Option<u64> {
        #[cfg(feature = "diagnostics")]
        record_index_store_prefix_cardinality_lookup();

        self.prefix_cardinality
            .exact_count(data_generation, key_kind, index_id, components)
    }

    /// Return the sum of exact prefix counts for prefixes on the same index
    /// when synchronized metadata can prove all requested counts.
    #[must_use]
    pub(in crate::db) fn exact_prefix_cardinality_sum<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        stop_after: Option<u64>,
    ) -> Option<u64> {
        #[cfg(feature = "diagnostics")]
        record_index_store_prefix_cardinality_lookup();

        self.prefix_cardinality.exact_count_sum(
            data_generation,
            key_kind,
            index_id,
            component_prefixes,
            stop_after,
        )
    }

    /// Mark prefix-cardinality metadata synchronized with the authoritative
    /// row-store generation after a committed row/index transition.
    pub(in crate::db) const fn mark_prefix_cardinality_data_generation(&mut self, generation: u64) {
        self.prefix_cardinality.mark_synchronized(generation);
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
        let cardinality_key = key.clone();
        let previous = match &mut self.backend {
            IndexStoreBackend::Heap(map) => map.insert(key, entry.clone()),
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, entry.clone());
                previous_journaled
            }
        };
        self.prefix_cardinality
            .apply_insert(&cardinality_key, previous.as_ref(), &entry);
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
            IndexStoreBackend::Heap(map) => map.remove(key),
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(key);
                tombstones.insert(key.clone());
                previous_journaled
            }
        };
        self.prefix_cardinality.apply_remove(key, previous.as_ref());
        self.bump_generation();
        previous
    }

    pub fn clear(&mut self) {
        match &mut self.backend {
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
        self.prefix_cardinality.clear_unsynchronized();
        self.bump_generation();
    }

    /// Fold the current journaled materialized index view into the canonical
    /// stable base and clear volatile projection state.
    pub(in crate::db) fn fold_journaled_materialized_view(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let entries = Self::journaled_entries_snapshot_for_fold(&self.backend);
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        canonical.clear_new();
        for (key, value) in entries {
            canonical.insert(key, value);
        }
        live.clear();
        tombstones.clear();
        let data_generation = self.prefix_cardinality.synchronized_generation();
        self.rebuild_prefix_cardinality_from_entries(data_generation);
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

    /// Return the monotonic perf-only count of index-entry fetches seen by this process.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_get_call_count() -> u64 {
        INDEX_STORE_GET_CALL_COUNT.with(Cell::get)
    }

    /// Return the monotonic perf-only count of index range traversal probes seen by this process.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_range_scan_call_count() -> u64 {
        INDEX_STORE_RANGE_SCAN_CALL_COUNT.with(Cell::get)
    }

    /// Return the monotonic perf-only count of index entries yielded by traversal.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_entry_read_count() -> u64 {
        INDEX_STORE_ENTRY_READ_COUNT.with(Cell::get)
    }

    /// Return the monotonic perf-only count of exact prefix-cardinality probes.
    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::db) fn current_prefix_cardinality_lookup_count() -> u64 {
        INDEX_STORE_PREFIX_CARDINALITY_LOOKUP_COUNT.with(Cell::get)
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db::index) fn record_range_scan_call() {
        record_index_store_range_scan_call();
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    fn rebuild_prefix_cardinality_from_entries(&mut self, data_generation: Option<u64>) {
        self.prefix_cardinality.clear_unsynchronized();
        let entries = Self::entries_snapshot_for_cardinality(&self.backend);
        for (key, value) in &entries {
            self.prefix_cardinality.apply_insert(key, None, value);
        }
        if let Some(data_generation) = data_generation {
            self.prefix_cardinality.mark_synchronized(data_generation);
        }
    }

    fn entries_snapshot_for_cardinality(
        backend: &IndexStoreBackend,
    ) -> HeapBTreeMap<RawIndexStoreKey, IndexEntryValue> {
        match backend {
            IndexStoreBackend::Heap(map) => map.clone(),
            IndexStoreBackend::Journaled { .. } => {
                Self::journaled_entries_snapshot_for_fold(backend)
            }
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            IndexStoreBackend::Journaled { canonical: map, .. } => map.len(),
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

    pub(super) fn journaled_entries_snapshot_for_fold(
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
                    if visit_index_store_entry(key, value, &mut visit)? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc if canonical.is_empty() => {
                for (key, value) in live.range((lower, upper)).rev() {
                    if visit_index_store_entry(key, value, &mut visit)? {
                        return Ok(());
                    }
                }
            }
            Direction::Asc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)) {
                    if visit_index_store_entry(entry.key(), &entry.value(), &mut visit)? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)).rev() {
                    if visit_index_store_entry(entry.key(), &entry.value(), &mut visit)? {
                        return Ok(());
                    }
                }
            }
            Direction::Asc => {
                visit_ordered_overlay(
                    canonical.range((lower.clone(), upper.clone())),
                    live.range((lower, upper)),
                    direction,
                    |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                    |canonical_entry| !tombstones.contains(canonical_entry.key()),
                    |live_entry| !tombstones.contains(live_entry.0),
                    |entry| {
                        let should_stop = match entry {
                            OrderedOverlayEntry::Canonical(canonical_entry) => {
                                visit_index_store_entry(
                                    canonical_entry.key(),
                                    &canonical_entry.value(),
                                    &mut visit,
                                )?
                            }
                            OrderedOverlayEntry::Live((key, value)) => {
                                visit_index_store_entry(key, value, &mut visit)?
                            }
                        };
                        Ok(if should_stop {
                            OrderedOverlayVisit::Stop
                        } else {
                            OrderedOverlayVisit::Continue
                        })
                    },
                )?;
            }
            Direction::Desc => {
                visit_ordered_overlay(
                    canonical.range((lower.clone(), upper.clone())).rev(),
                    live.range((lower, upper)).rev(),
                    direction,
                    |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                    |canonical_entry| !tombstones.contains(canonical_entry.key()),
                    |live_entry| !tombstones.contains(live_entry.0),
                    |entry| {
                        let should_stop = match entry {
                            OrderedOverlayEntry::Canonical(canonical_entry) => {
                                visit_index_store_entry(
                                    canonical_entry.key(),
                                    &canonical_entry.value(),
                                    &mut visit,
                                )?
                            }
                            OrderedOverlayEntry::Live((key, value)) => {
                                visit_index_store_entry(key, value, &mut visit)?
                            }
                        };
                        Ok(if should_stop {
                            OrderedOverlayVisit::Stop
                        } else {
                            OrderedOverlayVisit::Continue
                        })
                    },
                )?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            direction::Direction,
            index::{IndexId, IndexKey, IndexKeyKind},
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        },
        testing::test_memory,
        traits::Storable,
        types::EntityTag,
    };
    use std::{borrow::Cow, convert::Infallible};

    fn raw_key(value: u8) -> RawIndexStoreKey {
        <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]))
    }

    fn indexed_raw_key(
        index_id: &IndexId,
        components: Vec<Vec<u8>>,
        primary_key: u64,
    ) -> RawIndexStoreKey {
        indexed_raw_key_with_kind(index_id, IndexKeyKind::User, components, primary_key)
    }

    fn indexed_raw_key_with_kind(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        components: Vec<Vec<u8>>,
        primary_key: u64,
    ) -> RawIndexStoreKey {
        IndexKey::new_from_components_with_primary_key_value(
            index_id,
            key_kind,
            components.as_slice(),
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(primary_key)),
        )
        .to_raw()
    }

    fn malformed_index_entry_value() -> IndexEntryValue {
        <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![0xFF]))
    }

    fn missing_index_entry_value() -> IndexEntryValue {
        <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![1]))
    }

    #[test]
    fn index_prefix_cardinality_requires_explicit_data_generation_sync() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let draft = b"Draft".to_vec();
        let review = b"Review".to_vec();
        let mut store = IndexStore::init_heap();

        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
            IndexEntryValue::presence(),
        );
        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 2),
            IndexEntryValue::presence(),
        );
        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), review.clone()], 3),
            IndexEntryValue::presence(),
        );

        assert_eq!(
            store.exact_prefix_cardinality(
                0,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            None,
            "raw index mutations must not be trusted until row generation sync is stamped",
        );

        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(3),
        );
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                index_id,
                &[collection.clone(), draft],
            ),
            Some(2),
        );
        assert_eq!(
            store.exact_prefix_cardinality(8, IndexKeyKind::User, index_id, &[collection, review],),
            None,
            "row generation drift should force the caller to use the existing-row fallback",
        );
    }

    #[test]
    fn index_prefix_cardinality_ignores_system_index_mutations() {
        let user_index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let system_index_id = IndexId::new(EntityTag::new(0xCA7D), 2);
        let collection = b"collection-a".to_vec();
        let draft = b"Draft".to_vec();
        let system_component = b"reverse-edge".to_vec();
        let mut store = IndexStore::init_heap();

        store.insert(
            indexed_raw_key(&user_index_id, vec![collection.clone(), draft.clone()], 1),
            IndexEntryValue::presence(),
        );
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection.clone(), draft.clone()],
            ),
            Some(1),
        );

        let system_key = indexed_raw_key_with_kind(
            &system_index_id,
            IndexKeyKind::System,
            vec![system_component],
            1,
        );
        store.insert(system_key.clone(), IndexEntryValue::presence());
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection.clone(), draft.clone()],
            ),
            Some(1),
            "system index writes must not invalidate synchronized user-prefix cardinality",
        );

        store.remove(&system_key);
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection.clone(), draft.clone()],
            ),
            Some(1),
            "system index removals must not invalidate synchronized user-prefix cardinality",
        );

        let malformed_system_key = indexed_raw_key_with_kind(
            &system_index_id,
            IndexKeyKind::System,
            vec![b"malformed-reverse-edge".to_vec()],
            2,
        );
        store.insert(malformed_system_key.clone(), malformed_index_entry_value());
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection.clone(), draft.clone()],
            ),
            Some(1),
            "malformed system index payloads must not invalidate user-prefix cardinality",
        );

        store.remove(&malformed_system_key);
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection.clone(), draft],
            ),
            Some(1),
            "malformed system index removals must not invalidate user-prefix cardinality",
        );

        let review = b"Review".to_vec();
        store.insert(
            indexed_raw_key(&user_index_id, vec![collection.clone(), review.clone()], 2),
            IndexEntryValue::presence(),
        );
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                user_index_id,
                &[collection, review]
            ),
            None,
            "user-prefix count changes must still require a fresh row-generation stamp",
        );
    }

    #[test]
    fn index_prefix_cardinality_ignores_missing_user_index_mutations() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let draft = b"Draft".to_vec();
        let mut store = IndexStore::init_heap();

        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
            IndexEntryValue::presence(),
        );
        store.mark_prefix_cardinality_data_generation(7);

        let stale_key = indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 2);
        store.insert(stale_key.clone(), missing_index_entry_value());
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                index_id,
                &[collection.clone(), draft.clone()],
            ),
            Some(1),
            "missing user index entries must not affect synchronized prefix cardinality",
        );

        store.remove(&stale_key);
        assert_eq!(
            store.exact_prefix_cardinality(7, IndexKeyKind::User, index_id, &[collection, draft],),
            Some(1),
            "missing user index removals must not affect synchronized prefix cardinality",
        );
    }

    #[cfg(feature = "diagnostics")]
    #[test]
    fn index_store_diagnostic_counters_record_gets_range_scans_and_entry_reads() {
        let mut store = IndexStore::init_heap();
        store.insert(raw_key(7), IndexEntryValue::presence());
        store.insert(raw_key(9), IndexEntryValue::presence());

        let gets_before = IndexStore::current_get_call_count();
        assert_eq!(store.get(&raw_key(7)), Some(IndexEntryValue::presence()));
        assert_eq!(store.get(&raw_key(8)), None);

        assert_eq!(
            IndexStore::current_get_call_count().saturating_sub(gets_before),
            2,
            "diagnostic index-store get counter should count both hit and miss reads",
        );

        let range_scans_before = IndexStore::current_range_scan_call_count();
        let lower = Bound::Included(raw_key(7));
        let upper = Bound::Included(raw_key(9));
        store
            .visit_raw_entries_in_range((&lower, &upper), Direction::Asc, |_key, _entry| Ok(false))
            .expect("raw index range visit should succeed");

        assert_eq!(
            IndexStore::current_range_scan_call_count().saturating_sub(range_scans_before),
            1,
            "diagnostic index-store range-scan counter should count one range traversal probe",
        );

        let entries_before = IndexStore::current_entry_read_count();
        store
            .visit_entries(|_key, _entry| Ok::<_, Infallible>(IndexStoreVisit::Continue))
            .expect("index entry visit should succeed");

        assert_eq!(
            IndexStore::current_entry_read_count().saturating_sub(entries_before),
            2,
            "diagnostic index-store entry counter should count yielded traversal entries",
        );
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
