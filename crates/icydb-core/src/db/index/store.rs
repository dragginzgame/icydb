//! Module: index::store
//! Responsibility: journaled-or-heap index-entry storage behind the index-store boundary.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::db::index::{IndexId, IndexKeyKind};
use crate::db::{
    direction::Direction,
    index::{
        IndexEntryValue,
        cardinality::{IndexPrefixCardinality, IndexPrefixCardinalityDelta},
        key::RawIndexStoreKey,
    },
    journal::FoldWatermark,
    ordered_overlay::{OrderedOverlayEntry, ordered_overlay_entries},
    positioned_overlay::{
        JournalOverlayPosition, PositionedOverlayMetadata, PositionedOverlayRetirement,
    },
};

use candid::CandidType;
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use serde::Deserialize;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::ops::Bound;

#[cfg(test)]
thread_local! {
    static JOURNALED_SNAPSHOT_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static INDEX_STORE_ENTRY_READ_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_index_store_entry_read() {
    INDEX_STORE_ENTRY_READ_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

fn visit_index_store_entry<E>(
    key: &RawIndexStoreKey,
    value: &IndexEntryValue,
    visit: &mut impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
) -> Result<bool, E> {
    #[cfg(test)]
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
}

impl IndexState {
    /// Return the stable lowercase text label for this lifecycle state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Ready => "ready",
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
    access_state_revision: u64,
    prefix_cardinality: IndexPrefixCardinality,
}

pub(super) enum IndexStoreBackend {
    Heap(HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>),
    Journaled {
        canonical:
            StableBTreeMap<RawIndexStoreKey, IndexEntryValue, VirtualMemory<DefaultMemoryImpl>>,
        live: HeapBTreeMap<RawIndexStoreKey, IndexEntryValue>,
        tombstones: BTreeSet<RawIndexStoreKey>,
        positions: PositionedOverlayMetadata<RawIndexStoreKey>,
        prefix_cardinality_delta: Box<IndexPrefixCardinalityDelta>,
    },
}

/// Preflighted provenance publication for explicit journal index records.
pub(in crate::db) struct PreparedIndexPositionPublication {
    keys: Vec<RawIndexStoreKey>,
    position: JournalOverlayPosition,
}

/// Preflighted exact retirement for one complete journal batch.
pub(in crate::db) struct PreparedIndexPositionRetirement {
    entries: Vec<(RawIndexStoreKey, PositionedOverlayRetirement)>,
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
            access_state_revision: 1,
            prefix_cardinality: IndexPrefixCardinality::synchronized_empty(),
        }
    }

    /// Initialize a journaled cached-stable index store.
    ///
    /// Normal writes update only the live materialized projection. The
    /// canonical stable index is updated by future fold/rebuild paths.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let canonical = StableBTreeMap::init(memory);
        let prefix_cardinality = if canonical.is_empty() {
            IndexPrefixCardinality::synchronized_empty()
        } else {
            IndexPrefixCardinality::unavailable()
        };
        Self {
            backend: IndexStoreBackend::Journaled {
                canonical,
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
                positions: PositionedOverlayMetadata::new(),
                prefix_cardinality_delta: Box::new(IndexPrefixCardinalityDelta::unbound_empty()),
            },
            generation: 0,
            state: IndexState::Ready,
            access_state_revision: 1,
            // Exact zero cardinality is known for an empty canonical map;
            // populated maps remain unavailable without a startup scan.
            prefix_cardinality,
        }
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
                    #[cfg(test)]
                    record_index_store_entry_read();

                    if visitor(key, value)?.should_stop() {
                        return Ok(());
                    }
                }
            }
            IndexStoreBackend::Journaled { .. } => self.visit_journaled_entries_in_range(
                (&Bound::Unbounded, &Bound::Unbounded),
                Direction::Asc,
                |key, value| visitor(key, value).map(IndexStoreVisit::should_stop),
            )?,
        }

        Ok(())
    }

    pub(in crate::db) fn get(&self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        match &self.backend {
            IndexStoreBackend::Heap(map) => map.get(key).cloned(),
            IndexStoreBackend::Journaled { .. } => Self::journaled_get(&self.backend, key),
        }
    }

    /// Load one index entry from the canonical predecessor view.
    pub(in crate::db) fn get_canonical(&self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        match &self.backend {
            IndexStoreBackend::Heap(map) => map.get(key).cloned(),
            IndexStoreBackend::Journaled { canonical, .. } => canonical.get(key),
        }
    }

    /// Return whether the canonical predecessor domain is physically empty.
    ///
    /// This bounded root observation never walks live overlays or materializes
    /// index entries.
    pub(in crate::db) fn canonical_is_empty(&self) -> Result<bool, crate::error::InternalError> {
        match &self.backend {
            IndexStoreBackend::Journaled { canonical, .. } => Ok(canonical.is_empty()),
            IndexStoreBackend::Heap(_) => Err(crate::error::InternalError::store_invariant()),
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

    /// Return the current physical access-readiness revision.
    #[must_use]
    pub(in crate::db) const fn access_state_revision(&self) -> u64 {
        self.access_state_revision
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
        self.prefix_cardinality
            .exact_count(data_generation, key_kind, index_id, components)
    }

    /// Return the exact number of distinct non-empty leading components for
    /// one user index, bounded by `stop_after`, when metadata is synchronized.
    #[cfg(test)]
    pub(in crate::db) fn exact_first_component_distinct_cardinality(
        &self,
        data_generation: u64,
        index_id: IndexId,
        stop_after: u64,
    ) -> Result<Option<(u64, u64)>, crate::error::InternalError> {
        self.prefix_cardinality
            .exact_first_component_distinct_count(data_generation, index_id, stop_after)
    }

    /// Sum exact first-component multiplicities within the caller's bounded range and work cap.
    pub(in crate::db) fn exact_first_component_range_cardinality(
        &self,
        data_generation: u64,
        index_id: IndexId,
        lower: &Bound<Vec<u8>>,
        upper: &Bound<Vec<u8>>,
        stop_after: u64,
    ) -> Result<Option<(u64, u64, bool)>, crate::error::InternalError> {
        self.prefix_cardinality.exact_first_component_range_count(
            data_generation,
            index_id,
            lower,
            upper,
            stop_after,
        )
    }

    pub(in crate::db) fn exact_first_component_numeric_fold(
        &self,
        data_generation: u64,
        index_id: IndexId,
        stop_after: u64,
    ) -> Result<Option<(u64, i128, u64, bool)>, crate::error::InternalError> {
        self.prefix_cardinality.exact_first_component_numeric_fold(
            data_generation,
            index_id,
            stop_after,
        )
    }

    /// Return the exact live-overlay delta from canonical for one user-index prefix.
    #[must_use]
    pub(in crate::db) fn exact_prefix_cardinality_delta(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> Option<i64> {
        match &self.backend {
            IndexStoreBackend::Heap(_) => Some(0),
            IndexStoreBackend::Journaled {
                prefix_cardinality_delta,
                ..
            } => prefix_cardinality_delta.exact_delta(key_kind, index_id, components),
        }
    }

    /// Return the canonical fold boundary owning the exact live-overlay delta.
    #[must_use]
    pub(in crate::db) fn exact_prefix_cardinality_delta_watermark(&self) -> Option<FoldWatermark> {
        match &self.backend {
            IndexStoreBackend::Heap(_) => None,
            IndexStoreBackend::Journaled {
                prefix_cardinality_delta,
                ..
            } => prefix_cardinality_delta.base_watermark(),
        }
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
        self.prefix_cardinality.exact_count_sum(
            data_generation,
            key_kind,
            index_id,
            component_prefixes,
            stop_after,
        )
    }

    /// Return non-empty exact child prefixes under a sparse set of already-encoded
    /// parent prefixes when synchronized metadata can prove the bounded child set.
    #[must_use]
    pub(in crate::db) fn exact_child_prefixes_for_parent_set<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        parent_component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        max_children: usize,
    ) -> Option<Vec<Vec<Vec<u8>>>> {
        self.prefix_cardinality.exact_child_prefixes_for_parent_set(
            data_generation,
            key_kind,
            index_id,
            parent_component_prefixes,
            max_children,
        )
    }

    /// Mark prefix-cardinality metadata synchronized with the authoritative
    /// row-store generation after a committed row/index transition.
    pub(in crate::db) const fn mark_prefix_cardinality_data_generation(&mut self, generation: u64) {
        self.prefix_cardinality.mark_synchronized(generation);
    }

    /// Mark this index store as in-progress and therefore ineligible for
    /// planner visibility until a full authoritative rebuild ends.
    pub(in crate::db) const fn set_access_state(&mut self, state: IndexState, revision: u64) {
        self.state = state;
        self.access_state_revision = revision;
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
        self.apply_prefix_overlay_delta(&cardinality_key, previous.as_ref(), Some(&entry));
        self.bump_generation();
        previous
    }

    /// Insert one key whose absence was proved by complete-domain staging.
    ///
    /// Accepted-schema replacement first removes its complete current user
    /// domain. Raw index identity makes every final key owner-local, so no
    /// canonical point lookup can add information during mechanical Apply.
    pub(in crate::db) fn insert_preflighted_absent(
        &mut self,
        key: RawIndexStoreKey,
        entry: IndexEntryValue,
    ) {
        let cardinality_key = key.clone();
        match &mut self.backend {
            IndexStoreBackend::Heap(map) => {
                map.insert(key, entry.clone());
            }
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, entry.clone());
            }
        }
        self.prefix_cardinality
            .apply_insert(&cardinality_key, None, &entry);
        self.apply_prefix_overlay_delta(&cardinality_key, None, Some(&entry));
        self.bump_generation();
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
        self.apply_prefix_overlay_delta(key, previous.as_ref(), None);
        self.bump_generation();
        previous
    }

    /// Reset the disposable journaled index overlay without traversing or
    /// mutating the canonical stable index.
    pub(in crate::db) fn reset_journaled_live_projection(
        &mut self,
        data_generation: u64,
        fold_watermark: FoldWatermark,
    ) -> Result<(), crate::error::InternalError> {
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
            prefix_cardinality_delta,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        live.clear();
        tombstones.clear();
        positions.clear();
        prefix_cardinality_delta.reset(fold_watermark);
        self.prefix_cardinality = if canonical.is_empty() {
            let mut cardinality = IndexPrefixCardinality::synchronized_empty();
            cardinality.mark_synchronized(data_generation);
            cardinality
        } else {
            IndexPrefixCardinality::unavailable()
        };
        self.bump_generation();

        Ok(())
    }

    /// Preflight movement of the exact delta's canonical base boundary.
    pub(in crate::db) fn preflight_prefix_cardinality_delta_watermark(
        &self,
        current: FoldWatermark,
    ) -> Result<(), crate::error::InternalError> {
        (self.exact_prefix_cardinality_delta_watermark() == Some(current))
            .then_some(())
            .ok_or_else(crate::error::InternalError::store_corruption)
    }

    /// Publish a preflighted canonical base movement after the complete fold.
    pub(in crate::db) fn apply_prefix_cardinality_delta_watermark(
        &mut self,
        current: FoldWatermark,
        next: FoldWatermark,
    ) {
        if let IndexStoreBackend::Journaled {
            prefix_cardinality_delta,
            ..
        } = &mut self.backend
        {
            prefix_cardinality_delta.advance_watermark(current, next);
        }
    }

    /// Publish one preflighted positioned derived or explicit index effect.
    pub(in crate::db) fn publish_preflighted_journal_entry(
        &mut self,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
        position: JournalOverlayPosition,
    ) -> Result<Option<IndexEntryValue>, crate::error::InternalError> {
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
            prefix_cardinality_delta,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let previous = if tombstones.contains(&key) {
            None
        } else {
            live.get(&key).cloned().or_else(|| canonical.get(&key))
        };
        let cardinality_key = key.clone();
        let next_value = value.clone();

        if let Some(value) = value {
            tombstones.remove(&key);
            live.insert(key.clone(), value.clone());
            self.prefix_cardinality
                .apply_insert(&cardinality_key, previous.as_ref(), &value);
        } else {
            live.remove(&key);
            tombstones.insert(key.clone());
            self.prefix_cardinality
                .apply_remove(&cardinality_key, previous.as_ref());
        }
        prefix_cardinality_delta.apply_transition(
            &cardinality_key,
            previous.as_ref(),
            next_value.as_ref(),
        );
        positions.publish_preflighted(key, position);
        self.bump_generation();

        Ok(previous)
    }

    /// Validate and publish one positioned index effect for direct store tests.
    #[cfg(test)]
    pub(in crate::db) fn publish_positioned_journal_entry(
        &mut self,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
        position: JournalOverlayPosition,
    ) -> Result<Option<IndexEntryValue>, crate::error::InternalError> {
        self.preflight_positioned_journal_entry(&key, position)?;
        self.publish_preflighted_journal_entry(key, value, position)
    }

    /// Preflight index provenance before marker publication.
    pub(in crate::db) fn preflight_positioned_journal_entry(
        &self,
        key: &RawIndexStoreKey,
        position: JournalOverlayPosition,
    ) -> Result<(), crate::error::InternalError> {
        let IndexStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        positions.preflight_publish(key, position)
    }

    /// Preflight explicit index provenance before marker publication.
    pub(in crate::db) fn prepare_position_publication(
        &self,
        keys: impl IntoIterator<Item = RawIndexStoreKey>,
        position: JournalOverlayPosition,
    ) -> Result<PreparedIndexPositionPublication, crate::error::InternalError> {
        let IndexStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let keys = keys.into_iter().collect::<BTreeSet<_>>();
        for key in &keys {
            positions.preflight_publish(key, position)?;
        }
        Ok(PreparedIndexPositionPublication {
            keys: keys.into_iter().collect(),
            position,
        })
    }

    /// Publish explicit index provenance after its values have been applied.
    pub(in crate::db) fn publish_prepared_positions(
        &mut self,
        prepared: PreparedIndexPositionPublication,
    ) {
        let IndexStoreBackend::Journaled { positions, .. } = &mut self.backend else {
            debug_assert!(
                false,
                "preflighted index positions require a journaled store"
            );
            return;
        };
        for key in prepared.keys {
            positions.publish_preflighted(key, prepared.position);
        }
    }

    /// Preflight exact index-overlay retirement before canonical mutation.
    pub(in crate::db) fn prepare_position_retirement(
        &self,
        keys: impl IntoIterator<Item = RawIndexStoreKey>,
        position: JournalOverlayPosition,
    ) -> Result<PreparedIndexPositionRetirement, crate::error::InternalError> {
        let IndexStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let entries = keys
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|key| {
                positions
                    .preflight_retirement(&key, position)
                    .map(|retirement| (key, retirement))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PreparedIndexPositionRetirement { entries })
    }

    /// Retire only exact index overlays after canonical mutation succeeds.
    pub(in crate::db) fn apply_prepared_position_retirement(
        &mut self,
        prepared: PreparedIndexPositionRetirement,
    ) {
        let IndexStoreBackend::Journaled {
            live,
            tombstones,
            positions,
            ..
        } = &mut self.backend
        else {
            debug_assert!(
                false,
                "preflighted index retirement requires a journaled store"
            );
            return;
        };
        for (key, retirement) in prepared.entries {
            if retirement == PositionedOverlayRetirement::Exact {
                live.remove(&key);
                tombstones.remove(&key);
                positions.retire_preflighted(&key, retirement);
            }
        }
    }

    #[cfg(test)]
    fn retire_positioned_journal_effect(
        &mut self,
        key: &RawIndexStoreKey,
        position: JournalOverlayPosition,
    ) -> Result<PositionedOverlayRetirement, crate::error::InternalError> {
        let IndexStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let retirement = positions.preflight_retirement(key, position)?;
        let prepared = PreparedIndexPositionRetirement {
            entries: vec![(key.clone(), retirement)],
        };
        self.apply_prepared_position_retirement(prepared);
        Ok(retirement)
    }

    /// Apply one recovered index entry directly to canonical stable storage.
    pub(in crate::db) fn fold_recovered_journal_entry(
        &mut self,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
    ) -> Result<(), crate::error::InternalError> {
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        let visible = !live.contains_key(&key) && !tombstones.contains(&key);
        let cardinality_key = key.clone();
        let previous = if let Some(value) = value.as_ref() {
            canonical.insert(key, value.clone())
        } else {
            canonical.remove(&key)
        };
        if visible {
            if let Some(value) = value.as_ref() {
                self.prefix_cardinality
                    .apply_insert(&cardinality_key, previous.as_ref(), value);
            } else {
                self.prefix_cardinality
                    .apply_remove(&cardinality_key, previous.as_ref());
            }
        } else {
            self.apply_prefix_overlay_delta(&cardinality_key, value.as_ref(), previous.as_ref());
        }
        self.bump_generation();

        Ok(())
    }

    /// Prove that recovered journal entries can be folded into canonical storage.
    pub(in crate::db) fn preflight_fold_recovered_journal(
        &self,
    ) -> Result<(), crate::error::InternalError> {
        match self.backend {
            IndexStoreBackend::Journaled { .. } => Ok(()),
            IndexStoreBackend::Heap(_) => Err(crate::error::InternalError::store_invariant()),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.backend {
            IndexStoreBackend::Heap(map) => map.clear(),
            IndexStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                prefix_cardinality_delta,
                ..
            } => {
                live.clear();
                tombstones.clear();
                for entry in canonical.iter() {
                    tombstones.insert(entry.key().clone());
                }
                prefix_cardinality_delta.clear_unavailable();
            }
        }
        self.prefix_cardinality.clear_unsynchronized();
        self.bump_generation();
    }

    /// Fold the current journaled materialized index view into the canonical
    /// stable base and clear volatile projection state.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn fold_journaled_materialized_view(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let entries = Self::journaled_entries_snapshot_for_fold(&self.backend);
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            prefix_cardinality_delta,
            ..
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
        if let Some(watermark) = prefix_cardinality_delta.base_watermark() {
            prefix_cardinality_delta.reset(watermark);
        } else {
            **prefix_cardinality_delta = IndexPrefixCardinalityDelta::unbound_empty();
        }
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

    /// Return the monotonic perf-only count of index entries yielded by traversal.
    #[cfg(test)]
    pub(in crate::db) fn current_entry_read_count() -> u64 {
        INDEX_STORE_ENTRY_READ_COUNT.with(Cell::get)
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    fn apply_prefix_overlay_delta(
        &mut self,
        key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
        next: Option<&IndexEntryValue>,
    ) {
        let IndexStoreBackend::Journaled {
            prefix_cardinality_delta,
            ..
        } = &mut self.backend
        else {
            return;
        };
        prefix_cardinality_delta.apply_transition(key, previous, next);
    }

    #[cfg(any(test, feature = "migration"))]
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

    #[cfg(any(test, feature = "migration"))]
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

    fn journaled_get(
        backend: &IndexStoreBackend,
        key: &RawIndexStoreKey,
    ) -> Option<IndexEntryValue> {
        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = backend
        else {
            return None;
        };

        if tombstones.contains(key) {
            return None;
        }
        live.get(key).cloned().or_else(|| canonical.get(key))
    }

    #[cfg(any(test, feature = "migration"))]
    pub(super) fn journaled_entries_snapshot_for_fold(
        backend: &IndexStoreBackend,
    ) -> HeapBTreeMap<RawIndexStoreKey, IndexEntryValue> {
        #[cfg(test)]
        record_journaled_snapshot_call();

        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
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
            ..
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
                for entry in ordered_overlay_entries(
                    canonical.range((lower.clone(), upper.clone())),
                    live.range((lower, upper)),
                    direction,
                    |entry| entry.key(),
                    |entry| entry.0,
                    tombstones,
                ) {
                    let should_stop = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => visit_index_store_entry(
                            canonical_entry.key(),
                            &canonical_entry.value(),
                            &mut visit,
                        )?,
                        OrderedOverlayEntry::Live((key, value)) => {
                            visit_index_store_entry(key, value, &mut visit)?
                        }
                    };
                    if should_stop {
                        return Ok(());
                    }
                }
            }
            Direction::Desc => {
                for entry in ordered_overlay_entries(
                    canonical.range((lower.clone(), upper.clone())).rev(),
                    live.range((lower, upper)).rev(),
                    direction,
                    |entry| entry.key(),
                    |entry| entry.0,
                    tombstones,
                ) {
                    let should_stop = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => visit_index_store_entry(
                            canonical_entry.key(),
                            &canonical_entry.value(),
                            &mut visit,
                        )?,
                        OrderedOverlayEntry::Live((key, value)) => {
                            visit_index_store_entry(key, value, &mut visit)?
                        }
                    };
                    if should_stop {
                        return Ok(());
                    }
                }
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
            journal::JournalSequence,
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
            positioned_overlay::{JournalOverlayPosition, PositionedOverlayRetirement},
            registry::StoreAllocationIdentity,
        },
        testing::test_memory,
        types::EntityTag,
    };
    use ic_stable_structures::Storable;
    use std::{borrow::Cow, convert::Infallible};

    fn raw_key(value: u8) -> RawIndexStoreKey {
        <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]))
    }

    fn overlay_position(sequence: u64) -> JournalOverlayPosition {
        JournalOverlayPosition::new(
            StoreAllocationIdentity::new(231, "test::index"),
            JournalSequence::new(sequence),
        )
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
        .expect("test index key should build")
        .to_raw()
        .expect("test index key should encode")
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
    fn first_component_distinct_cardinality_is_exact_bounded_and_generation_matched() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let alpha = b"alpha".to_vec();
        let beta = b"beta".to_vec();
        let alpha_one = indexed_raw_key(&index_id, vec![alpha.clone()], 1);
        let alpha_two = indexed_raw_key(&index_id, vec![alpha], 2);
        let beta_one = indexed_raw_key(&index_id, vec![beta], 3);
        let mut store = IndexStore::init_heap();

        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(0, index_id, 1)
                .expect("initialized metadata should be structurally valid"),
            Some((0, 0)),
            "initialized empty metadata must positively prove exact zero",
        );
        store.insert(alpha_one.clone(), IndexEntryValue::presence());
        store.insert(alpha_two.clone(), IndexEntryValue::presence());
        store.insert(beta_one.clone(), IndexEntryValue::presence());
        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(0, index_id, 3)
                .expect("invalidated metadata should remain structurally valid"),
            None,
            "an unstamped mutation must make optional metadata unavailable",
        );

        store.mark_prefix_cardinality_data_generation(7);
        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(7, index_id, 3)
                .expect("synchronized metadata should be structurally valid"),
            Some((2, 2)),
            "duplicate physical entries must contribute one leading component",
        );
        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(7, index_id, 1)
                .expect("bounded metadata should be structurally valid"),
            Some((1, 1)),
            "stop-after must bound both result evidence and metadata work",
        );
        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(8, index_id, 3)
                .expect("stale metadata should remain structurally valid"),
            None,
            "row-generation drift must fail closed",
        );

        store.remove(&alpha_one);
        store.remove(&alpha_two);
        store.remove(&beta_one);
        store.mark_prefix_cardinality_data_generation(8);
        assert_eq!(
            store
                .exact_first_component_distinct_cardinality(8, index_id, 1)
                .expect("empty metadata should be structurally valid"),
            Some((0, 0)),
            "deleting every value must restore a positive exact-zero proof",
        );
    }

    #[test]
    fn index_prefix_cardinality_enumerates_bounded_child_prefixes() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let other_collection = b"collection-b".to_vec();
        let draft = b"Draft".to_vec();
        let review = b"Review".to_vec();
        let published = b"Published".to_vec();
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
        store.insert(
            indexed_raw_key(
                &index_id,
                vec![other_collection.clone(), published.clone()],
                4,
            ),
            IndexEntryValue::presence(),
        );
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            ),
            Some(vec![
                vec![collection.clone(), draft],
                vec![collection.clone(), review],
            ]),
            "child-prefix enumeration should return deterministic unique children under the requested parent",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&other_collection)],
                4,
            ),
            Some(vec![vec![other_collection, published]]),
            "child-prefix enumeration must stay scoped to the requested parent prefix",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                8,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            ),
            None,
            "row generation drift should keep child-prefix expansion fail-closed",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                1,
            ),
            None,
            "over-cap child-prefix expansion should fall back to the existing route",
        );
    }

    #[test]
    fn index_prefix_cardinality_batches_sparse_child_prefixes() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let other_collection = b"collection-b".to_vec();
        let missing_a = b"missing-a".to_vec();
        let missing_b = b"missing-b".to_vec();
        let draft = b"Draft".to_vec();
        let review = b"Review".to_vec();
        let published = b"Published".to_vec();
        let mut store = IndexStore::init_heap();

        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
            IndexEntryValue::presence(),
        );
        store.insert(
            indexed_raw_key(&index_id, vec![collection.clone(), review.clone()], 2),
            IndexEntryValue::presence(),
        );
        store.insert(
            indexed_raw_key(
                &index_id,
                vec![other_collection.clone(), published.clone()],
                3,
            ),
            IndexEntryValue::presence(),
        );
        store.mark_prefix_cardinality_data_generation(7);

        let parents = [
            std::slice::from_ref(&missing_a),
            std::slice::from_ref(&collection),
            std::slice::from_ref(&missing_b),
            std::slice::from_ref(&other_collection),
        ];
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(7, IndexKeyKind::User, index_id, parents, 4,),
            Some(vec![
                vec![collection.clone(), draft],
                vec![collection.clone(), review],
                vec![other_collection.clone(), published],
            ]),
            "batched child-prefix enumeration should skip missing sparse parents and return deterministic real children",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [
                    std::slice::from_ref(&missing_a),
                    std::slice::from_ref(&missing_b)
                ],
                4,
            ),
            Some(Vec::new()),
            "missing-only sparse parent sets should be proven empty when cardinality is synchronized",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [
                    std::slice::from_ref(&collection),
                    std::slice::from_ref(&other_collection)
                ],
                2,
            ),
            None,
            "over-cap sparse parent-set expansion should fail closed",
        );
        assert_eq!(
            store.exact_child_prefixes_for_parent_set(
                8,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            ),
            None,
            "generation drift should keep batched child-prefix expansion fail-closed",
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

    #[test]
    fn journaled_index_store_reopens_without_materializing_prefix_cardinality() {
        let memory = test_memory(94);
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let mut store = IndexStore::init_journaled(memory.clone());
        let key = indexed_raw_key(&index_id, vec![collection.clone()], 1);
        store.insert(key.clone(), IndexEntryValue::presence());
        store
            .fold_journaled_materialized_view()
            .expect("canonical index seed should fold");
        drop(store);

        let mut reopened = IndexStore::init_journaled(memory);

        assert_eq!(reopened.get(&key), Some(IndexEntryValue::presence()));
        assert_eq!(
            reopened.exact_prefix_cardinality(
                0,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            None,
            "startup must leave optional prefix cardinality unavailable without scanning stable entries",
        );
        assert_eq!(
            reopened
                .exact_first_component_distinct_cardinality(0, index_id, 2)
                .expect("unmaterialized metadata should remain structurally valid"),
            None,
            "startup must not treat an absent materialized leading-component map as exact evidence",
        );
        assert_eq!(
            reopened.exact_prefix_cardinality_delta(
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
        );
        let second = indexed_raw_key(&index_id, vec![collection.clone()], 2);
        reopened.insert(second.clone(), IndexEntryValue::presence());
        assert_eq!(
            reopened.exact_prefix_cardinality_delta(
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(1),
        );
        reopened
            .fold_recovered_journal_entry(second, Some(IndexEntryValue::presence()))
            .expect("matching canonical index entry should fold");
        assert_eq!(
            reopened.exact_prefix_cardinality_delta(
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
            "canonical fold must consume only its exact overlay prefix contribution",
        );
    }

    #[test]
    fn empty_journaled_index_store_retains_exact_prefix_cardinality_without_scanning() {
        let memory = test_memory(95);
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let mut store = IndexStore::init_journaled(memory.clone());

        assert_eq!(
            store.exact_prefix_cardinality(
                0,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
        );
        store
            .reset_journaled_live_projection(7, FoldWatermark::initial())
            .expect("empty projection reset should succeed");
        assert_eq!(
            store.exact_prefix_cardinality(
                7,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
        );
        drop(store);

        let reopened = IndexStore::init_journaled(memory);
        assert_eq!(
            reopened.exact_prefix_cardinality(
                0,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
        );
    }

    #[test]
    fn journaled_prefix_delta_binds_and_advances_its_canonical_watermark() {
        let mut store = IndexStore::init_journaled(test_memory(99));
        assert_eq!(store.exact_prefix_cardinality_delta_watermark(), None);

        let current = FoldWatermark::new(JournalSequence::new(7), 3);
        let next = FoldWatermark::new(JournalSequence::new(8), 4);
        store
            .reset_journaled_live_projection(0, current)
            .expect("delta reset should bind the current canonical watermark");
        assert_eq!(
            store.exact_prefix_cardinality_delta_watermark(),
            Some(current)
        );
        store
            .preflight_prefix_cardinality_delta_watermark(current)
            .expect("matching watermark should preflight");
        assert!(
            store
                .preflight_prefix_cardinality_delta_watermark(next)
                .is_err(),
            "a stale or future base must fail closed",
        );

        store.apply_prefix_cardinality_delta_watermark(current, next);
        assert_eq!(store.exact_prefix_cardinality_delta_watermark(), Some(next));
    }

    #[test]
    fn recovered_index_fold_maintains_available_prefix_cardinality() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let collection = b"collection-a".to_vec();
        let key = indexed_raw_key(&index_id, vec![collection.clone()], 1);
        let mut store = IndexStore::init_journaled(test_memory(96));

        store
            .fold_recovered_journal_entry(key.clone(), Some(IndexEntryValue::presence()))
            .expect("recovered index put should fold");
        store.mark_prefix_cardinality_data_generation(1);
        assert_eq!(
            store.exact_prefix_cardinality(
                1,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(1),
        );

        store
            .fold_recovered_journal_entry(key, None)
            .expect("recovered index delete should fold");
        store.mark_prefix_cardinality_data_generation(2);
        assert_eq!(
            store.exact_prefix_cardinality(
                2,
                IndexKeyKind::User,
                index_id,
                std::slice::from_ref(&collection),
            ),
            Some(0),
        );
    }

    #[test]
    fn positioned_index_overlay_preserves_later_membership_until_exact_retirement() {
        let key = raw_key(7);
        let mut store = IndexStore::init_journaled(test_memory(97));
        store
            .fold_recovered_journal_entry(key.clone(), Some(IndexEntryValue::presence()))
            .expect("canonical membership should seed");

        store
            .publish_positioned_journal_entry(key.clone(), None, overlay_position(1))
            .expect("positioned tombstone should publish");
        store
            .publish_positioned_journal_entry(
                key.clone(),
                Some(IndexEntryValue::presence()),
                overlay_position(2),
            )
            .expect("later membership should supersede the tombstone");
        store
            .fold_recovered_journal_entry(key.clone(), None)
            .expect("tombstone batch should become canonical");
        assert_eq!(
            store
                .retire_positioned_journal_effect(&key, overlay_position(1))
                .expect("older retirement should preserve later membership"),
            PositionedOverlayRetirement::Superseded,
        );
        assert_eq!(store.get(&key), Some(IndexEntryValue::presence()));

        store
            .fold_recovered_journal_entry(key.clone(), Some(IndexEntryValue::presence()))
            .expect("membership batch should become canonical");
        assert_eq!(
            store
                .retire_positioned_journal_effect(&key, overlay_position(2))
                .expect("latest retirement should be exact"),
            PositionedOverlayRetirement::Exact,
        );
        assert_eq!(store.get(&key), Some(IndexEntryValue::presence()));

        let mut visible = Vec::new();
        store
            .visit_entries(|visited_key, value| {
                visible.push((visited_key.clone(), value.clone()));
                Ok::<_, Infallible>(IndexStoreVisit::Continue)
            })
            .expect("positioned index should remain range-visible");
        assert_eq!(visible, vec![(key, IndexEntryValue::presence())]);
    }

    #[test]
    fn positioned_index_overlay_coalesces_repeated_same_batch_target() {
        let key = raw_key(8);
        let position = overlay_position(3);
        let mut store = IndexStore::init_journaled(test_memory(98));

        store
            .publish_positioned_journal_entry(
                key.clone(),
                Some(IndexEntryValue::presence()),
                position,
            )
            .expect("first same-batch effect should publish");
        store
            .publish_positioned_journal_entry(key.clone(), None, position)
            .expect("final same-batch effect should coalesce by logical target");
        store
            .fold_recovered_journal_entry(key.clone(), None)
            .expect("coalesced final effect should become canonical");
        assert_eq!(
            store
                .retire_positioned_journal_effect(&key, position)
                .expect("coalesced target should retire once"),
            PositionedOverlayRetirement::Exact,
        );
        assert!(store.get(&key).is_none());
    }
}
