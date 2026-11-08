//! Module: index::store
//! Responsibility: journaled-or-heap index-entry storage behind the index-store boundary.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

// Test counters stay at the actual store access boundary; counter state and helpers live in helper_tests.

use crate::db::index::{IndexId, IndexKeyKind, decode_index_key_frame};
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
    storage::open_stable_map,
};
use crate::types::EntityTag;

use crate::error::InternalError;

use ic_storage::adapter::{MapEntry, StableBTreeMap};
use ic_storage::{DefaultMemoryImpl, RuntimeMemory};
use serde::Deserialize;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet, btree_map::Entry};
use std::ops::{Bound, ControlFlow};

fn visit_index_store_entry<E: From<InternalError>>(
    key: &RawIndexStoreKey,
    value: &IndexEntryValue,
    visit: &mut (impl FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E> + ?Sized),
) -> Result<bool, E> {
    #[cfg(test)]
    record_index_store_entry_read();

    visit(key, value)
}

//
// IndexState
//
// Explicit lifecycle visibility state for one index store.
// Visibility matters because planner-visible indexes must already be complete:
// the index contents are fully built and query-visible for reads.
//
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
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
        canonical: Box<StableBTreeMap<RawIndexStoreKey, IndexEntryValue, DefaultMemoryImpl>>,
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
    ///
    /// # Errors
    /// Returns a store error if creation or retained collection validation fails.
    /// Invalid retained contents are not replaced.
    ///
    /// # Panics
    /// Propagates backing-storage or codec panics.
    pub fn init_journaled(memory: RuntimeMemory<DefaultMemoryImpl>) -> Result<Self, InternalError> {
        let canonical = open_stable_map(memory).map_err(InternalError::from)?;
        let prefix_cardinality = if canonical.is_empty() {
            IndexPrefixCardinality::synchronized_empty()
        } else {
            IndexPrefixCardinality::unavailable()
        };
        Ok(Self {
            backend: IndexStoreBackend::Journaled {
                canonical: Box::new(canonical),
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
        })
    }

    /// Visit all index entries in canonical store order without exposing the
    /// backing stable-map iterator.
    pub(in crate::db) fn visit_entries<E: From<InternalError>>(
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
                (Bound::Unbounded, Bound::Unbounded),
                Direction::Asc,
                &mut |key, value| visitor(key, value).map(IndexStoreVisit::should_stop),
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

    pub fn len(&self) -> Result<u64, InternalError> {
        Ok(match &self.backend {
            IndexStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            IndexStoreBackend::Journaled { .. } => {
                let mut count = 0_u64;
                self.visit_entries(|_key, _value| {
                    count = count.saturating_add(1);
                    Ok::<_, InternalError>(IndexStoreVisit::Continue)
                })?;
                count
            }
        })
    }

    pub fn is_empty(&self) -> Result<bool, InternalError> {
        Ok(match &self.backend {
            IndexStoreBackend::Heap(map) => map.is_empty(),
            IndexStoreBackend::Journaled { .. } => {
                let mut empty = true;
                self.visit_entries(|_key, _value| {
                    empty = false;
                    Ok::<_, InternalError>(IndexStoreVisit::Stop)
                })?;
                empty
            }
        })
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
    pub(in crate::db) fn exact_child_prefixes_for_parent_set<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        parent_component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        max_children: usize,
    ) -> Result<Option<Vec<Vec<Vec<u8>>>>, InternalError> {
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
        let previous = match &mut self.backend {
            IndexStoreBackend::Heap(map) => match map.entry(key) {
                Entry::Occupied(mut occupied) => {
                    let previous = occupied.insert(entry);
                    self.prefix_cardinality.apply_insert(
                        occupied.key(),
                        Some(&previous),
                        occupied.get(),
                    );
                    Some(previous)
                }
                Entry::Vacant(vacant) => {
                    self.prefix_cardinality
                        .apply_insert(vacant.key(), None, &entry);
                    vacant.insert(entry);
                    None
                }
            },
            IndexStoreBackend::Journaled {
                live,
                tombstones,
                prefix_cardinality_delta,
                ..
            } => {
                // Both projections are synchronous and infallible. Keep the
                // key owned until they have borrowed it, then publish the entry.
                self.prefix_cardinality
                    .apply_insert(&key, previous_journaled.as_ref(), &entry);
                prefix_cardinality_delta.apply_transition(
                    &key,
                    previous_journaled.as_ref(),
                    Some(&entry),
                );
                tombstones.remove(&key);
                live.insert(key, entry);
                previous_journaled
            }
        };
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
        self.prefix_cardinality.apply_insert(&key, None, &entry);
        self.apply_prefix_overlay_delta(&key, None, Some(&entry));
        match &mut self.backend {
            IndexStoreBackend::Heap(map) => {
                map.insert(key, entry);
            }
            IndexStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, entry);
            }
        }
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

    /// Reset journal-reconstructible entries without mutating the stable index.
    /// Live-source system entries belong to surviving heap rows, so they retain
    /// their existing overlay home across online recovery.
    pub(in crate::db) fn reset_journaled_live_projection(
        &mut self,
        data_generation: u64,
        fold_watermark: FoldWatermark,
        live_source_entities: &BTreeSet<EntityTag>,
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

        if live_source_entities.is_empty() {
            live.clear();
        } else {
            // Validate before removing anything: an undecodable key cannot be
            // safely classified as a disposable or a surviving live reference.
            for key in live.keys() {
                decode_index_key_frame(key).map_err(|_| InternalError::index_corruption())?;
            }
            live.retain(|key, _| {
                decode_index_key_frame(key).is_ok_and(|frame| {
                    frame.key_kind() == IndexKeyKind::System
                        && live_source_entities.contains(&frame.index_id().entity_tag())
                })
            });
        }
        // Live-source keys never enter the canonical tree. Their removals need
        // no retained tombstone; user cardinality ignores these system entries.
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

    /// Advance matching derived evidence, or leave it unavailable after the fold.
    /// A stale optional delta must not block authoritative journal convergence.
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
            match live.get(&key) {
                Some(value) => Some(value.clone()),
                None => canonical.get(&key),
            }
        };
        let next_value = value.clone();

        if let Some(value) = value {
            tombstones.remove(&key);
            live.insert(key.clone(), value.clone());
            self.prefix_cardinality
                .apply_insert(&key, previous.as_ref(), &value);
        } else {
            live.remove(&key);
            tombstones.insert(key.clone());
            self.prefix_cardinality
                .apply_remove(&key, previous.as_ref());
        }
        prefix_cardinality_delta.apply_transition(&key, previous.as_ref(), next_value.as_ref());
        positions.publish_preflighted(key, position);
        self.bump_generation();

        Ok(previous)
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
        positions.retire_overlays_preflighted(prepared.entries, live, tombstones);
    }

    /// Apply one exact index entry directly to canonical stable storage.
    ///
    /// Journal folding and planner-invisible migration staging share this
    /// primitive. Callers own key authority, journal watermarks and position
    /// retirement; unrelated live entries and their prefix deltas stay intact.
    pub(in crate::db) fn apply_canonical_entry(
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
        let (previous, key) = if let Some(value) = value.as_ref() {
            // The published stable insertion API consumes its key before
            // returning the previous entry; cardinality still needs this copy.
            let cardinality_key = key.clone();
            (canonical.insert(key, value.clone()), cardinality_key)
        } else {
            (canonical.remove(&key), key)
        };
        if visible {
            if let Some(value) = value.as_ref() {
                self.prefix_cardinality
                    .apply_insert(&key, previous.as_ref(), value);
            } else {
                self.prefix_cardinality
                    .apply_remove(&key, previous.as_ref());
            }
        } else {
            self.apply_prefix_overlay_delta(&key, value.as_ref(), previous.as_ref());
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

    /// Sum of bytes used by all stored index entries.
    pub fn memory_bytes(&self) -> Result<u64, InternalError> {
        let mut bytes = 0u64;
        self.visit_entries(|key, value| {
            bytes = bytes.saturating_add(key.as_bytes().len() as u64 + value.len() as u64);
            Ok::<_, InternalError>(IndexStoreVisit::Continue)
        })?;
        Ok(bytes)
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
        match live.get(key) {
            Some(value) => Some(value.clone()),
            None => canonical.get(key),
        }
    }

    // Share stable iteration and overlay merging across visitor closures. Error
    // types remain generic so store callers retain their original typed failures.
    pub(super) fn visit_journaled_entries_in_range<E: From<InternalError>>(
        &self,
        bounds: (Bound<&RawIndexStoreKey>, Bound<&RawIndexStoreKey>),
        direction: Direction,
        mut visit: &mut dyn FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, E>,
    ) -> Result<(), E> {
        type VisitEntry<'a> = OrderedOverlayEntry<
            MapEntry<RawIndexStoreKey, IndexEntryValue>,
            (&'a RawIndexStoreKey, &'a IndexEntryValue),
        >;

        let IndexStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &self.backend
        else {
            return Ok(());
        };

        let (lower, upper) = bounds;
        match direction {
            Direction::Asc if canonical.is_empty() => {
                for (key, value) in live.range((lower, upper)) {
                    if visit_index_store_entry(key, value, &mut visit)? {
                        return Ok(());
                    }
                }
                return Ok(());
            }
            Direction::Desc if canonical.is_empty() => {
                for (key, value) in live.range((lower, upper)).rev() {
                    if visit_index_store_entry(key, value, &mut visit)? {
                        return Ok(());
                    }
                }
                return Ok(());
            }
            Direction::Asc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)) {
                    if visit_index_store_entry(entry.key(), &entry.value(), &mut visit)? {
                        return Ok(());
                    }
                }
                return Ok(());
            }
            Direction::Desc if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((lower, upper)).rev() {
                    if visit_index_store_entry(entry.key(), &entry.value(), &mut visit)? {
                        return Ok(());
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        // Share visitor dispatch and stop/error propagation; iterator direction
        // stays statically selected before traversal.

        let mut visit_entry = |entry: VisitEntry<'_>| {
            let result = match entry {
                OrderedOverlayEntry::Canonical(entry) => {
                    visit_index_store_entry(entry.key(), &entry.value(), &mut visit)
                }
                OrderedOverlayEntry::Live((key, value)) => {
                    visit_index_store_entry(key, value, &mut visit)
                }
            };
            match result {
                Ok(visit) if !visit => ControlFlow::Continue(()),
                result => ControlFlow::Break(result.map(|_| ())),
            }
        };
        match direction {
            Direction::Asc => ordered_overlay_entries(
                canonical.range((lower, upper)),
                live.range((lower, upper)),
                direction,
                |entry| entry.key(),
                |entry| entry.0,
                tombstones,
            )
            .try_for_each(&mut visit_entry),
            Direction::Desc => ordered_overlay_entries(
                canonical.range((lower, upper)).rev(),
                live.range((lower, upper)).rev(),
                direction,
                |entry| entry.key(),
                |entry| entry.0,
                tombstones,
            )
            .try_for_each(&mut visit_entry),
        }
        .break_value()
        .unwrap_or(Ok(()))
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
use helper_tests::record_index_store_entry_read;
