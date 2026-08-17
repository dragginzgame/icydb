//! Module: data::store
//! Responsibility: journaled-or-heap row storage behind the data-store boundary.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::{
    db::{
        data::{CanonicalRow, RawDataStoreKey, RawRow},
        direction::Direction,
        ordered_overlay::{OrderedOverlayEntry, OrderedOverlayVisit, visit_ordered_overlay},
        positioned_overlay::{
            JournalOverlayPosition, PositionedOverlayMetadata, PositionedOverlayRetirement,
        },
    },
    types::EntityTag,
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::{Bound, RangeBounds};

#[cfg(all(feature = "sql", feature = "diagnostics"))]
thread_local! {
    static DATA_STORE_GET_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn record_data_store_get_call() {
    DATA_STORE_GET_CALL_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

///
/// DataStore
///
/// Thin persistence wrapper over one journaled or heap BTreeMap.
///
/// Invariant: callers provide already-validated `RawDataStoreKey` and canonical row bytes.
/// This type intentionally does not enforce commit-phase ordering.
///

pub struct DataStore {
    backend: DataStoreBackend,
    generation: u64,
    entity_cardinality: EntityCardinality,
}

enum DataStoreBackend {
    Heap(HeapBTreeMap<RawDataStoreKey, RawRow>),
    Journaled {
        canonical: StableBTreeMap<RawDataStoreKey, RawRow, VirtualMemory<DefaultMemoryImpl>>,
        live: HeapBTreeMap<RawDataStoreKey, RawRow>,
        tombstones: BTreeSet<RawDataStoreKey>,
        positions: PositionedOverlayMetadata<RawDataStoreKey>,
        entity_cardinality_delta: EntityCardinalityDelta,
    },
}

/// Preflighted provenance publication for direct journal record families.
#[cfg(any(test, feature = "migration"))]
pub(in crate::db) struct PreparedDataPositionPublication {
    keys: Vec<RawDataStoreKey>,
    position: JournalOverlayPosition,
}

/// Preflighted exact retirement for one complete journal batch.
pub(in crate::db) struct PreparedDataPositionRetirement {
    entries: Vec<(RawDataStoreKey, PositionedOverlayRetirement)>,
}

/// One visible row read that borrows heap/live state and owns stable state.
///
/// Callers that only need selected fields can evaluate a borrowed row while
/// the store handle is active. Stable-structure reads remain owned because
/// that backend cannot expose a value reference beyond its storage call.
pub(in crate::db) enum StoredRowRead<'a> {
    Missing,
    Borrowed(&'a RawRow),
    Owned(RawRow),
}

impl StoredRowRead<'_> {
    /// Borrow the visible row regardless of its physical backing.
    #[must_use]
    pub(in crate::db) const fn as_row(&self) -> Option<&RawRow> {
        match self {
            Self::Missing => None,
            Self::Borrowed(row) => Some(row),
            Self::Owned(row) => Some(row),
        }
    }

    /// Convert the visible row into the existing owned get contract.
    #[must_use]
    fn into_owned(self) -> Option<RawRow> {
        match self {
            Self::Missing => None,
            Self::Borrowed(row) => Some(row.clone()),
            Self::Owned(row) => Some(row),
        }
    }
}

/// Control-flow result for store traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum StoreVisit {
    Continue,
    Stop,
}

impl StoreVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl DataStore {
    /// Initialize a volatile heap-backed data store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: DataStoreBackend::Heap(HeapBTreeMap::new()),
            generation: 0,
            entity_cardinality: EntityCardinality::empty(),
        }
    }

    /// Initialize a journaled cached-stable data store.
    ///
    /// Normal writes update only the live projection. The canonical stable map
    /// is the future fold target and is not mutated by this wrapper's write
    /// methods.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let canonical = StableBTreeMap::init(memory);
        let entity_cardinality = if canonical.is_empty() {
            EntityCardinality::empty()
        } else {
            EntityCardinality::unavailable()
        };
        Self {
            backend: DataStoreBackend::Journaled {
                canonical,
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
                positions: PositionedOverlayMetadata::new(),
                entity_cardinality_delta: EntityCardinalityDelta::empty(),
            },
            generation: 0,
            // Stable rows remain authoritative after reinitialization. Exact
            // zero cardinality is still known for an empty canonical map;
            // populated maps remain unavailable without a startup scan.
            entity_cardinality,
        }
    }

    /// Insert or replace one row by raw key.
    pub(in crate::db) fn insert(
        &mut self,
        key: RawDataStoreKey,
        row: CanonicalRow,
    ) -> Option<RawRow> {
        let row = row.into_raw_row();
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(&key)
        } else {
            None
        };
        let cardinality_key = key.clone();
        let previous = match &mut self.backend {
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        };
        self.entity_cardinality
            .apply_insert(&cardinality_key, previous.as_ref());
        self.apply_entity_overlay_delta(&cardinality_key, previous.is_some(), true);
        self.bump_generation();
        previous
    }

    /// Insert one raw row directly for corruption-focused test setup only.
    #[cfg(test)]
    pub(in crate::db) fn insert_raw_for_test(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Option<RawRow> {
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(&key)
        } else {
            None
        };
        let cardinality_key = key.clone();
        let previous = match &mut self.backend {
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        };
        self.entity_cardinality
            .apply_insert(&cardinality_key, previous.as_ref());
        self.apply_entity_overlay_delta(&cardinality_key, previous.is_some(), true);
        self.bump_generation();
        previous
    }

    /// Remove one row by raw key.
    pub(in crate::db) fn remove(&mut self, key: &RawDataStoreKey) -> Option<RawRow> {
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(key)
        } else {
            None
        };
        let previous = match &mut self.backend {
            DataStoreBackend::Heap(map) => map.remove(key),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(key);
                tombstones.insert(key.clone());
                previous_journaled
            }
        };
        self.entity_cardinality.apply_remove(key, previous.as_ref());
        self.apply_entity_overlay_delta(key, previous.is_some(), false);
        self.bump_generation();
        previous
    }

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable base.
    pub(in crate::db) fn reset_journaled_live_projection(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
            entity_cardinality_delta,
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        live.clear();
        tombstones.clear();
        positions.clear();
        *entity_cardinality_delta = EntityCardinalityDelta::empty();
        self.entity_cardinality = if canonical.is_empty() {
            EntityCardinality::empty()
        } else {
            EntityCardinality::unavailable()
        };
        self.bump_generation();

        Ok(())
    }

    /// Apply one recovered journal row put into the volatile projection.
    pub(in crate::db) fn apply_recovered_journal_put(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        let previous = if tombstones.contains(&key) {
            None
        } else {
            live.get(&key).cloned().or_else(|| canonical.get(&key))
        };
        tombstones.remove(&key);
        let cardinality_key = key.clone();
        live.insert(key, row);
        self.entity_cardinality
            .apply_insert(&cardinality_key, previous.as_ref());
        self.apply_entity_overlay_delta(&cardinality_key, previous.is_some(), true);
        self.bump_generation();

        Ok(previous)
    }

    /// Apply one recovered journal row delete into the volatile projection.
    pub(in crate::db) fn apply_recovered_journal_delete(
        &mut self,
        key: &RawDataStoreKey,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        let previous = if tombstones.contains(key) {
            None
        } else {
            live.get(key).cloned().or_else(|| canonical.get(key))
        };
        live.remove(key);
        tombstones.insert(key.clone());
        self.entity_cardinality.apply_remove(key, previous.as_ref());
        self.apply_entity_overlay_delta(key, previous.is_some(), false);
        self.bump_generation();

        Ok(previous)
    }

    /// Publish one preflighted positioned row value or tombstone.
    pub(in crate::db) fn publish_preflighted_journal_entry(
        &mut self,
        key: RawDataStoreKey,
        row: Option<RawRow>,
        position: JournalOverlayPosition,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
            entity_cardinality_delta,
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
        let next_present = row.is_some();
        if let Some(row) = row {
            tombstones.remove(&key);
            live.insert(key.clone(), row);
            self.entity_cardinality
                .apply_insert(&cardinality_key, previous.as_ref());
        } else {
            live.remove(&key);
            tombstones.insert(key.clone());
            self.entity_cardinality
                .apply_remove(&cardinality_key, previous.as_ref());
        }
        entity_cardinality_delta.apply_presence_transition(
            &cardinality_key,
            previous.is_some(),
            next_present,
        );
        positions.publish_preflighted(key, position);
        self.bump_generation();

        Ok(previous)
    }

    /// Validate and publish one positioned row for direct store tests.
    #[cfg(test)]
    pub(in crate::db) fn publish_positioned_journal_entry(
        &mut self,
        key: RawDataStoreKey,
        row: Option<RawRow>,
        position: JournalOverlayPosition,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        self.preflight_positioned_journal_entry(&key, position)?;
        self.publish_preflighted_journal_entry(key, row, position)
    }

    /// Preflight row provenance before marker publication.
    pub(in crate::db) fn preflight_positioned_journal_entry(
        &self,
        key: &RawDataStoreKey,
        position: JournalOverlayPosition,
    ) -> Result<(), crate::error::InternalError> {
        let DataStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        positions.preflight_publish(key, position)
    }

    /// Preflight direct row provenance before marker publication.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn prepare_position_publication(
        &self,
        keys: impl IntoIterator<Item = RawDataStoreKey>,
        position: JournalOverlayPosition,
    ) -> Result<PreparedDataPositionPublication, crate::error::InternalError> {
        let DataStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let keys = keys.into_iter().collect::<BTreeSet<_>>();
        for key in &keys {
            positions.preflight_publish(key, position)?;
        }
        Ok(PreparedDataPositionPublication {
            keys: keys.into_iter().collect(),
            position,
        })
    }

    /// Publish direct row provenance after its values have been applied.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db) fn publish_prepared_positions(
        &mut self,
        prepared: PreparedDataPositionPublication,
    ) {
        let DataStoreBackend::Journaled { positions, .. } = &mut self.backend else {
            debug_assert!(false, "preflighted row positions require a journaled store");
            return;
        };
        for key in prepared.keys {
            positions.publish_preflighted(key, prepared.position);
        }
    }

    /// Preflight exact row-overlay retirement before canonical mutation.
    pub(in crate::db) fn prepare_position_retirement(
        &self,
        keys: impl IntoIterator<Item = RawDataStoreKey>,
        position: JournalOverlayPosition,
    ) -> Result<PreparedDataPositionRetirement, crate::error::InternalError> {
        let DataStoreBackend::Journaled { positions, .. } = &self.backend else {
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
        Ok(PreparedDataPositionRetirement { entries })
    }

    /// Retire only exact row overlays after canonical mutation succeeds.
    pub(in crate::db) fn apply_prepared_position_retirement(
        &mut self,
        prepared: PreparedDataPositionRetirement,
    ) {
        let DataStoreBackend::Journaled {
            live,
            tombstones,
            positions,
            ..
        } = &mut self.backend
        else {
            debug_assert!(
                false,
                "preflighted row retirement requires a journaled store"
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
        key: &RawDataStoreKey,
        position: JournalOverlayPosition,
    ) -> Result<PositionedOverlayRetirement, crate::error::InternalError> {
        let DataStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };
        let retirement = positions.preflight_retirement(key, position)?;
        let prepared = PreparedDataPositionRetirement {
            entries: vec![(key.clone(), retirement)],
        };
        self.apply_prepared_position_retirement(prepared);
        Ok(retirement)
    }

    /// Apply one folded journal row put into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_put(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
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
        let previous = canonical.insert(key, row);
        if visible {
            self.entity_cardinality
                .apply_insert(&cardinality_key, previous.as_ref());
        } else if previous.is_none() {
            self.apply_entity_overlay_delta(&cardinality_key, true, false);
        }
        self.bump_generation();

        Ok(previous)
    }

    /// Apply one folded journal row delete into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_delete(
        &mut self,
        key: &RawDataStoreKey,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        let visible = !live.contains_key(key) && !tombstones.contains(key);
        let previous = canonical.remove(key);
        if visible {
            self.entity_cardinality.apply_remove(key, previous.as_ref());
        } else if previous.is_some() {
            self.apply_entity_overlay_delta(key, false, true);
        }
        self.bump_generation();

        Ok(previous)
    }

    /// Prove that recovered journal rows can be folded into canonical storage.
    pub(in crate::db) fn preflight_fold_recovered_journal(
        &self,
    ) -> Result<(), crate::error::InternalError> {
        match self.backend {
            DataStoreBackend::Journaled { .. } => Ok(()),
            DataStoreBackend::Heap(_) => Err(crate::error::InternalError::store_invariant()),
        }
    }

    /// Load one row by raw key.
    pub(in crate::db) fn get(&self, key: &RawDataStoreKey) -> Option<RawRow> {
        self.read(key).into_owned()
    }

    /// Load one row from the canonical predecessor view.
    ///
    /// Online journal folding uses this view so a newer positioned live effect
    /// cannot replace the predecessor evidence for the older batch being
    /// canonicalized.
    pub(in crate::db) fn get_canonical(&self, key: &RawDataStoreKey) -> Option<RawRow> {
        match &self.backend {
            DataStoreBackend::Heap(map) => map.get(key).cloned(),
            DataStoreBackend::Journaled { canonical, .. } => canonical.get(key),
        }
    }

    /// Read one visible row without cloning heap/live payloads.
    pub(in crate::db) fn read<'a>(&'a self, key: &RawDataStoreKey) -> StoredRowRead<'a> {
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        record_data_store_get_call();

        match &self.backend {
            DataStoreBackend::Heap(map) => map
                .get(key)
                .map_or(StoredRowRead::Missing, StoredRowRead::Borrowed),
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } => {
                if tombstones.contains(key) {
                    StoredRowRead::Missing
                } else if let Some(row) = live.get(key) {
                    StoredRowRead::Borrowed(row)
                } else {
                    canonical
                        .get(key)
                        .map_or(StoredRowRead::Missing, StoredRowRead::Owned)
                }
            }
        }
    }

    /// Return whether one raw key exists without cloning the row payload.
    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        match &self.backend {
            DataStoreBackend::Heap(map) => map.contains_key(key),
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } => {
                !tombstones.contains(key)
                    && (live.contains_key(key) || canonical.get(key).is_some())
            }
        }
    }

    /// Return the number of stored rows without exposing the backing map.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        match &self.backend {
            DataStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            DataStoreBackend::Journaled { .. } => {
                let mut count = 0_u64;
                let _: Result<(), Infallible> = self.visit_entries(|_key, _row| {
                    count = count.saturating_add(1);
                    Ok(StoreVisit::Continue)
                });
                count
            }
        }
    }

    /// Return the row-store generation used to prove index metadata freshness.
    #[must_use]
    pub(in crate::db) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Return an exact current row count for one entity when store metadata is valid.
    #[must_use]
    pub(in crate::db) fn exact_entity_count(&self, entity: EntityTag) -> Option<u64> {
        self.entity_cardinality.exact_count(entity)
    }

    /// Return the exact live-overlay delta from the canonical base for one entity.
    #[must_use]
    pub(in crate::db) fn exact_entity_cardinality_delta(&self, entity: EntityTag) -> Option<i64> {
        match &self.backend {
            DataStoreBackend::Heap(_) => Some(0),
            DataStoreBackend::Journaled {
                entity_cardinality_delta,
                ..
            } => entity_cardinality_delta.exact_delta(entity),
        }
    }

    /// Visit raw row entries in canonical storage order.
    pub(in crate::db) fn visit_entries<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => Self::visit_journaled_entries_in_bounds(
                &self.backend,
                (Bound::Unbounded, Bound::Unbounded),
                visitor,
            )?,
        }

        Ok(())
    }

    /// Visit canonical predecessor rows after one exclusive physical checkpoint.
    ///
    /// Optional derived-evidence builders use this view so newer live overlays
    /// cannot contaminate a generation bound to the canonical fold watermark.
    pub(in crate::db) fn visit_canonical_entries_after(
        &self,
        checkpoint: Option<&RawDataStoreKey>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<bool, crate::error::InternalError>,
    ) -> Result<(), crate::error::InternalError> {
        let lower = checkpoint
            .cloned()
            .map_or(Bound::Unbounded, Bound::Excluded);
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range((lower, Bound::Unbounded)) {
                    if visitor(key, row)? {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { canonical, .. } => {
                for entry in canonical.range((lower, Bound::Unbounded)) {
                    if visitor(entry.key(), &entry.value())? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Return whether the canonical predecessor domain is physically empty.
    ///
    /// This bounded root observation never walks live overlays or materializes
    /// row payloads.
    pub(in crate::db) fn canonical_is_empty(&self) -> Result<bool, crate::error::InternalError> {
        match &self.backend {
            DataStoreBackend::Journaled { canonical, .. } => Ok(canonical.is_empty()),
            DataStoreBackend::Heap(_) => Err(crate::error::InternalError::store_invariant()),
        }
    }

    /// Visit raw row entries whose keys belong to the provided storage range.
    pub(in crate::db) fn visit_range<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let bounds = Self::owned_range_bounds(&key_range);
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range((bounds.0.clone(), bounds.1)) {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                Self::visit_journaled_entries_in_bounds(&self.backend, bounds, visitor)?;
            }
        }

        Ok(())
    }

    /// Visit one ascending row range while allowing the caller to stop after
    /// seeing the key but before a stable row payload is materialized.
    ///
    /// Mixed journal overlays retain the ordinary key-then-point-read path;
    /// one physical backing can keep the range iterator open for the complete
    /// scan and avoid a second tree lookup per row.
    pub(in crate::db) fn try_visit_range_with_row_preflight<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        mut preflight: impl FnMut(&RawDataStoreKey) -> Result<StoreVisit, E>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<Option<bool>, E> {
        let bounds = Self::owned_range_bounds(&key_range);
        let mut stopped = false;
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range((bounds.0.clone(), bounds.1)) {
                    if preflight(key)?.should_stop() {
                        stopped = true;
                        break;
                    }
                    if visitor(key, row)?.should_stop() {
                        stopped = true;
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } if canonical.is_empty() => {
                for (key, row) in live.range((bounds.0.clone(), bounds.1)) {
                    if tombstones.contains(key) {
                        continue;
                    }
                    if preflight(key)?.should_stop() {
                        stopped = true;
                        break;
                    }
                    if visitor(key, row)?.should_stop() {
                        stopped = true;
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } if live.is_empty() && tombstones.is_empty() => {
                for entry in canonical.range((bounds.0.clone(), bounds.1)) {
                    if preflight(entry.key())?.should_stop() {
                        stopped = true;
                        break;
                    }
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        stopped = true;
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => return Ok(None),
        }

        Ok(Some(!stopped))
    }

    /// Visit only raw keys in storage order without fetching row payloads.
    ///
    /// Primary-key access streams use this boundary to discover candidate
    /// identities before the terminal row runtime decides whether the payload
    /// is needed. Journaled traversal merges canonical and live keys while
    /// preserving live overrides and tombstones without reading stable values.
    pub(in crate::db) fn visit_key_range<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        visitor: impl FnMut(&RawDataStoreKey) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        self.visit_keys_in_bounds(Self::owned_range_bounds(&key_range), false, visitor)
    }

    /// Visit only raw keys in reverse storage order without fetching row payloads.
    pub(in crate::db) fn visit_key_range_rev<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        visitor: impl FnMut(&RawDataStoreKey) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        self.visit_keys_in_bounds(Self::owned_range_bounds(&key_range), true, visitor)
    }

    /// Sum of bytes used by all stored rows.
    pub(in crate::db) fn memory_bytes(&self) -> u64 {
        // Report map footprint as key bytes + row bytes per entry.
        let mut bytes = 0u64;
        let _: Result<(), Infallible> = self.visit_entries(|key, row| {
            bytes = bytes.saturating_add(key.as_bytes().len() as u64 + row.len() as u64);
            Ok(StoreVisit::Continue)
        });
        bytes
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    #[cfg(test)]
    fn rebuild_entity_cardinality_from_entries(&mut self) {
        let mut cardinality = EntityCardinality::empty();
        let _: Result<(), Infallible> = self.visit_entries(|key, _row| {
            cardinality.apply_present_key(key);
            Ok(StoreVisit::Continue)
        });
        self.entity_cardinality = cardinality;
    }

    fn apply_entity_overlay_delta(
        &mut self,
        key: &RawDataStoreKey,
        previous_present: bool,
        next_present: bool,
    ) {
        let DataStoreBackend::Journaled {
            entity_cardinality_delta,
            ..
        } = &mut self.backend
        else {
            return;
        };
        entity_cardinality_delta.apply_presence_transition(key, previous_present, next_present);
    }

    /// Return the monotonic perf-only count of stable row fetches seen by this process.
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    pub(in crate::db) fn current_get_call_count() -> u64 {
        DATA_STORE_GET_CALL_COUNT.with(Cell::get)
    }

    fn owned_range_bounds(
        key_range: &impl RangeBounds<RawDataStoreKey>,
    ) -> (Bound<RawDataStoreKey>, Bound<RawDataStoreKey>) {
        let lower = match key_range.start_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match key_range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        (lower, upper)
    }

    fn visit_keys_in_bounds<E>(
        &self,
        bounds: (Bound<RawDataStoreKey>, Bound<RawDataStoreKey>),
        reverse: bool,
        mut visitor: impl FnMut(&RawDataStoreKey) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                if reverse {
                    for (key, _row) in map.range(bounds).rev() {
                        if visitor(key)?.should_stop() {
                            break;
                        }
                    }
                } else {
                    for (key, _row) in map.range(bounds) {
                        if visitor(key)?.should_stop() {
                            break;
                        }
                    }
                }
            }
            DataStoreBackend::Journaled { .. } => {
                Self::visit_journaled_keys_in_bounds(&self.backend, bounds, reverse, visitor)?;
            }
        }

        Ok(())
    }

    fn visit_journaled_keys_in_bounds<E>(
        backend: &DataStoreBackend,
        bounds: (Bound<RawDataStoreKey>, Bound<RawDataStoreKey>),
        reverse: bool,
        mut visitor: impl FnMut(&RawDataStoreKey) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = backend
        else {
            return Ok(());
        };

        if canonical.is_empty() {
            if reverse {
                for (key, _row) in live.range(bounds).rev() {
                    if visitor(key)?.should_stop() {
                        return Ok(());
                    }
                }
            } else {
                for (key, _row) in live.range(bounds) {
                    if visitor(key)?.should_stop() {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }

        if live.is_empty() && tombstones.is_empty() {
            if reverse {
                for entry in canonical.range(bounds).rev() {
                    if visitor(entry.key())?.should_stop() {
                        return Ok(());
                    }
                }
            } else {
                for entry in canonical.range(bounds) {
                    if visitor(entry.key())?.should_stop() {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }

        let direction = if reverse {
            Direction::Desc
        } else {
            Direction::Asc
        };
        match direction {
            Direction::Asc => visit_ordered_overlay(
                canonical.range((bounds.0.clone(), bounds.1.clone())),
                live.range((bounds.0, bounds.1)),
                direction,
                |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.key()),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key())?
                        }
                        OrderedOverlayEntry::Live((key, _row)) => visitor(key)?,
                    };
                    Ok(if visit.should_stop() {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
            Direction::Desc => visit_ordered_overlay(
                canonical.range((bounds.0.clone(), bounds.1.clone())).rev(),
                live.range((bounds.0, bounds.1)).rev(),
                direction,
                |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.key()),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key())?
                        }
                        OrderedOverlayEntry::Live((key, _row)) => visitor(key)?,
                    };
                    Ok(if visit.should_stop() {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
        }
    }

    fn visit_journaled_entries_in_bounds<E>(
        backend: &DataStoreBackend,
        bounds: (Bound<RawDataStoreKey>, Bound<RawDataStoreKey>),
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = backend
        else {
            return Ok(());
        };

        if canonical.is_empty() {
            for (key, row) in live.range(bounds) {
                if visitor(key, row)?.should_stop() {
                    return Ok(());
                }
            }
            return Ok(());
        }

        if live.is_empty() && tombstones.is_empty() {
            for entry in canonical.range(bounds) {
                if visitor(entry.key(), &entry.value())?.should_stop() {
                    return Ok(());
                }
            }
            return Ok(());
        }

        visit_ordered_overlay(
            canonical.range((bounds.0.clone(), bounds.1.clone())),
            live.range((bounds.0, bounds.1)),
            Direction::Asc,
            |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
            |canonical_entry| !tombstones.contains(canonical_entry.key()),
            |live_entry| !tombstones.contains(live_entry.0),
            |entry| {
                let visit = match entry {
                    OrderedOverlayEntry::Canonical(canonical_entry) => {
                        visitor(canonical_entry.key(), &canonical_entry.value())?
                    }
                    OrderedOverlayEntry::Live((key, row)) => visitor(key, row)?,
                };
                Ok(if visit.should_stop() {
                    OrderedOverlayVisit::Stop
                } else {
                    OrderedOverlayVisit::Continue
                })
            },
        )
    }
}

#[derive(Clone, Debug)]
struct EntityCardinality {
    counts: HeapBTreeMap<EntityTag, u64>,
    decodable: bool,
}

#[derive(Clone, Debug)]
struct EntityCardinalityDelta {
    counts: HeapBTreeMap<EntityTag, i64>,
    decodable: bool,
}

impl EntityCardinalityDelta {
    const fn empty() -> Self {
        Self {
            counts: HeapBTreeMap::new(),
            decodable: true,
        }
    }

    fn exact_delta(&self, entity: EntityTag) -> Option<i64> {
        self.decodable
            .then(|| self.counts.get(&entity).copied().unwrap_or(0))
    }

    fn apply_presence_transition(
        &mut self,
        key: &RawDataStoreKey,
        previous_present: bool,
        next_present: bool,
    ) {
        if !self.decodable || previous_present == next_present {
            return;
        }
        let Some(entity) = key.entity_tag_prefix() else {
            self.counts.clear();
            self.decodable = false;
            return;
        };
        let delta = if next_present { 1_i64 } else { -1_i64 };
        let count = self.counts.entry(entity).or_insert(0);
        let Some(next) = count.checked_add(delta) else {
            self.counts.clear();
            self.decodable = false;
            return;
        };
        *count = next;
        if *count == 0 {
            self.counts.remove(&entity);
        }
    }
}

impl EntityCardinality {
    const fn empty() -> Self {
        Self {
            counts: HeapBTreeMap::new(),
            decodable: true,
        }
    }

    const fn unavailable() -> Self {
        Self {
            counts: HeapBTreeMap::new(),
            decodable: false,
        }
    }

    fn exact_count(&self, entity: EntityTag) -> Option<u64> {
        self.decodable
            .then(|| self.counts.get(&entity).copied().unwrap_or(0))
    }

    fn apply_insert(&mut self, key: &RawDataStoreKey, previous: Option<&RawRow>) {
        if previous.is_some() {
            return;
        }
        self.apply_present_key(key);
    }

    fn apply_remove(&mut self, key: &RawDataStoreKey, previous: Option<&RawRow>) {
        if previous.is_none() {
            return;
        }
        self.apply_removed_key(key);
    }

    fn apply_present_key(&mut self, key: &RawDataStoreKey) {
        if !self.decodable {
            return;
        }
        let Some(entity) = key.entity_tag_prefix() else {
            self.invalidate();
            return;
        };

        let count = self.counts.entry(entity).or_insert(0);
        *count = count.saturating_add(1);
    }

    fn apply_removed_key(&mut self, key: &RawDataStoreKey) {
        if !self.decodable {
            return;
        }
        let Some(entity) = key.entity_tag_prefix() else {
            self.invalidate();
            return;
        };

        if let Some(count) = self.counts.get_mut(&entity) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.counts.remove(&entity);
            }
        }
    }

    fn invalidate(&mut self) {
        self.counts.clear();
        self.decodable = false;
    }
}

#[cfg(test)]
mod tests;
