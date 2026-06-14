//! Module: data::store
//! Responsibility: journaled-or-heap row storage behind the data-store boundary.
//! Does not own: key/row validation policy beyond type boundaries.
//! Boundary: commit/executor call into this layer after prevalidation.

use crate::{
    db::{
        data::{CanonicalRow, RawDataStoreKey, RawRow},
        direction::Direction,
        key_taxonomy::RawDataStoreKeyRange,
        ordered_overlay::{OrderedOverlayEntry, OrderedOverlayVisit, visit_ordered_overlay},
    },
    types::EntityTag,
};
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
#[cfg(feature = "diagnostics")]
use std::cell::Cell;
use std::collections::{BTreeMap as HeapBTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::{Bound, RangeBounds};

#[cfg(feature = "diagnostics")]
thread_local! {
    static DATA_STORE_GET_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(feature = "diagnostics")]
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
}

enum DataStoreBackend {
    Heap(HeapBTreeMap<RawDataStoreKey, RawRow>),
    Journaled {
        canonical: StableBTreeMap<RawDataStoreKey, RawRow, VirtualMemory<DefaultMemoryImpl>>,
        live: HeapBTreeMap<RawDataStoreKey, RawRow>,
        tombstones: BTreeSet<RawDataStoreKey>,
    },
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
        }
    }

    /// Initialize a journaled cached-stable data store.
    ///
    /// Normal writes update only the live projection. The canonical stable map
    /// is the future fold target and is not mutated by this wrapper's write
    /// methods.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: DataStoreBackend::Journaled {
                canonical: StableBTreeMap::init(memory),
                live: HeapBTreeMap::new(),
                tombstones: BTreeSet::new(),
            },
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
        match &mut self.backend {
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        }
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
        match &mut self.backend {
            DataStoreBackend::Heap(map) => map.insert(key, row),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, row);
                previous_journaled
            }
        }
    }

    /// Remove one row by raw key.
    pub(in crate::db) fn remove(&mut self, key: &RawDataStoreKey) -> Option<RawRow> {
        let previous_journaled = if matches!(self.backend, DataStoreBackend::Journaled { .. }) {
            self.get(key)
        } else {
            None
        };
        match &mut self.backend {
            DataStoreBackend::Heap(map) => map.remove(key),
            DataStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(key);
                tombstones.insert(key.clone());
                previous_journaled
            }
        }
    }

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable base.
    pub(in crate::db) fn reset_journaled_live_projection(
        &mut self,
    ) -> Result<(), crate::error::InternalError> {
        let DataStoreBackend::Journaled {
            live, tombstones, ..
        } = &mut self.backend
        else {
            return Err(crate::error::InternalError::store_invariant());
        };

        live.clear();
        tombstones.clear();

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
        live.insert(key, row);

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

        Ok(previous)
    }

    /// Apply one folded journal row put into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_put(
        &mut self,
        key: RawDataStoreKey,
        row: RawRow,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };

        Ok(canonical.insert(key, row))
    }

    /// Apply one folded journal row delete into the canonical stable base.
    pub(in crate::db) fn fold_recovered_journal_delete(
        &mut self,
        key: &RawDataStoreKey,
    ) -> Result<Option<RawRow>, crate::error::InternalError> {
        let DataStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(crate::error::InternalError::store_invariant());
        };

        Ok(canonical.remove(key))
    }

    /// Load one row by raw key.
    pub(in crate::db) fn get(&self, key: &RawDataStoreKey) -> Option<RawRow> {
        #[cfg(feature = "diagnostics")]
        record_data_store_get_call();

        match &self.backend {
            DataStoreBackend::Heap(map) => map.get(key).cloned(),
            DataStoreBackend::Journaled { .. } => Self::journaled_get_raw(&self.backend, key),
        }
    }

    /// Return whether one raw key exists without cloning the row payload.
    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        match &self.backend {
            DataStoreBackend::Heap(map) => map.contains_key(key),
            DataStoreBackend::Journaled { .. } => {
                Self::journaled_get_raw(&self.backend, key).is_some()
            }
        }
    }

    /// Clear all stored rows from the data store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        match &mut self.backend {
            DataStoreBackend::Heap(map) => map.clear(),
            DataStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                canonical.clear_new();
                live.clear();
                tombstones.clear();
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

    /// Return whether the data store currently contains no rows.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        match &self.backend {
            DataStoreBackend::Heap(map) => map.is_empty(),
            DataStoreBackend::Journaled { .. } => {
                let mut empty = true;
                let _: Result<(), Infallible> = self.visit_entries(|_key, _row| {
                    empty = false;
                    Ok(StoreVisit::Stop)
                });
                empty
            }
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
            DataStoreBackend::Journaled {
                canonical: _,
                live: _,
                tombstones: _,
            } => Self::visit_journaled_entries_in_bounds(
                &self.backend,
                (Bound::Unbounded, Bound::Unbounded),
                false,
                visitor,
            )?,
        }

        Ok(())
    }

    /// Visit raw row entries in reverse canonical storage order.
    pub(in crate::db) fn visit_entries_rev<E>(
        &self,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.iter().rev() {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled {
                canonical: _,
                live: _,
                tombstones: _,
            } => Self::visit_journaled_entries_in_bounds(
                &self.backend,
                (Bound::Unbounded, Bound::Unbounded),
                true,
                visitor,
            )?,
        }

        Ok(())
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
            DataStoreBackend::Journaled {
                canonical: _,
                live: _,
                tombstones: _,
            } => Self::visit_journaled_entries_in_bounds(&self.backend, bounds, false, visitor)?,
        }

        Ok(())
    }

    /// Visit raw row entries in reverse order whose keys belong to the provided storage range.
    pub(in crate::db) fn visit_range_rev<E>(
        &self,
        key_range: impl RangeBounds<RawDataStoreKey>,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let bounds = Self::owned_range_bounds(&key_range);
        match &self.backend {
            DataStoreBackend::Heap(map) => {
                for (key, row) in map.range((bounds.0.clone(), bounds.1)).rev() {
                    if visitor(key, row)?.should_stop() {
                        break;
                    }
                }
            }
            DataStoreBackend::Journaled {
                canonical: _,
                live: _,
                tombstones: _,
            } => Self::visit_journaled_entries_in_bounds(&self.backend, bounds, true, visitor)?,
        }

        Ok(())
    }

    /// Visit raw row entries for one entity using compact prefix bounds.
    pub(in crate::db) fn visit_entity<E>(
        &self,
        entity: EntityTag,
        visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        self.visit_range(RawDataStoreKey::store_range_bounds(&range), visitor)
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

    /// Return the monotonic perf-only count of stable row fetches seen by this process.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn current_get_call_count() -> u64 {
        DATA_STORE_GET_CALL_COUNT.with(Cell::get)
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            DataStoreBackend::Journaled { canonical: map, .. } => map.len(),
            DataStoreBackend::Heap(_) => 0,
        }
    }

    fn journaled_get_raw(backend: &DataStoreBackend, key: &RawDataStoreKey) -> Option<RawRow> {
        let DataStoreBackend::Journaled {
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

    fn visit_journaled_entries_in_bounds<E>(
        backend: &DataStoreBackend,
        bounds: (Bound<RawDataStoreKey>, Bound<RawDataStoreKey>),
        reverse: bool,
        mut visitor: impl FnMut(&RawDataStoreKey, &RawRow) -> Result<StoreVisit, E>,
    ) -> Result<(), E> {
        let DataStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = backend
        else {
            return Ok(());
        };

        if canonical.is_empty() {
            if reverse {
                for (key, row) in live.range(bounds).rev() {
                    if visitor(key, row)?.should_stop() {
                        return Ok(());
                    }
                }
            } else {
                for (key, row) in live.range(bounds) {
                    if visitor(key, row)?.should_stop() {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }

        if live.is_empty() && tombstones.is_empty() {
            if reverse {
                for entry in canonical.range(bounds).rev() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        return Ok(());
                    }
                }
            } else {
                for entry in canonical.range(bounds) {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }

        match if reverse {
            Direction::Desc
        } else {
            Direction::Asc
        } {
            Direction::Asc => visit_ordered_overlay(
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
            ),
            Direction::Desc => visit_ordered_overlay(
                canonical.range((bounds.0.clone(), bounds.1.clone())).rev(),
                live.range((bounds.0, bounds.1)).rev(),
                Direction::Desc,
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
            ),
        }
    }
}

#[cfg(test)]
mod tests;
