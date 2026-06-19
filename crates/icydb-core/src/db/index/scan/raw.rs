//! Module: index::scan::raw
//! Responsibility: pure raw index range traversal over the index store.
//! Does not own: cursor continuation, executor metrics, predicate execution, or row decoding.
//! Boundary: executor and diagnostics wrap this traversal with their runtime policies.

use crate::{
    db::direction::Direction,
    db::index::{
        IndexEntryValue, envelope_is_empty,
        key::RawIndexStoreKey,
        store::{IndexStore, IndexStoreBackend},
    },
    error::InternalError,
};
use std::ops::Bound;

impl IndexStore {
    /// Visit raw index entries in one bounded range using canonical store order.
    ///
    /// The visitor returns `true` to stop traversal. This keeps the index layer
    /// independent of emitted-row limits, cursor anchors, predicate filters, and
    /// metric attribution while preserving the existing BTreeMap range order.
    pub(in crate::db) fn visit_raw_entries_in_range<F>(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        direction: Direction,
        mut visit: F,
    ) -> Result<(), InternalError>
    where
        F: FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, InternalError>,
    {
        if envelope_is_empty(bounds.0, bounds.1) {
            return Ok(());
        }

        #[cfg(any(test, feature = "diagnostics"))]
        Self::record_range_scan_call();

        match direction {
            Direction::Asc => match &self.backend {
                IndexStoreBackend::Heap(map) => {
                    for (key, value) in map.range((bounds.0.clone(), bounds.1.clone())) {
                        if visit(key, value)? {
                            return Ok(());
                        }
                    }
                }
                IndexStoreBackend::Journaled { .. } => {
                    self.visit_journaled_entries_in_range(bounds, direction, visit)?;
                }
            },
            Direction::Desc => match &self.backend {
                IndexStoreBackend::Heap(map) => {
                    for (key, value) in map.range((bounds.0.clone(), bounds.1.clone())).rev() {
                        if visit(key, value)? {
                            return Ok(());
                        }
                    }
                }
                IndexStoreBackend::Journaled { .. } => {
                    self.visit_journaled_entries_in_range(bounds, direction, visit)?;
                }
            },
        }

        Ok(())
    }
}
