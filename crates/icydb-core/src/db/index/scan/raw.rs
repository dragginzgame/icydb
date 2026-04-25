//! Module: index::scan::raw
//! Responsibility: pure raw index range traversal over the index store.
//! Does not own: cursor continuation, executor metrics, predicate execution, or row decoding.
//! Boundary: executor and diagnostics wrap this traversal with their runtime policies.

use crate::{
    db::direction::Direction,
    db::index::{entry::RawIndexEntry, envelope_is_empty, key::RawIndexKey, store::IndexStore},
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
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        direction: Direction,
        mut visit: F,
    ) -> Result<(), InternalError>
    where
        F: FnMut(&RawIndexKey, &RawIndexEntry) -> Result<bool, InternalError>,
    {
        if envelope_is_empty(bounds.0, bounds.1) {
            return Ok(());
        }

        match direction {
            Direction::Asc => {
                for entry in self.map.range((bounds.0.clone(), bounds.1.clone())) {
                    if visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((bounds.0.clone(), bounds.1.clone())).rev() {
                    if visit(entry.key(), &entry.value())? {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}
