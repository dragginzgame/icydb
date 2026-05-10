//! Module: index::plan::read
//! Responsibility: abstract read view needed by forward-index delta derivation.
//! Does not own: store lookup, executor context wiring, or commit materialization.
//! Boundary: commit/executor adapters provide this view before invoking index planning.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey},
        index::{IndexReadContract, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    types::EntityTag,
};
use std::ops::Bound;

///
/// IndexPlanReadView
///
/// Abstract preflight reads needed while deriving forward-index deltas. This
/// port intentionally accepts reduced index contract facts instead of generated
/// index definitions so index planning remains independent of registry,
/// executor, and commit state.
///

pub(in crate::db) trait IndexPlanReadView {
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError>;

    /// Return the raw entry for one index key, or `None` when no entry exists.
    fn read_index_entry(
        &self,
        index: IndexReadContract<'_>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from one raw
    /// index-key range.
    fn read_index_keys_in_raw_range(
        &self,
        entity_path: &'static str,
        entity_tag: EntityTag,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError>;
}
