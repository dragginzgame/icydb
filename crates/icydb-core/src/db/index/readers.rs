//! Module: index::readers
//! Responsibility: narrow read ports used by commit and executor preflight flows.
//! Does not own: index delta derivation, commit-op materialization, or executor state.
//! Boundary: callers implement these ports; index-adjacent planners consume them abstractly.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey},
        index::{IndexStore, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

///
/// SealedPrimaryRowReader
///
/// Internal marker used to seal `PrimaryRowReader` implementations.
///

pub(in crate::db) trait SealedPrimaryRowReader<E: EntityKind + EntityValue> {}

///
/// SealedStructuralPrimaryRowReader
///
/// Internal marker used to seal nongeneric structural primary-row readers.
///

pub(in crate::db) trait SealedStructuralPrimaryRowReader {}

///
/// SealedIndexEntryReader
///
/// Internal marker used to seal `IndexEntryReader` implementations.
///

pub(in crate::db) trait SealedIndexEntryReader<E: EntityKind + EntityValue> {}

///
/// SealedStructuralIndexEntryReader
///
/// Internal marker used to seal nongeneric structural index-entry readers.
///

pub(in crate::db) trait SealedStructuralIndexEntryReader {}

///
/// PrimaryRowReader
///
/// Preflight port used for reading authoritative primary rows without
/// depending on executor context internals.
///

pub(in crate::db) trait PrimaryRowReader<E: EntityKind + EntityValue>:
    SealedPrimaryRowReader<E>
{
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError>;
}

///
/// StructuralPrimaryRowReader
///
/// Narrow nongeneric read port used by structural commit helpers that only
/// need authoritative primary-row lookup.
///

pub(in crate::db) trait StructuralPrimaryRowReader:
    SealedStructuralPrimaryRowReader
{
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError>;
}

///
/// IndexEntryReader
///
/// Preflight port used for reading authoritative index entries without
/// requiring commit preflight to mutate real stores.
///

pub(in crate::db) trait IndexEntryReader<E: EntityKind + EntityValue>:
    SealedIndexEntryReader<E>
{
    /// Return the index entry for `(index_store, key)`, or `None` when no entry exists.
    fn read_index_entry(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from
    /// `index_store` in raw key range.
    fn read_index_keys_in_raw_range(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError>;
}

///
/// StructuralIndexEntryReader
///
/// Narrow nongeneric read port used by structural relation/commit helpers that
/// only need authoritative index-entry lookup.
///

pub(in crate::db) trait StructuralIndexEntryReader:
    SealedStructuralIndexEntryReader
{
    /// Return the index entry for `(index_store, key)`, or `None` when no entry exists.
    fn read_index_entry_structural(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from
    /// `index_store` in raw key range.
    fn read_index_keys_in_raw_range_structural(
        &self,
        entity_path: &'static str,
        entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError>;
}

impl<E> StructuralIndexEntryReader for dyn IndexEntryReader<E> + '_
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry_structural(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        self.read_index_entry(index_store, key)
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        _entity_path: &'static str,
        _entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        self.read_index_keys_in_raw_range(index_store, index, bounds, limit)
    }
}

impl<E> StructuralPrimaryRowReader for dyn PrimaryRowReader<E> + '_
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        self.read_primary_row(key)
    }
}

impl<E> SealedStructuralPrimaryRowReader for dyn PrimaryRowReader<E> + '_ where
    E: EntityKind + EntityValue
{
}

impl<E> SealedStructuralIndexEntryReader for dyn IndexEntryReader<E> + '_ where
    E: EntityKind + EntityValue
{
}
