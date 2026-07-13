//! Module: index::readers
//! Responsibility: narrow read ports used by commit and executor preflight flows.
//! Does not own: index delta derivation, commit-op materialization, or executor state.
//! Boundary: callers implement these ports; index-adjacent planners consume them abstractly.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow},
        index::{IndexEntryValue, IndexStore, RawIndexStoreKey},
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

///
/// IndexReadContract
///
/// Reduced index facts needed to decode raw index-entry membership without
/// reopening generated index definitions.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct IndexReadContract<'a> {
    store_path: &'a str,
    unique: bool,
}

impl<'a> IndexReadContract<'a> {
    /// Build one reduced index read contract.
    #[must_use]
    pub(in crate::db) const fn new(store_path: &'a str, unique: bool) -> Self {
        Self { store_path, unique }
    }

    /// Borrow the schema-owned index store path.
    #[must_use]
    pub(in crate::db) const fn store_path(self) -> &'a str {
        self.store_path
    }

    /// Return whether index entries are unique-entry encoded.
    #[must_use]
    pub(in crate::db) const fn unique(self) -> bool {
        self.unique
    }
}

///
/// StructuralPrimaryRowReader
///
/// Narrow nongeneric read port used by structural commit helpers that only
/// need authoritative primary-row lookup.
///

pub(in crate::db) trait StructuralPrimaryRowReader {
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError>;
}

///
/// StructuralIndexEntryReader
///
/// Narrow nongeneric read port used by structural relation/commit helpers that
/// only need authoritative index-entry lookup.
///

pub(in crate::db) trait StructuralIndexEntryReader {
    /// Return the index entry for `(index_store, key)`, or `None` when no entry exists.
    fn read_index_entry(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from
    /// `index_store` in raw key range.
    fn read_index_keys_in_raw_range(
        &self,
        entity_path: &'static str,
        entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError>;
}
