//! Module: index::readers
//! Responsibility: narrow read ports used by commit and executor preflight flows.
//! Does not own: index delta derivation, commit-op materialization, or executor state.
//! Boundary: callers implement these ports; index-adjacent planners consume them abstractly.

use crate::{
    db::{
        Db,
        data::{DecodedDataStoreKey, RawRow},
        index::{IndexEntryValue, IndexStore, RawIndexStoreKey},
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    traits::CanisterKind,
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

    /// Return whether this reader overrides committed state for `key`.
    ///
    /// Commit-batch overlays use this to distinguish an intentional final
    /// update/delete from a corrupt index entry whose committed row is absent
    /// or no longer belongs to the indexed key.
    fn has_primary_row_override(&self, _key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        Ok(false)
    }
}

impl<C: CanisterKind> StructuralPrimaryRowReader for Db<C> {
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError> {
        let runtime_entity = self.accepted_runtime_entity_for_tag(key.entity_tag())?;
        let store = self.recovered_store(runtime_entity.store_path())?;
        let raw_key = key.to_raw()?;

        Ok(store.with_data(|data_store| data_store.get(&raw_key)))
    }
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
        entity_path: &str,
        entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError>;
}
