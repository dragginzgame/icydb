//! Module: db::registry::readers
//! Responsibility: structural row and index reader adapters for registered stores.
//! Does not own: executor scan policy or index-entry decode semantics.
//! Boundary: lets StoreHandle satisfy structural preflight reader traits.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow},
        direction::Direction,
        index::{
            IndexEntryValue, IndexStore, RawIndexStoreKey, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, push_structural_index_entry_primary_key_values_limited,
        },
        key_taxonomy::PrimaryKeyValue,
        registry::StoreHandle,
    },
    error::InternalError,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

impl StructuralPrimaryRowReader for StoreHandle {
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;

        Ok(self.with_data(|store| store.get(&raw_key)))
    }
}

impl StructuralIndexEntryReader for StoreHandle {
    fn read_index_entry(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        Ok(index_store.with_borrow(|store| store.get(key)))
    }

    fn read_index_keys_in_raw_range(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError> {
        let mut out = Vec::with_capacity(limit.min(32));
        index_store.with_borrow(|store| {
            store.visit_raw_entries_in_range(bounds, Direction::Asc, |raw_key, raw_entry| {
                push_structural_index_entry_primary_key_values_limited(
                    raw_key, raw_entry, &mut out, limit,
                )
            })
        })?;

        Ok(out)
    }
}
