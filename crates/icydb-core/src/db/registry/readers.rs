use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow, StorageKey},
        direction::Direction,
        index::{
            IndexEntryValue, IndexReadContract, IndexStore, RawIndexStoreKey,
            SealedStructuralIndexEntryReader, SealedStructuralPrimaryRowReader,
            StructuralIndexEntryReader, StructuralPrimaryRowReader,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

impl StructuralPrimaryRowReader for StoreHandle {
    fn read_primary_row_structural(
        &self,
        key: &DecodedDataStoreKey,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;

        Ok(self.with_data(|store| store.get(&raw_key)))
    }
}

impl SealedStructuralPrimaryRowReader for StoreHandle {}

impl StructuralIndexEntryReader for StoreHandle {
    fn read_index_entry_structural(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        Ok(index_store.with_borrow(|store| store.get(key)))
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        _entity_path: &'static str,
        _entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        let mut out = Vec::with_capacity(limit.min(32));
        index_store.with_borrow(|store| {
            store.visit_raw_entries_in_range(bounds, Direction::Asc, |raw_key, raw_entry| {
                push_index_entry_primary_key_values(index, raw_key, raw_entry, &mut out, limit)
            })
        })?;

        Ok(out)
    }
}

impl SealedStructuralIndexEntryReader for StoreHandle {}

// Decode one raw index entry into structural primary-key values for
// non-executor preflight readers.
fn push_index_entry_primary_key_values(
    index: IndexReadContract<'_>,
    raw_key: &RawIndexStoreKey,
    raw_entry: &IndexEntryValue,
    out: &mut Vec<StorageKey>,
    limit: usize,
) -> Result<bool, InternalError> {
    raw_entry.push_row_identity_keys_limited(raw_key, out, limit, |err| {
        InternalError::index_plan_index_corruption(format!(
            "index corrupted: ({}) -> {err}",
            index.fields()
        ))
    })
}
