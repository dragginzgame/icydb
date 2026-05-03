use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey},
        direction::Direction,
        index::{
            IndexStore, RawIndexEntry, RawIndexKey, SealedStructuralIndexEntryReader,
            SealedStructuralPrimaryRowReader, StructuralIndexEntryReader,
            StructuralPrimaryRowReader,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

impl StructuralPrimaryRowReader for StoreHandle {
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;

        Ok(self.with_data(|store| store.get(&raw_key)))
    }
}

impl SealedStructuralPrimaryRowReader for StoreHandle {}

impl StructuralIndexEntryReader for StoreHandle {
    fn read_index_entry_structural(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        Ok(index_store.with_borrow(|store| store.get(key)))
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
        let mut out = Vec::with_capacity(limit.min(32));
        index_store.with_borrow(|store| {
            store.visit_raw_entries_in_range(bounds, Direction::Asc, |_, raw_entry| {
                push_index_entry_storage_keys(index, raw_entry, &mut out, limit)
            })
        })?;

        Ok(out)
    }
}

impl SealedStructuralIndexEntryReader for StoreHandle {}

// Decode one raw index entry into structural storage keys for non-executor
// preflight readers.
fn push_index_entry_storage_keys(
    index: &IndexModel,
    raw_entry: &RawIndexEntry,
    out: &mut Vec<StorageKey>,
    limit: usize,
) -> Result<bool, InternalError> {
    raw_entry.push_membership_storage_keys_limited(
        index.is_unique(),
        out,
        limit,
        |err| {
            InternalError::index_plan_index_corruption(format!(
                "index corrupted: ({}) -> {}",
                index.fields().join(", "),
                err
            ))
        },
        InternalError::unique_index_entry_single_key_required,
    )
}
