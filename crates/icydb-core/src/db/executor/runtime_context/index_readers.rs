//! Module: executor::runtime_context::index_readers
//! Responsibility: adapt executor contexts to index-planning reader ports.
//! Does not own: index delta derivation or commit-op materialization.
//! Boundary: executor-owned wiring between `Context` and index read traits.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey},
        direction::Direction,
        executor::Context,
        index::{
            IndexEntryReader, IndexStore, PrimaryRowReader, RawIndexEntry, RawIndexKey,
            SealedIndexEntryReader, SealedPrimaryRowReader, SealedStructuralIndexEntryReader,
            SealedStructuralPrimaryRowReader, StructuralIndexEntryReader,
            StructuralPrimaryRowReader,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

impl<E> PrimaryRowReader<E> for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        match self.read(key) {
            Ok(row) => Ok(Some(row)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<E> SealedPrimaryRowReader<E> for Context<'_, E> where E: EntityKind + EntityValue {}

impl<E> StructuralPrimaryRowReader for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        PrimaryRowReader::<E>::read_primary_row(self, key)
    }
}

impl<E> SealedStructuralPrimaryRowReader for Context<'_, E> where E: EntityKind + EntityValue {}

impl<E> IndexEntryReader<E> for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        Ok(store.with_borrow(|index_store| index_store.get(key)))
    }

    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        read_index_storage_keys_in_raw_range(E::ENTITY_TAG, store, index, bounds, limit)
    }
}

impl<E> SealedIndexEntryReader<E> for Context<'_, E> where E: EntityKind + EntityValue {}

impl<E> StructuralIndexEntryReader for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry_structural(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        IndexEntryReader::<E>::read_index_entry(self, store, key)
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        _entity_path: &'static str,
        entity_tag: EntityTag,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        read_index_storage_keys_in_raw_range(entity_tag, store, index, bounds, limit)
    }
}

impl<E> SealedStructuralIndexEntryReader for Context<'_, E> where E: EntityKind + EntityValue {}

// Resolve structural storage keys from one raw index range using the shared
// context-backed index-store reader path.
fn read_index_storage_keys_in_raw_range(
    _entity_tag: EntityTag,
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &IndexModel,
    bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    limit: usize,
) -> Result<Vec<StorageKey>, InternalError> {
    let mut out = Vec::with_capacity(limit.min(32));
    store.with_borrow(|index_store| {
        index_store.visit_raw_entries_in_range(bounds, Direction::Asc, |_, raw_entry| {
            push_index_entry_storage_keys(index, raw_entry, &mut out, limit)
        })
    })?;

    Ok(out)
}

// Decode one raw index entry into structural storage keys for executor context
// preflight reads that are not part of a user-visible scan.
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
