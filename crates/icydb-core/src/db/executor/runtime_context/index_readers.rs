//! Module: executor::runtime_context::index_readers
//! Responsibility: adapt executor contexts to index-planning reader ports.
//! Does not own: index delta derivation or commit-op materialization.
//! Boundary: executor-owned wiring between `Context` and index read traits.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow},
        direction::Direction,
        executor::Context,
        index::{
            IndexEntryValue, IndexReadContract, IndexStore, RawIndexStoreKey,
            StructuralIndexEntryReader, StructuralPrimaryRowReader,
        },
        key_taxonomy::PrimaryKeyValue,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

impl<E> StructuralPrimaryRowReader for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError> {
        match self.read(key) {
            Ok(row) => Ok(Some(row)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<E> StructuralIndexEntryReader for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        Ok(index_store.with_borrow(|store| store.get(key)))
    }

    fn read_index_keys_in_raw_range(
        &self,
        _entity_path: &'static str,
        entity_tag: EntityTag,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError> {
        read_index_primary_key_values_in_raw_range(entity_tag, index_store, index, bounds, limit)
    }
}

// Resolve structural primary-key values from one raw index range using the shared
// context-backed index-store reader path.
fn read_index_primary_key_values_in_raw_range(
    _entity_tag: EntityTag,
    index_store: &'static LocalKey<RefCell<IndexStore>>,
    index: IndexReadContract<'_>,
    bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
    limit: usize,
) -> Result<Vec<PrimaryKeyValue>, InternalError> {
    let mut out = Vec::with_capacity(limit.min(32));
    index_store.with_borrow(|store| {
        store.visit_raw_entries_in_range(bounds, Direction::Asc, |raw_key, raw_entry| {
            push_index_entry_primary_key_values(index, raw_key, raw_entry, &mut out, limit)
        })
    })?;

    Ok(out)
}

// Decode one raw index entry into structural primary-key values for executor
// context preflight reads that are not part of a user-visible scan.
fn push_index_entry_primary_key_values(
    _index: IndexReadContract<'_>,
    raw_key: &RawIndexStoreKey,
    raw_entry: &IndexEntryValue,
    out: &mut Vec<PrimaryKeyValue>,
    limit: usize,
) -> Result<bool, InternalError> {
    raw_entry.push_row_identity_primary_key_values_limited(raw_key, out, limit, |_err| {
        InternalError::index_plan_index_corruption()
    })
}
