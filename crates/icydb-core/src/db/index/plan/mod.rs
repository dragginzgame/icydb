//! Module: index::plan
//! Responsibility: preflight planning for deterministic index mutations.
//! Does not own: commit marker protocol or runtime apply sequencing.
//! Boundary: executor/commit call this module before writing commit markers.

mod commit_ops;
mod unique;

use crate::{
    db::{
        Db,
        commit::CommitIndexOp,
        data::{DataKey, RawRow},
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

///
/// IndexApplyPlan
///

#[derive(Debug)]
pub(crate) struct IndexApplyPlan {
    pub index: &'static IndexModel,
    pub store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
///

#[derive(Debug)]
pub(crate) struct IndexMutationPlan {
    pub apply: Vec<IndexApplyPlan>,
    pub commit_ops: Vec<CommitIndexOp>,
}

///
/// PrimaryRowReader
///
/// Index-planning port used for reading authoritative primary rows without
/// depending on executor context internals.
///

pub(in crate::db) trait PrimaryRowReader<E: EntityKind + EntityValue> {
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError>;
}

///
/// IndexEntryReader
///
/// Index-planning port used for reading authoritative index entries without
/// requiring commit preflight to mutate real stores.
///

pub(in crate::db) trait IndexEntryReader<E: EntityKind + EntityValue> {
    /// Return the index entry for `(store, key)`, or `None` when no entry exists.
    fn read_index_entry(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` entity keys resolved from `store` in raw key range.
    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<E::Key>, InternalError>;
}

/// Plan all index mutations for a single entity transition.
///
/// This function:
/// - Loads existing index entries
/// - Validates unique constraints
/// - Computes the exact index writes/deletes required
///
/// All fallible work happens here. The returned plan is safe to apply
/// infallibly after a commit marker is written.
pub(in crate::db) fn plan_index_mutation_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_reader: &impl PrimaryRowReader<E>,
    index_reader: &impl IndexEntryReader<E>,
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    // Phase 1: derive old/new entity identities and allocate plan buffers.
    let old_entity_key = old.map(|entity| entity.id().key());
    let new_entity_key = new.map(|entity| entity.id().key());

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    // Phase 2: per-index load, validate, and synthesize commit ops.
    for index in E::INDEXES {
        let store = db
            .with_store_registry(|registry| registry.try_get_store(index.store))?
            .index_store();

        let old_key = match old {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };
        let new_key = match new {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };

        let old_entry = load_existing_entry(index_reader, store, index, old)?;

        // Prevalidate membership so commit-phase mutations cannot surface corruption.
        if let Some(old_key) = &old_key {
            let Some(old_entity_key) = old_entity_key else {
                return Err(InternalError::index_internal(
                    "missing old entity key for index removal".to_string(),
                ));
            };

            let entry = old_entry.as_ref().ok_or_else(|| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                ))
            })?;

            if index.unique && entry.len() > 1 {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }
                )));
            }

            if !entry.contains(old_entity_key) {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                )));
            }
        }

        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load_existing_entry(index_reader, store, index, new)?
        };

        // Unique validation is evaluated against the currently committed store
        // state for the target unique value. Commit-op synthesis then applies
        // remove-old/add-new semantics, so valid key transitions are evaluated
        // on the correct post-transition logical ownership model.
        unique::validate_unique_constraint::<E>(
            row_reader,
            index_reader,
            index,
            store,
            new_entity_key.as_ref(),
            new,
        )?;

        commit_ops::build_commit_ops_for_index::<E>(
            &mut commit_ops,
            index,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_entity_key,
            new_entity_key,
        )?;

        apply.push(IndexApplyPlan { index, store });
    }

    // Phase 3: return deterministic apply + commit-op plan.
    Ok(IndexMutationPlan { apply, commit_ops })
}

pub(super) fn load_existing_entry<E: EntityKind + EntityValue>(
    index_reader: &impl IndexEntryReader<E>,
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry<E>>, InternalError> {
    // No entity transition input means no index entry to load.
    let Some(entity) = entity else {
        return Ok(None);
    };

    // Build the candidate key; non-indexable values produce no entry.
    let Some(key) = IndexKey::new(entity, index)? else {
        return Ok(None);
    };
    let raw_key = key.to_raw();

    index_reader
        .read_index_entry(store, &raw_key)?
        .map(|raw_entry| {
            raw_entry.try_decode().map_err(|err| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    err
                ))
            })
        })
        .transpose()
}
