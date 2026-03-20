//! Module: index::plan
//! Responsibility: preflight planning for deterministic index mutations.
//! Does not own: commit marker protocol or runtime apply sequencing.
//! Boundary: executor/commit call this module before writing commit markers.

mod commit_ops;
mod private;
mod unique;

use crate::{
    db::{
        Db,
        commit::CommitIndexOp,
        cursor::IndexScanContinuationInput,
        data::{DataKey, RawRow, StorageKey},
        direction::Direction,
        executor::Context,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
            canonical_index_predicate,
        },
        predicate::PredicateProgram,
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue, FieldValue},
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

pub(in crate::db) use private::{SealedIndexEntryReader, SealedPrimaryRowReader};

///
/// IndexApplyPlan
/// Planned apply target for one index store mutation group.
///

#[derive(Debug)]
pub(in crate::db) struct IndexApplyPlan {
    pub(in crate::db) index: &'static IndexModel,
    pub(in crate::db) store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
/// Deterministic mutation plan containing apply targets and commit ops.
///

#[derive(Debug)]
pub(in crate::db) struct IndexMutationPlan {
    pub(in crate::db) apply: Vec<IndexApplyPlan>,
    pub(in crate::db) commit_ops: Vec<CommitIndexOp>,
}

///
/// PrimaryRowReader
///
/// Index-planning port used for reading authoritative primary rows without
/// depending on executor context internals.
///

pub(in crate::db) trait PrimaryRowReader<E: EntityKind + EntityValue>:
    SealedPrimaryRowReader<E>
{
    /// Return the primary row for `key`, or `None` when no row exists.
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError>;
}

///
/// IndexEntryReader
///
/// Index-planning port used for reading authoritative index entries without
/// requiring commit preflight to mutate real stores.
///

pub(in crate::db) trait IndexEntryReader<E: EntityKind + EntityValue>:
    SealedIndexEntryReader<E>
{
    /// Return the index entry for `(store, key)`, or `None` when no entry exists.
    fn read_index_entry(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from `store`
    /// in raw key range.
    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError>;
}

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
        let data_keys = store.with_borrow(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                E::ENTITY_TAG,
                index,
                bounds,
                IndexScanContinuationInput::new(None, Direction::Asc),
                limit,
                None,
            )
        })?;

        let mut out = Vec::with_capacity(data_keys.len());
        for data_key in data_keys {
            out.push(data_key.storage_key());
        }

        Ok(out)
    }
}

impl<E> SealedIndexEntryReader<E> for Context<'_, E> where E: EntityKind + EntityValue {}

/// Compile the optional conditional-index predicate into one runtime program.
pub(in crate::db) fn compile_index_membership_predicate<E: EntityKind>(
    index: &IndexModel,
) -> Result<Option<PredicateProgram>, InternalError> {
    let Some(predicate_sql) = index.predicate() else {
        return Ok(None);
    };

    let predicate = canonical_index_predicate(index).map_err(|err| {
        InternalError::index_invariant(format!(
            "index predicate parse failed: {} ({}) WHERE {} -> {err}",
            E::PATH,
            index.name(),
            predicate_sql,
        ))
    })?;
    let predicate = predicate.expect("index predicate metadata was checked above");

    Ok(Some(PredicateProgram::compile::<E>(predicate)))
}

/// Build one index key for an entity after applying optional predicate gating.
pub(in crate::db) fn index_key_for_entity_with_membership<E: EntityKind + EntityValue>(
    index: &IndexModel,
    predicate_program: Option<&PredicateProgram>,
    entity: &E,
) -> Result<Option<IndexKey>, InternalError> {
    if let Some(predicate_program) = predicate_program
        && !predicate_program.eval(entity)
    {
        return Ok(None);
    }

    IndexKey::new(entity, index)
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
    row_reader: &(impl PrimaryRowReader<E> + ?Sized),
    index_reader: &(impl IndexEntryReader<E> + ?Sized),
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    // Phase 1: derive old/new entity identities and allocate plan buffers.
    let old_entity_key = old.map(|entity| entity.id().key());
    let new_entity_key = new.map(|entity| entity.id().key());
    let old_entity_storage_key = old_entity_key
        .as_ref()
        .map(|key| StorageKey::try_from_value(&key.to_value()))
        .transpose()?;
    let new_entity_storage_key = new_entity_key
        .as_ref()
        .map(|key| StorageKey::try_from_value(&key.to_value()))
        .transpose()?;

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    // Phase 2: per-index load, validate, and synthesize commit ops.
    for index in E::INDEXES {
        let store = db
            .with_store_registry(|registry| registry.try_get_store(index.store()))?
            .index_store();
        let membership_program = compile_index_membership_predicate::<E>(index)?;

        let old_key = match old {
            Some(entity) => {
                index_key_for_entity_with_membership(index, membership_program.as_ref(), entity)?
            }
            None => None,
        };
        let new_key = match new {
            Some(entity) => {
                index_key_for_entity_with_membership(index, membership_program.as_ref(), entity)?
            }
            None => None,
        };

        let old_entry = load_existing_entry(index_reader, store, index, old_key.as_ref())?;

        // Prevalidate membership so commit-phase mutations cannot surface corruption.
        if let Some(old_key) = &old_key {
            let Some(old_entity_storage_key) = old_entity_storage_key else {
                return Err(InternalError::index_internal(
                    "missing old entity key for index removal".to_string(),
                ));
            };

            let entry = old_entry.as_ref().ok_or_else(|| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields().join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_storage_key)
                ))
            })?;

            if index.is_unique() && entry.len() > 1 {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields().join(", "),
                    IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }
                )));
            }

            if !entry.contains(old_entity_storage_key) {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields().join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_storage_key)
                )));
            }
        }

        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load_existing_entry(index_reader, store, index, new_key.as_ref())?
        };

        // Unique validation is evaluated through the provided reader view for
        // the target unique value. During commit-window preflight that reader
        // can include staged prior row ops (overlay), so same-window conflicts
        // are validated against the correct logical ownership model.
        let unique_new_entity = if new_key.is_some() { new } else { None };
        let unique_new_entity_key = if new_key.is_some() {
            new_entity_key.as_ref()
        } else {
            None
        };
        unique::validate_unique_constraint::<E>(
            row_reader,
            index_reader,
            index,
            store,
            unique_new_entity_key,
            unique_new_entity,
        )?;

        commit_ops::build_commit_ops_for_index(
            &mut commit_ops,
            index,
            E::PATH,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_entity_storage_key,
            new_entity_storage_key,
        )?;

        apply.push(IndexApplyPlan { index, store });
    }

    // Phase 3: return deterministic apply + commit-op plan.
    Ok(IndexMutationPlan { apply, commit_ops })
}

pub(super) fn load_existing_entry<E: EntityKind + EntityValue>(
    index_reader: &(impl IndexEntryReader<E> + ?Sized),
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    key: Option<&IndexKey>,
) -> Result<Option<IndexEntry>, InternalError> {
    // No indexed key means no index entry to load.
    let Some(key) = key else {
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
                    index.fields().join(", "),
                    err
                ))
            })
        })
        .transpose()
}
