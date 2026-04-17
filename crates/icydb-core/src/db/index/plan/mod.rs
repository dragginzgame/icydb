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
        data::{CanonicalSlotReader, DataKey, RawRow, StorageKey},
        direction::Direction,
        executor::Context,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
            canonical_index_predicate,
        },
        predicate::PredicateProgram,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    traits::{CanisterKind, EntityKind, EntityValue},
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

pub(in crate::db) use private::{
    SealedIndexEntryReader, SealedPrimaryRowReader, SealedStructuralIndexEntryReader,
    SealedStructuralPrimaryRowReader,
};

// Narrow store-lookup callback used to keep the structural planner body
// nongeneric after the `Db<C>` wrapper has resolved registry access.
type IndexStoreLookup<'a> =
    dyn FnMut(&IndexModel) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError> + 'a;

// Distinguish the two structural key-build lanes so planner diagnostics can
// preserve the existing insertion-vs-removal error taxonomy.
#[derive(Clone, Copy)]
enum IndexKeyLane {
    Old,
    New,
}

impl IndexKeyLane {
    // Map one missing entity-key case back onto the planner-owned internal error.
    fn missing_entity_key_error(self) -> InternalError {
        match self {
            Self::Old => InternalError::structural_index_removal_entity_key_required(),
            Self::New => InternalError::structural_index_insertion_entity_key_required(),
        }
    }
}

// Format the canonical human-readable index field list once at the plan boundary.
pub(super) fn index_fields_csv(index: &IndexModel) -> String {
    index.fields().join(", ")
}

///
/// IndexMutationPlan
/// Deterministic mutation plan containing mechanical commit ops.
///

#[derive(Debug)]
pub(in crate::db) struct IndexMutationPlan {
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

///
/// StructuralIndexEntryReader
///
/// Narrow nongeneric read port used by structural relation/commit helpers that
/// only need authoritative index-entry lookup.
///

pub(in crate::db) trait StructuralIndexEntryReader:
    SealedStructuralIndexEntryReader
{
    /// Return the index entry for `(store, key)`, or `None` when no entry exists.
    fn read_index_entry_structural(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError>;

    /// Return up to `limit` structural primary-key values resolved from `store`
    /// in raw key range.
    fn read_index_keys_in_raw_range_structural(
        &self,
        entity_path: &'static str,
        entity_tag: EntityTag,
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

impl<E> StructuralIndexEntryReader for dyn IndexEntryReader<E> + '_
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry_structural(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        self.read_index_entry(store, key)
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        _entity_path: &'static str,
        _entity_tag: EntityTag,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        self.read_index_keys_in_raw_range(store, index, bounds, limit)
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

// Resolve structural storage keys from one raw index range using the shared
// context-backed index-store reader path.
fn read_index_storage_keys_in_raw_range(
    entity_tag: EntityTag,
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &IndexModel,
    bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    limit: usize,
) -> Result<Vec<StorageKey>, InternalError> {
    let data_keys = store.with_borrow(|index_store| {
        index_store.resolve_data_values_in_raw_range_limited(
            entity_tag,
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

/// Compile the optional conditional-index predicate from structural entity
/// authority only.
pub(in crate::db) fn compile_index_membership_predicate_structural(
    _entity_path: &'static str,
    model: &'static EntityModel,
    index: &IndexModel,
) -> Option<PredicateProgram> {
    let predicate = canonical_index_predicate(index)?;

    Some(PredicateProgram::compile(model, predicate))
}

/// Build one index key from one slot reader using structural entity authority only.
pub(in crate::db) fn index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    index: &IndexModel,
    predicate_program: Option<&PredicateProgram>,
    storage_key: StorageKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    if let Some(predicate_program) = predicate_program {
        let keep_row = predicate_program.eval_with_structural_slot_reader(slots)?;
        if !keep_row {
            return Ok(None);
        }
    }

    let index_key = IndexKey::new_from_slots(entity_tag, storage_key, slots, index)?;

    Ok(index_key)
}

// Build one optional structural index key for the requested planner lane.
fn load_structural_index_key(
    lane: IndexKeyLane,
    entity_tag: EntityTag,
    index: &IndexModel,
    predicate_program: Option<&PredicateProgram>,
    storage_key: Option<StorageKey>,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    let Some(storage_key) = storage_key else {
        return Err(lane.missing_entity_key_error());
    };

    index_key_for_slot_reader_with_membership_structural(
        entity_tag,
        index,
        predicate_program,
        storage_key,
        slots,
    )
}

// Prove that the pre-existing old index entry still contains the expected row
// membership before commit planning becomes purely mechanical.
fn validate_existing_old_index_membership(
    entity_path: &'static str,
    index_fields: &str,
    index_is_unique: bool,
    old_storage_key: Option<StorageKey>,
    old_key: Option<&IndexKey>,
    old_entry: Option<&IndexEntry>,
) -> Result<(), InternalError> {
    let Some(old_key) = old_key else {
        return Ok(());
    };

    let Some(old_storage_key) = old_storage_key else {
        return Err(InternalError::structural_index_removal_entity_key_required());
    };

    let entry = old_entry.as_ref().ok_or_else(|| {
        InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::missing_key(old_key.to_raw(), old_storage_key),
        )
    })?;

    if index_is_unique && entry.len() > 1 {
        return Err(InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::NonUniqueEntry { keys: entry.len() },
        ));
    }

    if !entry.contains(old_storage_key) {
        return Err(InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::missing_key(old_key.to_raw(), old_storage_key),
        ));
    }

    Ok(())
}

/// Plan all index mutations for one persisted-row transition using structural
/// entity authority only.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn plan_index_mutation_for_slot_reader_structural<C>(
    db: &Db<C>,
    entity_path: &'static str,
    entity_tag: EntityTag,
    model: &'static EntityModel,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    old_storage_key: Option<StorageKey>,
    old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_storage_key: Option<StorageKey>,
    new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, InternalError>
where
    C: CanisterKind,
{
    let mut store_for_index = |index: &IndexModel| {
        db.with_store_registry(|registry| registry.try_get_store(index.store()))
            .map(|store| store.index_store())
    };

    plan_index_mutation_for_slot_reader_structural_impl(
        &mut store_for_index,
        entity_path,
        entity_tag,
        model,
        row_reader,
        index_reader,
        old_storage_key,
        old_slots,
        new_storage_key,
        new_slots,
    )
}

// Keep the structural planner loop nongeneric once store lookup has already
// been lowered onto one index-store callback.
#[expect(clippy::too_many_arguments)]
fn plan_index_mutation_for_slot_reader_structural_impl(
    store_for_index: &mut IndexStoreLookup<'_>,
    entity_path: &'static str,
    entity_tag: EntityTag,
    model: &'static EntityModel,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    old_storage_key: Option<StorageKey>,
    mut old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_storage_key: Option<StorageKey>,
    mut new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, InternalError> {
    let indexes = model.indexes();
    let mut commit_ops = Vec::new();

    // Phase 1: per-index load, validate, and synthesize commit ops from
    // slot-reader projections only.
    for index in indexes {
        let store = store_for_index(index)?;
        let index_fields = index_fields_csv(index);
        let membership_program =
            compile_index_membership_predicate_structural(entity_path, model, index);

        let old_key = match old_slots.as_deref_mut() {
            Some(slots) => load_structural_index_key(
                IndexKeyLane::Old,
                entity_tag,
                index,
                membership_program.as_ref(),
                old_storage_key,
                slots,
            )?,
            None => None,
        };
        let new_key = match new_slots.as_deref_mut() {
            Some(slots) => load_structural_index_key(
                IndexKeyLane::New,
                entity_tag,
                index,
                membership_program.as_ref(),
                new_storage_key,
                slots,
            )?,
            None => None,
        };

        let old_entry = load_existing_entry_structural(
            index_reader,
            store,
            &index_fields,
            old_key.as_ref(),
            entity_path,
        )?;

        // Phase 2: ensure any existing old membership is still present before
        // commit-phase mutations become mechanical.
        validate_existing_old_index_membership(
            entity_path,
            &index_fields,
            index.is_unique(),
            old_storage_key,
            old_key.as_ref(),
            old_entry.as_ref(),
        )?;

        let new_entry = if old_key == new_key {
            None
        } else {
            load_existing_entry_structural(
                index_reader,
                store,
                &index_fields,
                new_key.as_ref(),
                entity_path,
            )?
        };

        unique::validate_unique_constraint_structural(
            entity_path,
            entity_tag,
            model,
            row_reader,
            index_reader,
            index,
            &index_fields,
            store,
            if new_key.is_some() {
                new_storage_key
            } else {
                None
            },
            new_key.as_ref(),
        )?;

        commit_ops::build_commit_ops_for_index(
            &mut commit_ops,
            store,
            entity_path,
            &index_fields,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_storage_key,
            new_storage_key,
        )?;
    }

    Ok(IndexMutationPlan { commit_ops })
}

pub(super) fn load_existing_entry_structural(
    index_reader: &dyn StructuralIndexEntryReader,
    store: &'static LocalKey<RefCell<IndexStore>>,
    index_fields: &str,
    key: Option<&IndexKey>,
    entity_path: &'static str,
) -> Result<Option<IndexEntry>, InternalError> {
    // No indexed key means no index entry to load.
    let Some(key) = key else {
        return Ok(None);
    };

    let raw_key = key.to_raw();

    index_reader
        .read_index_entry_structural(store, &raw_key)?
        .map(|raw_entry| {
            raw_entry.try_decode().map_err(|err| {
                InternalError::structural_index_entry_corruption(entity_path, index_fields, err)
            })
        })
        .transpose()
}
