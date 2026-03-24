//! Module: commit::prepare
//! Responsibility: decode commit-marker row ops into mechanical store mutations.
//! Does not own: marker persistence, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::marker -> commit::prepare -> commit::apply (one-way).

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedIndexDeltaKind, PreparedIndexMutation, PreparedRowCommitOp,
            decode_data_key, decode_index_entry, decode_index_key,
        },
        data::{
            DataKey, DataStore, RawDataKey, RawRow, SlotReader, StorageKey, StructuralSlotReader,
        },
        index::{
            IndexEntryReader, IndexMutationPlan, IndexStore, PrimaryRowReader, RawIndexEntry,
            RawIndexKey, StructuralIndexEntryReader, StructuralPrimaryRowReader,
            compile_index_membership_predicate_structural,
            index_key_for_slot_reader_with_membership_structural,
            plan_index_mutation_for_slot_reader_structural,
        },
        relation::{
            ReverseRelationSourceInfo, prepare_reverse_relation_index_mutations_for_source_rows,
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, InternalError},
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue, Path},
    types::EntityTag,
};
use std::{cell::RefCell, collections::BTreeMap, marker::PhantomData, thread::LocalKey};

///
/// CommitPrepareAuthority
///
/// Structural authority needed by nongeneric commit-preparation stages.
///

struct CommitPrepareAuthority {
    entity_path: &'static str,
    entity_tag: EntityTag,
    schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    data_store_path: &'static str,
    relation_source: ReverseRelationSourceInfo,
    model: &'static EntityModel,
}

impl CommitPrepareAuthority {
    /// Lower one entity type into the structural authority used by commit preparation.
    fn for_type<E>() -> Self
    where
        E: EntityKind + Path,
    {
        Self {
            entity_path: E::PATH,
            entity_tag: E::ENTITY_TAG,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            data_store_path: E::Store::PATH,
            relation_source: ReverseRelationSourceInfo::for_type::<E>(),
            model: E::MODEL,
        }
    }
}

///
/// ForwardIndexCommitPreparation
///
/// Structural forward-index output produced from slot readers only.
///

struct ForwardIndexCommitPreparation {
    index_plan: IndexMutationPlan,
    index_delta_kind_by_key: BTreeMap<RawIndexKey, PreparedIndexDeltaKind>,
}

///
/// StructuralCommitInputs
///
/// Structural commit inputs decoded before forward-index planning runs.
///

struct StructuralCommitInputs {
    raw_key: RawDataKey,
    data_key: DataKey,
    old_row: Option<RawRow>,
    new_row: Option<RawRow>,
}

///
/// PreparedRowCommitMaterialization
///
/// Generic-free commit-preparation payload after forward-index planning.
/// Carries only structural index/data artifacts so the final materialization
/// loop does not monomorphize per entity.
///

struct PreparedRowCommitMaterialization {
    entity_path: &'static str,
    index_plan: IndexMutationPlan,
    index_delta_kind_by_key: BTreeMap<RawIndexKey, PreparedIndexDeltaKind>,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_store: &'static LocalKey<RefCell<DataStore>>,
    data_key: RawDataKey,
    data_value: Option<RawRow>,
}

///
/// StructuralPrimaryRowReaderAdapter
///
/// Typed primary-row reader adapter over the nongeneric structural reader
/// boundary used by runtime hook dispatch.
///

struct StructuralPrimaryRowReaderAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    reader: &'a dyn StructuralPrimaryRowReader,
    _marker: PhantomData<E>,
}

impl<'a, E> StructuralPrimaryRowReaderAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    const fn new(reader: &'a dyn StructuralPrimaryRowReader) -> Self {
        Self {
            reader,
            _marker: PhantomData,
        }
    }
}

impl<E> PrimaryRowReader<E> for StructuralPrimaryRowReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        self.reader.read_primary_row_structural(key)
    }
}

impl<E> crate::db::index::SealedPrimaryRowReader<E> for StructuralPrimaryRowReaderAdapter<'_, E> where
    E: EntityKind + EntityValue
{
}

impl<E> StructuralPrimaryRowReader for StructuralPrimaryRowReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        self.reader.read_primary_row_structural(key)
    }
}

impl<E> crate::db::index::SealedStructuralPrimaryRowReader
    for StructuralPrimaryRowReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
}

///
/// StructuralCommitPrepareIndexReaderAdapter
///
/// Typed index-reader adapter over the nongeneric structural reader boundary
/// used by runtime hook dispatch.
///

struct StructuralCommitPrepareIndexReaderAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    entity_path: &'static str,
    reader: &'a dyn StructuralIndexEntryReader,
    _marker: PhantomData<E>,
}

impl<'a, E> StructuralCommitPrepareIndexReaderAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    const fn new(entity_path: &'static str, reader: &'a dyn StructuralIndexEntryReader) -> Self {
        Self {
            entity_path,
            reader,
            _marker: PhantomData,
        }
    }
}

impl<E> IndexEntryReader<E> for StructuralCommitPrepareIndexReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        self.reader.read_index_entry_structural(store, key)
    }

    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &crate::model::index::IndexModel,
        bounds: (&std::ops::Bound<RawIndexKey>, &std::ops::Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        self.reader.read_index_keys_in_raw_range_structural(
            self.entity_path,
            E::ENTITY_TAG,
            store,
            index,
            bounds,
            limit,
        )
    }
}

impl<E> StructuralIndexEntryReader for StructuralCommitPrepareIndexReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry_structural(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        self.reader.read_index_entry_structural(store, key)
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        entity_path: &'static str,
        entity_tag: crate::types::EntityTag,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &crate::model::index::IndexModel,
        bounds: (&std::ops::Bound<RawIndexKey>, &std::ops::Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        self.reader.read_index_keys_in_raw_range_structural(
            entity_path,
            entity_tag,
            store,
            index,
            bounds,
            limit,
        )
    }
}

impl<E> crate::db::index::SealedIndexEntryReader<E>
    for StructuralCommitPrepareIndexReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
}

impl<E> crate::db::index::SealedStructuralIndexEntryReader
    for StructuralCommitPrepareIndexReaderAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
}

/// Prepare a typed row-level commit op against nongeneric structural readers.
pub(in crate::db) fn prepare_row_commit_for_entity_with_structural_readers<
    E: EntityKind + EntityValue,
>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
) -> Result<PreparedRowCommitOp, InternalError> {
    let authority = CommitPrepareAuthority::for_type::<E>();
    let row_reader = StructuralPrimaryRowReaderAdapter::<E>::new(row_reader);
    let index_reader = StructuralCommitPrepareIndexReaderAdapter::<E>::new(E::PATH, index_reader);

    prepare_row_commit_for_entity_impl(db, op, authority, &row_reader, &index_reader)
}

/// Prepare a typed row-level commit op against typed preflight readers.
pub(in crate::db) fn prepare_row_commit_for_entity_with_readers<E, R, I>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &R,
    index_reader: &I,
) -> Result<PreparedRowCommitOp, InternalError>
where
    E: EntityKind + EntityValue,
    R: PrimaryRowReader<E> + StructuralPrimaryRowReader,
    I: IndexEntryReader<E> + StructuralIndexEntryReader,
{
    prepare_row_commit_for_entity_impl(
        db,
        op,
        CommitPrepareAuthority::for_type::<E>(),
        row_reader,
        index_reader,
    )
}

/// Prepare a typed row-level commit op against committed-store readers.
pub(in crate::db) fn prepare_row_commit_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let context = db.context::<E>();
    prepare_row_commit_for_entity_impl(
        db,
        op,
        CommitPrepareAuthority::for_type::<E>(),
        &context,
        &context,
    )
}

// Keep the full commit-preparation body out of the thin wrapper entrypoints so
// codegen does not clone the same logic into both prepare surfaces per entity.
#[inline(never)]
fn prepare_row_commit_for_entity_impl<C>(
    db: &Db<C>,
    op: &CommitRowOp,
    authority: CommitPrepareAuthority,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let structural = prepare_row_commit_structural_inputs(op, &authority)?;
    let forward_index = if authority.model.indexes().is_empty() {
        empty_forward_index_commit_preparation()
    } else {
        prepare_forward_index_commit_leaf(
            db,
            &authority,
            row_reader,
            index_reader,
            &structural.data_key,
            structural.old_row.as_ref(),
            structural.new_row.as_ref(),
        )?
    };

    finalize_row_commit_structural(db, index_reader, authority, structural, forward_index)
}

// Return one empty forward-index preparation when the entity has no secondary indexes.
const fn empty_forward_index_commit_preparation() -> ForwardIndexCommitPreparation {
    ForwardIndexCommitPreparation {
        index_plan: IndexMutationPlan {
            apply: Vec::new(),
            commit_ops: Vec::new(),
        },
        index_delta_kind_by_key: BTreeMap::new(),
    }
}

// Decode only the structural row views required for forward-index planning and
// produce structural-ready forward-index outputs.
fn prepare_forward_index_commit_leaf<C>(
    db: &Db<C>,
    authority: &CommitPrepareAuthority,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    data_key: &DataKey,
    old_row: Option<&RawRow>,
    new_row: Option<&RawRow>,
) -> Result<ForwardIndexCommitPreparation, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let storage_key = data_key.storage_key();
    let mut old_slots = old_row
        .map(|row| decode_commit_marker_row_slots(data_key, row, "before", authority.model))
        .transpose()?;
    let mut new_slots = new_row
        .map(|row| decode_commit_marker_row_slots(data_key, row, "after", authority.model))
        .transpose()?;

    let index_plan = plan_index_mutation_for_slot_reader_structural(
        db,
        authority.entity_path,
        authority.entity_tag,
        authority.model,
        row_reader,
        index_reader,
        old_row.map(|_| storage_key),
        old_slots.as_mut().map(|slots| slots as &mut dyn SlotReader),
        new_row.map(|_| storage_key),
        new_slots.as_mut().map(|slots| slots as &mut dyn SlotReader),
    )?;
    let index_delta_kind_by_key = annotate_forward_index_delta_kinds_from_slots(
        authority.entity_tag,
        authority.entity_path,
        authority.model,
        old_row.map(|_| storage_key),
        old_slots.as_mut().map(|slots| slots as &mut dyn SlotReader),
        new_row.map(|_| storage_key),
        new_slots.as_mut().map(|slots| slots as &mut dyn SlotReader),
    )?;

    Ok(ForwardIndexCommitPreparation {
        index_plan,
        index_delta_kind_by_key,
    })
}

// Decode one commit-marker row into one validated slot reader so forward-index
// planning can stay entirely on structural persisted-row access.
fn decode_commit_marker_row_slots<'a>(
    data_key: &DataKey,
    row: &'a RawRow,
    label: &str,
    model: &'static EntityModel,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    let slots = StructuralSlotReader::from_raw_row(row, model).map_err(|err| {
        let message = format!("commit marker {label} row decode failed: {err}");
        if err.class() == ErrorClass::IncompatiblePersistedFormat {
            InternalError::serialize_incompatible_persisted_format(message)
        } else {
            InternalError::serialize_corruption(message)
        }
    })?;
    slots.validate_storage_key(data_key).map_err(|err| {
        InternalError::store_corruption(format!("commit marker {label} row key mismatch: {err}"))
    })?;

    Ok(slots)
}

// Decode structural commit inputs before the typed forward-index leaf runs.
fn prepare_row_commit_structural_inputs(
    op: &CommitRowOp,
    authority: &CommitPrepareAuthority,
) -> Result<StructuralCommitInputs, InternalError> {
    if op.entity_path != authority.entity_path {
        return Err(InternalError::store_corruption(format!(
            "commit marker entity path mismatch: expected '{}', found '{}'",
            authority.entity_path, op.entity_path
        )));
    }
    if op.schema_fingerprint != authority.schema_fingerprint {
        return Err(InternalError::store_unsupported(format!(
            "commit marker schema fingerprint mismatch for entity '{}': marker={:?}, runtime={:?}",
            authority.entity_path, op.schema_fingerprint, authority.schema_fingerprint
        )));
    }

    let (raw_key, data_key) = decode_data_key(&op.key)?;
    let old_row = op
        .before
        .as_ref()
        .map(|bytes| RawRow::try_new(bytes.clone()))
        .transpose()?;
    let new_row = op
        .after
        .as_ref()
        .map(|bytes| RawRow::try_new(bytes.clone()))
        .transpose()?;

    if old_row.is_none() && new_row.is_none() {
        return Err(InternalError::store_corruption(
            "commit marker row op is a no-op (before/after both missing)",
        ));
    }

    Ok(StructuralCommitInputs {
        raw_key,
        data_key,
        old_row,
        new_row,
    })
}

// Resume structural orchestration after the typed forward-index leaf has
// produced structural-ready outputs.
fn finalize_row_commit_structural<C>(
    db: &Db<C>,
    index_reader: &dyn StructuralIndexEntryReader,
    authority: CommitPrepareAuthority,
    structural: StructuralCommitInputs,
    forward_index: ForwardIndexCommitPreparation,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let reverse_index_ops = prepare_reverse_relation_index_mutations_for_source_rows(
        db,
        index_reader,
        authority.relation_source,
        authority.model,
        structural.data_key.storage_key(),
        structural.old_row.as_ref(),
        structural.new_row.as_ref(),
    )?;
    let data_store = db.with_store_registry(|reg| reg.try_get_store(authority.data_store_path))?;

    materialize_prepared_row_commit(PreparedRowCommitMaterialization {
        entity_path: authority.entity_path,
        index_plan: forward_index.index_plan,
        index_delta_kind_by_key: forward_index.index_delta_kind_by_key,
        reverse_index_ops,
        data_store: data_store.data_store(),
        data_key: structural.raw_key,
        data_value: structural.new_row,
    })
}

// Derive the forward-index delta-kind annotations needed by commit-window
// observability from slot-reader projections only.
fn annotate_forward_index_delta_kinds_from_slots(
    entity_tag: EntityTag,
    entity_path: &'static str,
    model: &'static EntityModel,
    old_storage_key: Option<crate::value::StorageKey>,
    mut old_slots: Option<&mut dyn SlotReader>,
    new_storage_key: Option<crate::value::StorageKey>,
    mut new_slots: Option<&mut dyn SlotReader>,
) -> Result<BTreeMap<RawIndexKey, PreparedIndexDeltaKind>, InternalError> {
    let mut index_delta_kind_by_key = BTreeMap::new();
    for index in model.indexes() {
        let membership_program =
            compile_index_membership_predicate_structural(entity_path, model, index)?;
        let old_key = match old_slots.as_deref_mut() {
            Some(slots) => old_storage_key
                .map(|storage_key| {
                    index_key_for_slot_reader_with_membership_structural(
                        entity_tag,
                        index,
                        membership_program.as_ref(),
                        storage_key,
                        slots,
                    )
                })
                .transpose()?
                .flatten()
                .map(|key| key.to_raw()),
            None => None,
        };
        let new_key = match new_slots.as_deref_mut() {
            Some(slots) => new_storage_key
                .map(|storage_key| {
                    index_key_for_slot_reader_with_membership_structural(
                        entity_tag,
                        index,
                        membership_program.as_ref(),
                        storage_key,
                        slots,
                    )
                })
                .transpose()?
                .flatten()
                .map(|key| key.to_raw()),
            None => None,
        };

        if old_key != new_key {
            if let Some(old_key) = old_key {
                let previous =
                    index_delta_kind_by_key.insert(old_key, PreparedIndexDeltaKind::IndexRemove);
                debug_assert!(
                    previous.is_none(),
                    "duplicate forward-index remove delta annotation for one key",
                );
            }
            if let Some(new_key) = new_key {
                let previous =
                    index_delta_kind_by_key.insert(new_key, PreparedIndexDeltaKind::IndexInsert);
                debug_assert!(
                    previous.is_none(),
                    "duplicate forward-index insert delta annotation for one key",
                );
            }
        }
    }

    Ok(index_delta_kind_by_key)
}

// Materialize one prepared row commit entirely from structural planning outputs.
fn materialize_prepared_row_commit(
    prepared: PreparedRowCommitMaterialization,
) -> Result<PreparedRowCommitOp, InternalError> {
    let PreparedRowCommitMaterialization {
        entity_path,
        index_plan,
        index_delta_kind_by_key,
        reverse_index_ops,
        data_store,
        data_key,
        data_value,
    } = prepared;

    // Phase 1: resolve index-store handles once from the already-planned apply targets.
    let mut index_stores = BTreeMap::new();
    for apply in &index_plan.apply {
        index_stores.insert(apply.index.store(), apply.store);
    }

    // Phase 2: decode planned commit-op payloads into mechanical index mutations.
    let mut index_ops = Vec::with_capacity(index_plan.commit_ops.len() + reverse_index_ops.len());
    for index_op in index_plan.commit_ops {
        let store = index_stores
            .get(index_op.store.as_str())
            .copied()
            .ok_or_else(|| {
                InternalError::executor_invariant(format!(
                    "commit prepare missing index store mapping: store='{}' entity='{}'",
                    index_op.store, entity_path
                ))
            })?;
        let key = decode_index_key(&index_op.key)?;
        let value = index_op
            .value
            .as_ref()
            .map(|bytes| decode_index_entry(bytes))
            .transpose()?;
        let delta_kind = index_delta_kind_by_key
            .get(&key)
            .copied()
            .unwrap_or(PreparedIndexDeltaKind::None);

        index_ops.push(PreparedIndexMutation {
            store,
            key,
            value,
            delta_kind,
        });
    }

    // Phase 3: append the already-prepared reverse-index mutations unchanged.
    index_ops.extend(reverse_index_ops);

    Ok(PreparedRowCommitOp {
        index_ops,
        data_store,
        data_key,
        data_value,
    })
}
