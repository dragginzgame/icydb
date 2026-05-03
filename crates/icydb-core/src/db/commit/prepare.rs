//! Module: commit::prepare
//! Responsibility: decode commit-marker row ops into mechanical store mutations.
//! Does not own: marker persistence, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::marker -> commit::prepare -> commit::apply (one-way).

use crate::{
    db::{
        Db,
        commit::{
            CommitIndexOp, CommitRowOp, CommitSchemaFingerprint, PreparedIndexMutation,
            PreparedRowCommitOp,
        },
        data::{
            CanonicalRow, CanonicalSlotReader, DataKey, DataStore, RawDataKey, RawRow, StorageKey,
            StructuralSlotReader, canonical_row_from_structural_slot_reader,
        },
        index::{
            IndexDelta, IndexDeltaGroup, IndexEntry, IndexMembershipDelta, IndexMutationPlan,
            IndexPlanReadView, RawIndexEntry, RawIndexKey, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, plan_index_mutation_for_slot_reader_structural,
        },
        relation::{
            ReverseRelationSourceInfo,
            prepare_reverse_relation_index_mutations_for_source_slot_readers,
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, InternalError},
    metrics::sink::{MetricsEvent, record},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, thread::LocalKey};

///
/// CommitPrepareAuthority
///
/// Resolved authority needed by nongeneric commit-preparation stages.
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
    /// Lower one entity type into the resolved authority using a caller-cached schema fingerprint.
    const fn for_type_with_schema_fingerprint<E>(
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self
    where
        E: EntityKind + Path,
    {
        Self {
            entity_path: E::PATH,
            entity_tag: E::ENTITY_TAG,
            schema_fingerprint,
            data_store_path: E::Store::PATH,
            relation_source: ReverseRelationSourceInfo::for_type::<E>(),
            model: E::MODEL,
        }
    }
}

///
/// CommitInputs
///
/// Structural commit inputs decoded before forward-index planning runs.
///

struct CommitInputs {
    raw_key: RawDataKey,
    data_key: DataKey,
    old_row: Option<RawRow>,
    new_row: Option<RawRow>,
}

impl CommitInputs {
    // Build the canonical schema-fingerprint mismatch mapping for structural commit inputs.
    fn schema_fingerprint_mismatch(
        entity_path: &str,
        marker: crate::db::commit::CommitSchemaFingerprint,
        runtime: crate::db::commit::CommitSchemaFingerprint,
    ) -> InternalError {
        InternalError::store_unsupported(format!(
            "commit marker schema fingerprint mismatch for entity '{entity_path}': marker={marker:?}, runtime={runtime:?}",
        ))
    }
}

///
/// DecodedCommitRows
///
/// Reusable structural slot readers for one commit-marker row transition.
/// This keeps commit-preflight row decoding on one owned pass so validation and
/// forward-index planning do not each rebuild the same slot-reader state.
///

struct DecodedCommitRows<'a> {
    old_slots: Option<StructuralSlotReader<'a>>,
    new_slots: Option<StructuralSlotReader<'a>>,
}

///
/// CommitIndexPlanReadView
///
/// Commit-owned adapter that resolves model indexes to concrete stores before
/// delegating reads to the active preflight reader view. Keeping this adapter
/// here prevents index planning from depending on registry or executor state.
///

struct CommitIndexPlanReadView<'a, C: CanisterKind> {
    db: &'a Db<C>,
    row_reader: &'a dyn StructuralPrimaryRowReader,
    index_reader: &'a dyn StructuralIndexEntryReader,
}

impl<C> CommitIndexPlanReadView<'_, C>
where
    C: CanisterKind,
{
    // Resolve the store handle for one model-owned index definition.
    fn index_store(
        &self,
        index: &crate::model::index::IndexModel,
    ) -> Result<&'static LocalKey<RefCell<crate::db::index::IndexStore>>, InternalError> {
        self.db
            .with_store_registry(|registry| registry.try_get_store(index.store()))
            .map(|store| store.index_store())
    }
}

impl<C> IndexPlanReadView for CommitIndexPlanReadView<'_, C>
where
    C: CanisterKind,
{
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        self.row_reader.read_primary_row_structural(key)
    }

    fn read_index_entry(
        &self,
        index: &crate::model::index::IndexModel,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        let index_store = self.index_store(index)?;

        self.index_reader
            .read_index_entry_structural(index_store, key)
    }

    fn read_index_keys_in_raw_range(
        &self,
        entity_path: &'static str,
        entity_tag: EntityTag,
        index: &crate::model::index::IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        let index_store = self.index_store(index)?;

        self.index_reader.read_index_keys_in_raw_range_structural(
            entity_path,
            entity_tag,
            index_store,
            index,
            bounds,
            limit,
        )
    }
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
    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
        db,
        op,
        row_reader,
        index_reader,
        commit_schema_fingerprint_for_entity::<E>(),
    )
}

/// Prepare a typed row-level commit op against nongeneric structural readers
/// while reusing a caller-resolved schema fingerprint.
pub(in crate::db) fn prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint<
    E: EntityKind + EntityValue,
>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    schema_fingerprint: CommitSchemaFingerprint,
) -> Result<PreparedRowCommitOp, InternalError> {
    prepare_row_commit_for_entity_impl(
        db,
        op,
        CommitPrepareAuthority::for_type_with_schema_fingerprint::<E>(schema_fingerprint),
        row_reader,
        index_reader,
    )
}

// Decode both optional commit-marker row images through the structural row
// boundary once so malformed fields fail closed before index planning.
fn decode_commit_marker_rows_for_preflight<'a>(
    data_key: &DataKey,
    before: Option<&'a RawRow>,
    after: Option<&'a RawRow>,
    model: &'static EntityModel,
) -> Result<DecodedCommitRows<'a>, InternalError> {
    let old_slots = decode_optional_commit_marker_row_slots(data_key, before, "before", model)?;
    let new_slots = decode_optional_commit_marker_row_slots(data_key, after, "after", model)?;

    Ok(DecodedCommitRows {
        old_slots,
        new_slots,
    })
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
    // Phase 1: resolve nongeneric marker authority before any model-driven row
    // decode runs so miswired hooks fail on path/schema mismatch first.
    let structural = prepare_row_commit_structural_inputs(op, &authority)?;

    // Phase 2: decode the persisted row images once through the structural
    // slot-reader boundary before any forward-index planning runs.
    let (decoded, forward_index_ops) = {
        let mut decoded = decode_commit_marker_rows_for_preflight(
            &structural.data_key,
            structural.old_row.as_ref(),
            structural.new_row.as_ref(),
            authority.model,
        )?;

        // Phase 3: derive forward index work from the already validated
        // structural rows when the entity owns secondary indexes.
        let index_plan = if authority.model.indexes().is_empty() {
            empty_forward_index_plan()
        } else {
            prepare_forward_index_commit_leaf(
                db,
                &authority,
                row_reader,
                index_reader,
                &structural.data_key,
                &mut decoded,
            )?
        };
        let forward_index_ops = materialize_forward_index_commit_ops(
            db,
            index_plan,
            index_reader,
            authority.entity_path,
        )?;

        (decoded, forward_index_ops)
    };

    let reverse_index_ops = prepare_reverse_relation_index_mutations_for_source_slot_readers(
        db,
        index_reader,
        authority.relation_source,
        authority.model,
        structural.data_key.storage_key(),
        decoded.old_slots.as_ref(),
        decoded.new_slots.as_ref(),
    )?;
    let data_value = decoded
        .new_slots
        .as_ref()
        .map(canonical_row_from_structural_slot_reader)
        .transpose()?;

    finalize_row_commit_structural(
        db,
        authority,
        structural.raw_key,
        forward_index_ops,
        reverse_index_ops,
        data_value,
    )
}

// Return one empty forward-index plan when the entity has no secondary indexes.
const fn empty_forward_index_plan() -> IndexMutationPlan {
    IndexMutationPlan::new(Vec::new())
}

// Decode only the structural row views required for forward-index planning and
// produce structural-ready forward-index outputs.
fn prepare_forward_index_commit_leaf<C>(
    db: &Db<C>,
    authority: &CommitPrepareAuthority,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    data_key: &DataKey,
    decoded: &mut DecodedCommitRows<'_>,
) -> Result<IndexMutationPlan, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let storage_key = data_key.storage_key();

    let read_view = CommitIndexPlanReadView {
        db,
        row_reader,
        index_reader,
    };

    match plan_index_mutation_for_slot_reader_structural(
        authority.entity_path,
        authority.entity_tag,
        authority.model,
        &read_view,
        decoded.old_slots.as_ref().map(|_| storage_key),
        decoded
            .old_slots
            .as_mut()
            .map(|slots| slots as &mut dyn CanonicalSlotReader),
        decoded.new_slots.as_ref().map(|_| storage_key),
        decoded
            .new_slots
            .as_mut()
            .map(|slots| slots as &mut dyn CanonicalSlotReader),
    ) {
        Ok(index_plan) => Ok(index_plan),
        Err(err) => {
            if let Some(entity_path) = err.unique_violation_entity_path() {
                record(MetricsEvent::UniqueViolation { entity_path });
            }

            Err(err.into_internal_error())
        }
    }
}

// Decode one optional commit-marker row into one validated structural slot
// reader for forward-index planning.
fn decode_optional_commit_marker_row_slots<'a>(
    data_key: &DataKey,
    row: Option<&'a RawRow>,
    label: &str,
    model: &'static EntityModel,
) -> Result<Option<StructuralSlotReader<'a>>, InternalError> {
    row.map(|row| decode_commit_marker_structural_slots(data_key, row, label, model))
        .transpose()
}

// Decode one commit-marker row into one validated slot reader so both
// hardening and forward-index planning share the same structural row boundary.
fn decode_commit_marker_structural_slots<'a>(
    data_key: &DataKey,
    row: &'a RawRow,
    label: &str,
    model: &'static EntityModel,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    let slots = StructuralSlotReader::from_raw_row(row, model).map_err(|err| {
        let message = format!("commit marker {label} row: {err}");
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
) -> Result<CommitInputs, InternalError> {
    if op.entity_path != authority.entity_path {
        return Err(InternalError::store_corruption(format!(
            "commit marker entity path mismatch: expected '{}', found '{}'",
            authority.entity_path, op.entity_path,
        )));
    }
    if op.schema_fingerprint != authority.schema_fingerprint {
        return Err(CommitInputs::schema_fingerprint_mismatch(
            authority.entity_path,
            op.schema_fingerprint,
            authority.schema_fingerprint,
        ));
    }

    let raw_key = op.key;
    let data_key = DataKey::try_from_raw(&raw_key).map_err(|_| {
        InternalError::store_corruption("commit marker row op key decode: invalid primary key")
    })?;
    let old_row = op
        .before
        .as_ref()
        .map(|bytes| RawRow::from_untrusted_bytes(bytes.clone()))
        .transpose()?;
    let new_row = op
        .after
        .as_ref()
        .map(|bytes| RawRow::from_untrusted_bytes(bytes.clone()))
        .transpose()?;

    if old_row.is_none() && new_row.is_none() {
        return Err(InternalError::store_corruption(
            "commit marker row op is a no-op (before/after both missing)",
        ));
    }

    Ok(CommitInputs {
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
    authority: CommitPrepareAuthority,
    data_key: RawDataKey,
    forward_index_ops: Vec<CommitIndexOp>,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_value: Option<CanonicalRow>,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let data_store = db.with_store_registry(|reg| reg.try_get_store(authority.data_store_path))?;

    Ok(materialize_prepared_row_commit(
        forward_index_ops,
        reverse_index_ops,
        data_store.data_store(),
        data_key,
        data_value,
    ))
}

// Materialize one prepared row commit entirely from structural planning outputs.
fn materialize_prepared_row_commit(
    forward_index_ops: Vec<CommitIndexOp>,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_store: &'static LocalKey<RefCell<DataStore>>,
    data_key: RawDataKey,
    data_value: Option<CanonicalRow>,
) -> PreparedRowCommitOp {
    // Phase 1: lower planned commit ops into mechanical index mutations.
    let mut index_ops = Vec::with_capacity(forward_index_ops.len() + reverse_index_ops.len());
    index_ops.extend(
        forward_index_ops
            .into_iter()
            .map(PreparedIndexMutation::from),
    );

    // Phase 2: append the already-prepared reverse-index mutations unchanged.
    index_ops.extend(reverse_index_ops);

    PreparedRowCommitOp {
        index_ops,
        data_store,
        data_key,
        data_value,
    }
}

// Convert index-domain deltas into commit-owned raw index operations. This is
// the first layer that knows both the active preflight reader view and the
// commit op shape.
fn materialize_forward_index_commit_ops<C>(
    db: &Db<C>,
    index_plan: IndexMutationPlan,
    index_reader: &dyn StructuralIndexEntryReader,
    entity_path: &'static str,
) -> Result<Vec<CommitIndexOp>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let mut commit_ops = Vec::with_capacity(index_plan.groups.len().saturating_mul(2));

    for group in index_plan.groups {
        build_commit_ops_for_index_group(&mut commit_ops, db, index_reader, entity_path, group)?;
    }

    Ok(commit_ops)
}

// Materialize one per-index delta group. The logic mirrors the previous
// index-owned commit-op builder so same-key and different-key transitions keep
// their exact raw-entry behavior and deterministic ordering.
fn build_commit_ops_for_index_group<C>(
    commit_ops: &mut Vec<CommitIndexOp>,
    db: &Db<C>,
    index_reader: &dyn StructuralIndexEntryReader,
    entity_path: &'static str,
    group: IndexDeltaGroup,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    let mut remove_delta = None;
    let mut insert_delta = None;
    let index_store = db
        .with_store_registry(|registry| registry.try_get_store(group.index_store))
        .map(|store| store.index_store())?;

    for delta in group.deltas {
        match delta {
            IndexDelta::Remove(delta) => remove_delta = Some(delta),
            IndexDelta::Insert(delta) => insert_delta = Some(delta),
        }
    }

    let old_entry = load_existing_index_entry_for_commit(
        index_reader,
        index_store,
        &group.index_fields,
        remove_delta.as_ref(),
        entity_path,
    )?;

    let new_entry = if remove_delta
        .as_ref()
        .zip(insert_delta.as_ref())
        .is_some_and(|(old_delta, new_delta)| old_delta.key == new_delta.key)
    {
        None
    } else {
        load_existing_index_entry_for_commit(
            index_reader,
            index_store,
            &group.index_fields,
            insert_delta.as_ref(),
            entity_path,
        )?
    };

    build_commit_ops_for_index_delta_pair(
        commit_ops,
        index_store,
        entity_path,
        &group.index_fields,
        remove_delta,
        insert_delta,
        old_entry,
        new_entry,
    )
}

// Load and decode the current raw index entry for one membership delta.
fn load_existing_index_entry_for_commit(
    index_reader: &dyn StructuralIndexEntryReader,
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    index_fields: &str,
    delta: Option<&IndexMembershipDelta>,
    entity_path: &'static str,
) -> Result<Option<IndexEntry>, InternalError> {
    let Some(delta) = delta else {
        return Ok(None);
    };

    let raw_key = delta.key.to_raw();

    index_reader
        .read_index_entry_structural(store, &raw_key)?
        .map(|raw_entry| {
            raw_entry.try_decode().map_err(|err| {
                InternalError::structural_index_entry_corruption(entity_path, index_fields, err)
            })
        })
        .transpose()
}

// Compute commit-time index operations for one old/new membership pair.
#[expect(clippy::too_many_arguments)]
fn build_commit_ops_for_index_delta_pair(
    commit_ops: &mut Vec<CommitIndexOp>,
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    entity_path: &str,
    fields: &str,
    remove_delta: Option<IndexMembershipDelta>,
    insert_delta: Option<IndexMembershipDelta>,
    old_entry: Option<IndexEntry>,
    new_entry: Option<IndexEntry>,
) -> Result<(), InternalError> {
    // Phase 1: same-key transitions collapse into one entry mutation.
    if remove_delta
        .as_ref()
        .zip(insert_delta.as_ref())
        .is_some_and(|(old_delta, new_delta)| old_delta.key == new_delta.key)
    {
        if let Some(insert_delta) = insert_delta {
            let old_primary_key = remove_delta.map(|delta| delta.primary_key);
            let mut entry = old_entry.unwrap_or_else(|| IndexEntry::new(insert_delta.primary_key));
            if let Some(old_primary_key) = old_primary_key {
                entry.remove(old_primary_key);
            }
            entry.insert(insert_delta.primary_key);

            push_commit_op_for_index_entry(
                commit_ops,
                store,
                entity_path,
                fields,
                insert_delta.key.to_raw(),
                Some(entry),
                CommitIndexOp::unchanged,
            )?;
        }

        return Ok(());
    }

    // Phase 2: different-key transitions can touch at most two keys. Preserve
    // deterministic key order without the general BTreeMap machinery.
    let mut first: Option<(RawIndexKey, Option<IndexEntry>, CommitIndexOpBuilder)> = None;
    let mut second: Option<(RawIndexKey, Option<IndexEntry>, CommitIndexOpBuilder)> = None;

    if let Some(remove_delta) = remove_delta {
        let after = old_entry.map(|mut entry| {
            entry.remove(remove_delta.primary_key);
            entry
        });
        let after = after.filter(|entry| !entry.is_empty());
        insert_commit_candidate(
            &mut first,
            &mut second,
            remove_delta.key.to_raw(),
            after,
            CommitIndexOp::index_remove,
        );
    }

    if let Some(insert_delta) = insert_delta {
        let mut entry = new_entry.unwrap_or_else(|| IndexEntry::new(insert_delta.primary_key));
        entry.insert(insert_delta.primary_key);
        insert_commit_candidate(
            &mut first,
            &mut second,
            insert_delta.key.to_raw(),
            Some(entry),
            CommitIndexOp::index_insert,
        );
    }

    if let Some((raw_key, entry, build_commit_op)) = first {
        push_commit_op_for_index_entry(
            commit_ops,
            store,
            entity_path,
            fields,
            raw_key,
            entry,
            build_commit_op,
        )?;
    }
    if let Some((raw_key, entry, build_commit_op)) = second {
        push_commit_op_for_index_entry(
            commit_ops,
            store,
            entity_path,
            fields,
            raw_key,
            entry,
            build_commit_op,
        )?;
    }

    Ok(())
}

/// Insert one touched key into the small fixed-size ordered candidate set.
fn insert_commit_candidate(
    first: &mut Option<(RawIndexKey, Option<IndexEntry>, CommitIndexOpBuilder)>,
    second: &mut Option<(RawIndexKey, Option<IndexEntry>, CommitIndexOpBuilder)>,
    raw_key: RawIndexKey,
    entry: Option<IndexEntry>,
    build_commit_op: CommitIndexOpBuilder,
) {
    match first {
        None => *first = Some((raw_key, entry, build_commit_op)),
        Some((first_key, _, _)) if raw_key < *first_key => {
            *second = first.take();
            *first = Some((raw_key, entry, build_commit_op));
        }
        _ => *second = Some((raw_key, entry, build_commit_op)),
    }
}

type CommitIndexOpBuilder = fn(
    &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    RawIndexKey,
    Option<RawIndexEntry>,
) -> CommitIndexOp;

// Encode one touched index entry into one deterministic commit operation.
fn push_commit_op_for_index_entry(
    commit_ops: &mut Vec<CommitIndexOp>,
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    entity_path: &str,
    fields: &str,
    raw_key: RawIndexKey,
    entry: Option<IndexEntry>,
    build_commit_op: CommitIndexOpBuilder,
) -> Result<(), InternalError> {
    let value = if let Some(entry) = entry {
        let raw = RawIndexEntry::try_from(&entry)
            .map_err(|err| err.into_commit_internal_error(entity_path, fields))?;
        Some(raw)
    } else {
        None
    };

    commit_ops.push(build_commit_op(store, raw_key, value));

    Ok(())
}
