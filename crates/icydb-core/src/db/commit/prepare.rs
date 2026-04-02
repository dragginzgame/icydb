//! Module: commit::prepare
//! Responsibility: decode commit-marker row ops into mechanical store mutations.
//! Does not own: marker persistence, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::marker -> commit::prepare -> commit::apply (one-way).

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, CommitSchemaFingerprint, PreparedIndexMutation, PreparedRowCommitOp,
        },
        data::{
            CanonicalRow, CanonicalSlotReader, DataKey, DataStore, RawDataKey, RawRow,
            StructuralSlotReader, canonical_row_from_structural_slot_reader,
        },
        index::{
            IndexEntryReader, IndexMutationPlan, PrimaryRowReader, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, plan_index_mutation_for_slot_reader_structural,
        },
        relation::{
            ReverseRelationSourceInfo,
            prepare_reverse_relation_index_mutations_for_source_slot_readers,
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, InternalError},
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue, Path},
    types::EntityTag,
};
use std::{cell::RefCell, thread::LocalKey};

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

/// Prepare a typed row-level commit op against typed preflight readers while
/// reusing a caller-resolved schema fingerprint.
pub(in crate::db) fn prepare_row_commit_for_entity_with_readers_and_schema_fingerprint<E, R, I>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &R,
    index_reader: &I,
    schema_fingerprint: CommitSchemaFingerprint,
) -> Result<PreparedRowCommitOp, InternalError>
where
    E: EntityKind + EntityValue,
    R: PrimaryRowReader<E> + StructuralPrimaryRowReader,
    I: IndexEntryReader<E> + StructuralIndexEntryReader,
{
    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
        db,
        op,
        row_reader,
        index_reader,
        schema_fingerprint,
    )
}

/// Prepare a typed row-level commit op against committed-store readers.
pub(in crate::db) fn prepare_row_commit_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let context = db.context::<E>();
    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
        db,
        op,
        &context,
        &context,
        commit_schema_fingerprint_for_entity::<E>(),
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
    let (decoded, index_plan) = {
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

        (decoded, index_plan)
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
        index_plan,
        reverse_index_ops,
        data_value,
    )
}

// Return one empty forward-index plan when the entity has no secondary indexes.
const fn empty_forward_index_plan() -> IndexMutationPlan {
    IndexMutationPlan {
        commit_ops: Vec::new(),
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
    decoded: &mut DecodedCommitRows<'_>,
) -> Result<IndexMutationPlan, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let storage_key = data_key.storage_key();

    plan_index_mutation_for_slot_reader_structural(
        db,
        authority.entity_path,
        authority.entity_tag,
        authority.model,
        row_reader,
        index_reader,
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
    )
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
        InternalError::store_corruption(format!("commit marker {label} row key mismatch: {err}",))
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
    index_plan: IndexMutationPlan,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_value: Option<CanonicalRow>,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let data_store = db.with_store_registry(|reg| reg.try_get_store(authority.data_store_path))?;

    Ok(materialize_prepared_row_commit(
        index_plan,
        reverse_index_ops,
        data_store.data_store(),
        data_key,
        data_value,
    ))
}

// Materialize one prepared row commit entirely from structural planning outputs.
fn materialize_prepared_row_commit(
    index_plan: IndexMutationPlan,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_store: &'static LocalKey<RefCell<DataStore>>,
    data_key: RawDataKey,
    data_value: Option<CanonicalRow>,
) -> PreparedRowCommitOp {
    // Phase 1: lower planned commit ops into mechanical index mutations.
    let mut index_ops = Vec::with_capacity(index_plan.commit_ops.len() + reverse_index_ops.len());
    index_ops.extend(
        index_plan
            .commit_ops
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
