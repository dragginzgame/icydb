//! Module: commit::prepare
//! Responsibility: decode commit-marker row ops into mechanical store mutations.
//! Does not own: marker persistence, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::marker -> commit::prepare -> commit::apply (one-way).

use crate::{
    db::{
        Db,
        codec::deserialize_row,
        commit::{
            CommitRowOp, PreparedIndexDeltaKind, PreparedIndexMutation, PreparedRowCommitOp,
            decode_data_key, decode_index_entry, decode_index_key,
        },
        data::{DataStore, RawDataKey, RawRow, decode_and_validate_entity_key},
        index::{
            IndexEntryReader, IndexMutationPlan, PrimaryRowReader, RawIndexKey,
            compile_index_membership_predicate, index_key_for_entity_with_membership,
            plan_index_mutation_for_entity,
        },
        relation::prepare_reverse_relation_index_mutations_for_source,
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, InternalError},
    traits::{EntityKind, EntityValue, Path},
};
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// PreparedRowCommitMaterialization
///
/// Generic-free commit-preparation payload after typed decode and planning.
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
/// Prepare a typed row-level commit op for one entity type.
///
/// This resolves store handles and index/data mutations so commit/recovery
/// apply can remain mechanical.
pub(in crate::db) fn prepare_row_commit_for_entity_with_readers<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &impl PrimaryRowReader<E>,
    index_reader: &impl IndexEntryReader<E>,
) -> Result<PreparedRowCommitOp, InternalError> {
    prepare_row_commit_for_entity_impl(db, op, row_reader, index_reader)
}

/// Prepare a typed row-level commit op against committed-store readers.
pub(in crate::db) fn prepare_row_commit_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let context = db.context::<E>();
    prepare_row_commit_for_entity_impl(db, op, &context, &context)
}

// Keep the full commit-preparation body out of the thin wrapper entrypoints so
// codegen does not clone the same logic into both prepare surfaces per entity.
#[inline(never)]
fn prepare_row_commit_for_entity_impl<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &dyn PrimaryRowReader<E>,
    index_reader: &dyn IndexEntryReader<E>,
) -> Result<PreparedRowCommitOp, InternalError> {
    // Phase 1: validate that marker metadata matches runtime entity authority.
    if op.entity_path != E::PATH {
        return Err(InternalError::store_corruption(format!(
            "commit marker entity path mismatch: expected '{}', found '{}'",
            E::PATH,
            op.entity_path
        )));
    }
    // No fingerprint fallback is allowed: replay must match exactly.
    let expected_schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
    if op.schema_fingerprint != expected_schema_fingerprint {
        return Err(InternalError::store_unsupported(format!(
            "commit marker schema fingerprint mismatch for entity '{}': marker={:?}, runtime={:?}",
            E::PATH,
            op.schema_fingerprint,
            expected_schema_fingerprint
        )));
    }

    // Phase 2: decode and validate key + before/after row payloads.
    let (raw_key, data_key) = decode_data_key(&op.key)?;
    let expected_key = data_key.try_key::<E>()?;

    let decode_entity_from_marker_row = |bytes: &[u8], label: &str| -> Result<E, InternalError> {
        RawRow::ensure_size(bytes)?;
        decode_and_validate_entity_key::<E, _, _, _, _>(
            expected_key,
            || deserialize_row::<E>(bytes),
            |err| {
                let message = format!("commit marker {label} row decode failed: {err}");
                if err.class() == ErrorClass::IncompatiblePersistedFormat {
                    InternalError::serialize_incompatible_persisted_format(message)
                } else {
                    InternalError::serialize_corruption(message)
                }
            },
            |expected, actual| {
                InternalError::store_corruption(format!(
                    "commit marker row key mismatch: expected {expected:?}, found {actual:?}"
                ))
            },
        )
    };

    let old_entity = op
        .before
        .as_ref()
        .map(|bytes| decode_entity_from_marker_row(bytes, "before"))
        .transpose()?;
    let new_pair = op
        .after
        .as_ref()
        .map(|bytes| {
            let row = RawRow::try_new(bytes.clone())?;
            let entity = decode_entity_from_marker_row(bytes, "after")?;
            Ok::<(RawRow, E), InternalError>((row, entity))
        })
        .transpose()?;

    if old_entity.is_none() && new_pair.is_none() {
        return Err(InternalError::store_corruption(
            "commit marker row op is a no-op (before/after both missing)",
        ));
    }

    // Phase 3: compute forward index mutation plan and classify delta kind per key.
    let index_plan = plan_index_mutation_for_entity::<E>(
        db,
        row_reader,
        index_reader,
        old_entity.as_ref(),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let index_delta_kind_by_key =
        annotate_forward_index_delta_kinds::<E>(old_entity.as_ref(), new_pair.as_ref())?;
    let reverse_index_ops = prepare_reverse_relation_index_mutations_for_source::<E>(
        db,
        index_reader,
        old_entity.as_ref(),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let data_store = db.with_store_registry(|reg| reg.try_get_store(E::Store::PATH))?;
    let data_value = new_pair.map(|(row, _)| row);

    materialize_prepared_row_commit(PreparedRowCommitMaterialization {
        entity_path: E::PATH,
        index_plan,
        index_delta_kind_by_key,
        reverse_index_ops,
        data_store: data_store.data_store(),
        data_key: raw_key,
        data_value,
    })
}

// Derive the forward-index delta-kind annotations needed by commit-window
// observability before the generic shell hands control to structural materialization.
fn annotate_forward_index_delta_kinds<E: EntityKind + EntityValue>(
    old_entity: Option<&E>,
    new_pair: Option<&(RawRow, E)>,
) -> Result<BTreeMap<RawIndexKey, PreparedIndexDeltaKind>, InternalError> {
    let mut index_delta_kind_by_key = BTreeMap::new();
    for index in E::INDEXES {
        let membership_program = compile_index_membership_predicate::<E>(index)?;
        let old_key = old_entity
            .map(|entity| {
                index_key_for_entity_with_membership(index, membership_program.as_ref(), entity)
            })
            .transpose()?
            .flatten()
            .map(|key| key.to_raw());
        let new_key = new_pair
            .map(|(_, new_entity)| {
                index_key_for_entity_with_membership(index, membership_program.as_ref(), new_entity)
            })
            .transpose()?
            .flatten()
            .map(|key| key.to_raw());

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
                crate::db::error::executor_invariant(format!(
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
