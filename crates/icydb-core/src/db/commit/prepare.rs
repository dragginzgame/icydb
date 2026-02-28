use crate::{
    db::{
        Db,
        codec::deserialize_row,
        commit::{
            CommitRowOp, PreparedIndexDeltaKind, PreparedIndexMutation, PreparedRowCommitOp,
            UNSET_COMMIT_SCHEMA_FINGERPRINT, commit_schema_fingerprint_for_entity, decode_data_key,
            decode_index_entry, decode_index_key,
        },
        data::{RawRow, decode_and_validate_entity_key},
        index::{IndexEntryReader, IndexKey, PrimaryRowReader, plan_index_mutation_for_entity},
        relation::prepare_reverse_relation_index_mutations_for_source,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
};
use std::collections::BTreeMap;

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

#[expect(clippy::too_many_lines)]
fn prepare_row_commit_for_entity_impl<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
    row_reader: &impl PrimaryRowReader<E>,
    index_reader: &impl IndexEntryReader<E>,
) -> Result<PreparedRowCommitOp, InternalError> {
    if op.entity_path != E::PATH {
        return Err(InternalError::store_corruption(format!(
            "commit marker entity path mismatch: expected '{}', found '{}'",
            E::PATH,
            op.entity_path
        )));
    }
    let expected_schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
    if op.schema_fingerprint != UNSET_COMMIT_SCHEMA_FINGERPRINT
        && op.schema_fingerprint != expected_schema_fingerprint
    {
        return Err(InternalError::store_unsupported(format!(
            "commit marker schema fingerprint mismatch for entity '{}': marker={:?}, runtime={:?}",
            E::PATH,
            op.schema_fingerprint,
            expected_schema_fingerprint
        )));
    }

    let (raw_key, data_key) = decode_data_key(&op.key)?;
    let expected_key = data_key.try_key::<E>()?;

    let decode_entity_from_marker_row = |bytes: &[u8], label: &str| -> Result<E, InternalError> {
        RawRow::ensure_size(bytes)?;
        decode_and_validate_entity_key::<E, _, _, _, _>(
            expected_key,
            || deserialize_row::<E>(bytes),
            |err| {
                InternalError::serialize_corruption(format!(
                    "commit marker {label} row decode failed: {err}"
                ))
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

    let index_plan = plan_index_mutation_for_entity::<E>(
        db,
        row_reader,
        index_reader,
        old_entity.as_ref(),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let mut index_delta_kind_by_key = BTreeMap::new();
    for index in E::INDEXES {
        let old_key = old_entity
            .as_ref()
            .map(|entity| IndexKey::new(entity, index))
            .transpose()?
            .flatten()
            .map(|key| key.to_raw());
        let new_key = new_pair
            .as_ref()
            .map(|(_, new_entity)| IndexKey::new(new_entity, index))
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
    let mut index_stores = BTreeMap::new();
    for apply in &index_plan.apply {
        index_stores.insert(apply.index.store, apply.store);
    }

    let mut index_ops = Vec::with_capacity(index_plan.commit_ops.len());
    for index_op in index_plan.commit_ops {
        let store = index_stores
            .get(index_op.store.as_str())
            .copied()
            .ok_or_else(|| {
                InternalError::executor_invariant(format!(
                    "commit prepare missing index store mapping: store='{}' entity='{}'",
                    index_op.store,
                    E::PATH
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
    let reverse_index_ops = prepare_reverse_relation_index_mutations_for_source::<E>(
        db,
        index_reader,
        old_entity.as_ref(),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    index_ops.extend(reverse_index_ops);

    let data_store = db.with_store_registry(|reg| reg.try_get_store(E::Store::PATH))?;
    let data_value = new_pair.map(|(row, _)| row);

    Ok(PreparedRowCommitOp {
        index_ops,
        data_store: data_store.data_store(),
        data_key: raw_key,
        data_value,
    })
}
