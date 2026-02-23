use crate::{
    db::{
        Db,
        codec::deserialize_row,
        commit::{
            CommitRowOp, PreparedIndexMutation, PreparedRowCommitOp,
            decode::{decode_data_key, decode_index_entry, decode_index_key},
        },
        data::{RawRow, decode_and_validate_entity_key},
        index::{IndexKey, plan_index_mutation_for_entity},
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
#[expect(clippy::too_many_lines)]
pub(in crate::db) fn prepare_row_commit_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    if op.entity_path != E::PATH {
        return Err(InternalError::store_corruption(format!(
            "commit marker entity path mismatch: expected '{}', found '{}'",
            E::PATH,
            op.entity_path
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
        old_entity.as_ref(),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let mut index_remove_count = 0usize;
    let mut index_insert_count = 0usize;
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
            if old_key.is_some() {
                index_remove_count = index_remove_count.saturating_add(1);
            }
            if new_key.is_some() {
                index_insert_count = index_insert_count.saturating_add(1);
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
        index_ops.push(PreparedIndexMutation { store, key, value });
    }
    let (reverse_index_ops, reverse_remove_count, reverse_insert_count) =
        prepare_reverse_relation_index_mutations_for_source::<E>(
            db,
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
        index_remove_count,
        index_insert_count,
        reverse_index_remove_count: reverse_remove_count,
        reverse_index_insert_count: reverse_insert_count,
    })
}
