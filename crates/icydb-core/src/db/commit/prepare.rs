use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedIndexMutation, PreparedRowCommitOp,
            decode::{decode_data_key, decode_index_entry, decode_index_key},
        },
        data::{DataKey, RawRow},
        decode::decode_entity_with_expected_key,
        index::{IndexKey, plan_index_mutation_for_entity},
        relation::prepare_reverse_relation_index_mutations_for_source,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
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
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!(
                "commit marker entity path mismatch: expected '{}', found '{}'",
                E::PATH,
                op.entity_path
            ),
        ));
    }

    let raw_key = decode_data_key(&op.key)?;
    let data_key = DataKey::try_from_raw(&raw_key).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!("commit marker data key corrupted: {err}"),
        )
    })?;
    let expected_key = data_key.try_key::<E>()?;

    let decode_entity = |bytes: &[u8], label: &str| -> Result<(RawRow, E), InternalError> {
        let row = RawRow::try_new(bytes.to_vec())?;
        let entity = decode_entity_with_expected_key::<E, _, _, _, _>(
            expected_key,
            || row.try_decode::<E>(),
            |err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Serialize,
                    format!("commit marker {label} row decode failed: {err}"),
                )
            },
            |expected, actual| {
                Ok(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker row key mismatch: expected {expected:?}, found {actual:?}"
                    ),
                ))
            },
        )?;

        Ok((row, entity))
    };

    let old_pair = op
        .before
        .as_ref()
        .map(|bytes| decode_entity(bytes, "before"))
        .transpose()?;
    let new_pair = op
        .after
        .as_ref()
        .map(|bytes| decode_entity(bytes, "after"))
        .transpose()?;

    if old_pair.is_none() && new_pair.is_none() {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            "commit marker row op is a no-op (before/after both missing)",
        ));
    }

    let index_plan = plan_index_mutation_for_entity::<E>(
        db,
        old_pair.as_ref().map(|(_, entity)| entity),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let mut index_remove_count = 0usize;
    let mut index_insert_count = 0usize;
    for index in E::INDEXES {
        let old_key = old_pair
            .as_ref()
            .map(|(_, old_entity)| IndexKey::new(old_entity, index))
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
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!(
                        "missing index store '{}' for entity '{}'",
                        index_op.store,
                        E::PATH
                    ),
                )
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
            old_pair.as_ref().map(|(_, entity)| entity),
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
