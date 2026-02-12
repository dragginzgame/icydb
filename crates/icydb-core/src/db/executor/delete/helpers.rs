use crate::{
    db::{
        CommitDataOp, CommitIndexOp,
        executor::{
            ExecutorError,
            delete::DeleteExecutor,
            mutation::{
                IndexEntryPresencePolicy, PreparedDataRollback, PreparedIndexRollback,
                prepare_index_ops,
            },
        },
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
        },
        store::{DataKey, DataRow, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::*,
    traits::{EntityKind, EntityValue, Path, Storable},
};
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// IndexPlan
/// Prevalidated handle to an index store used during commit planning.
///

pub(super) struct IndexPlan {
    pub(super) index: &'static IndexModel,
    pub(super) store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// DeleteRow
/// Row wrapper used during delete planning and execution.
///

pub(super) struct DeleteRow<E>
where
    E: EntityKind,
{
    pub(super) key: DataKey,
    pub(super) raw: Option<RawRow>,
    pub(super) entity: E,
}

impl<E: EntityKind> crate::db::query::plan::logical::PlanRow<E> for DeleteRow<E> {
    fn entity(&self) -> &E {
        &self.entity
    }
}

impl<E> DeleteExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve commit marker index ops into stores and rollback bytes before committing.
    #[expect(clippy::type_complexity)]
    pub(super) fn prepare_index_delete_ops(
        plans: &[IndexPlan],
        ops: &[CommitIndexOp],
    ) -> Result<
        (
            Vec<&'static LocalKey<RefCell<IndexStore>>>,
            Vec<PreparedIndexRollback>,
        ),
        InternalError,
    > {
        // Resolve store handles once so commit-time apply is mechanical.
        let mut stores = BTreeMap::new();
        for plan in plans {
            stores.insert(plan.index.store, plan.store);
        }
        prepare_index_ops(
            &stores,
            ops,
            E::PATH,
            "delete",
            IndexEntryPresencePolicy::RequireExisting,
        )
    }

    // Resolve commit marker data ops and capture rollback rows before committing.
    pub(super) fn prepare_data_delete_ops(
        ops: &[CommitDataOp],
        rollback_rows: &BTreeMap<RawDataKey, RawRow>,
    ) -> Result<Vec<PreparedDataRollback>, InternalError> {
        let mut rollbacks = Vec::with_capacity(ops.len());

        // Validate marker ops and map them to rollback rows.
        for op in ops {
            if op.store != E::DataStore::PATH {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker references unexpected data store '{}' ({})",
                        op.store,
                        E::PATH
                    ),
                ));
            }
            if op.key.len() != DataKey::STORED_SIZE_USIZE {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker data key length {} does not match {} ({})",
                        op.key.len(),
                        DataKey::STORED_SIZE_USIZE,
                        E::PATH
                    ),
                ));
            }
            if op.value.is_some() {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker delete includes data payload ({})", E::PATH),
                ));
            }

            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let raw_row = rollback_rows.get(&raw_key).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker data op missing rollback row ({})", E::PATH),
                )
            })?;
            rollbacks.push(PreparedDataRollback {
                key: raw_key,
                value: Some(raw_row.clone()),
            });
        }

        Ok(rollbacks)
    }

    pub(super) fn build_index_plans(&self) -> Result<Vec<IndexPlan>, InternalError> {
        E::INDEXES
            .iter()
            .map(|index| {
                let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
                Ok(IndexPlan { index, store })
            })
            .collect()
    }

    // Build commit-time index ops and count entity-level removals for metrics.
    #[expect(clippy::too_many_lines)]
    pub(super) fn build_index_removal_ops(
        plans: &[IndexPlan],
        entities: &[&E],
    ) -> Result<(Vec<CommitIndexOp>, usize), InternalError> {
        let mut ops = Vec::new();
        let mut removed = 0usize;

        // Process each index independently to compute its resulting mutations.
        for plan in plans {
            let fields = plan.index.fields.join(", ");

            // Map raw index keys → updated entry (or None if fully removed).
            let mut entries: BTreeMap<RawIndexKey, Option<IndexEntry<E>>> = BTreeMap::new();

            // Fold entity deletions into per-key index entry updates.
            for entity in entities {
                let Some(key) = IndexKey::new(*entity, plan.index)? else {
                    continue;
                };
                let raw_key = key.to_raw();
                let entity_id = entity.id().key();

                // Lazily load and decode the existing index entry once per key.
                let entry = match entries.entry(raw_key) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        let decoded = plan.store.with_borrow(|s| {
                            s.get(&raw_key)
                                .map(|raw| {
                                    raw.try_decode::<E>().map_err(|err| {
                                        ExecutorError::corruption(
                                            ErrorOrigin::Index,
                                            format!(
                                                "index corrupted: {} ({}) -> {}",
                                                E::PATH,
                                                fields,
                                                err
                                            ),
                                        )
                                    })
                                })
                                .transpose()
                        })?;
                        slot.insert(decoded)
                    }

                    std::collections::btree_map::Entry::Occupied(slot) => slot.into_mut(),
                };

                // Prevalidate membership to keep commit-phase mutations infallible.
                let Some(e) = entry.as_ref() else {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::missing_key(raw_key, entity_id),
                        ),
                    )
                    .into());
                };

                if plan.index.unique && e.len() > 1 {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::NonUniqueEntry { keys: e.len() },
                        ),
                    )
                    .into());
                }

                if !e.contains(entity_id) {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::missing_key(raw_key, entity_id),
                        ),
                    )
                    .into());
                }
                removed = removed.saturating_add(1);

                // Remove this entity’s key from the index entry.
                if let Some(e) = entry.as_mut() {
                    e.remove(entity_id);
                    if e.is_empty() {
                        *entry = None;
                    }
                }
            }

            // Emit commit ops for each touched index key.
            for (raw_key, entry) in entries {
                let value = if let Some(entry) = entry {
                    let raw = RawIndexEntry::try_from(&entry).map_err(|err| match err {
                        crate::db::index::entry::IndexEntryEncodeError::TooManyKeys { keys } => {
                            InternalError::new(
                                ErrorClass::Corruption,
                                ErrorOrigin::Index,
                                format!(
                                    "index corrupted: {} ({}) -> {}",
                                    E::PATH,
                                    fields,
                                    IndexEntryCorruption::TooManyKeys { count: keys }
                                ),
                            )
                        }
                        crate::db::index::entry::IndexEntryEncodeError::KeyEncoding(err) => {
                            InternalError::new(
                                ErrorClass::Unsupported,
                                ErrorOrigin::Index,
                                format!(
                                    "index key encoding failed: {} ({fields}) -> {err}",
                                    E::PATH
                                ),
                            )
                        }
                    })?;

                    Some(raw.as_bytes().to_vec())
                } else {
                    // None means the index entry is fully removed.
                    None
                };

                ops.push(CommitIndexOp {
                    store: plan.index.store.to_string(),
                    key: raw_key.as_bytes().to_vec(),
                    value,
                });
            }
        }

        Ok((ops, removed))
    }
}

pub(super) fn decode_rows<E: EntityKind + EntityValue>(
    rows: Vec<DataRow>,
) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let dk_for_err = dk.clone();
            let entity = raw.try_decode::<E>().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Serialize,
                    format!("failed to deserialize row: {dk_for_err} ({err})"),
                )
            })?;

            let expected = dk.try_key::<E>()?;
            let actual = entity.id().key();
            if expected != actual {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Store,
                    format!("row key mismatch: expected {expected:?}, found {actual:?}"),
                )
                .into());
            }

            Ok(DeleteRow {
                key: dk,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}
