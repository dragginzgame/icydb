//! Module: commit::rebuild
//! Responsibility: rebuild secondary indexes from authoritative persisted rows.
//! Does not own: commit-marker replay, commit-marker persistence, or query planning.
//! Boundary: commit::recovery -> commit::rebuild -> commit::{prepare,apply} (one-way).

use crate::{
    db::{
        Db,
        commit::{CommitRowOp, PreparedIndexMutation},
        data::{DataKey, RawDataKey, RawRow},
        index::{IndexStore, RawIndexEntry, RawIndexKey},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
};

/// Rebuild all secondary indexes from authoritative data rows.
///
/// Invariant: row stores are the source of truth; index stores are fully
/// derived and can be recreated exactly from persisted rows.
pub(in crate::db) fn rebuild_secondary_indexes_from_rows(
    db: &Db<impl CanisterKind>,
) -> Result<(), InternalError> {
    if !db.has_runtime_hooks() {
        return Ok(());
    }

    // Phase 1: capture deterministic store ordering and rollback snapshots.
    let stores = sorted_store_handles(db);
    let snapshots = stores
        .iter()
        .map(|(_, handle)| IndexStoreSnapshot {
            handle: *handle,
            entries: handle.with_index(IndexStore::entries),
        })
        .collect::<Vec<_>>();

    // Phase 2: clear and rebuild all index entries from authoritative rows.
    let rebuild_result = rebuild_secondary_indexes_in_place(db, &stores);
    if let Err(err) = rebuild_result {
        // Phase 3: fail closed by restoring the exact pre-rebuild snapshot.
        restore_index_store_snapshots(snapshots);
        return Err(err);
    }

    Ok(())
}

///
/// IndexStoreSnapshot
///
/// Rollback snapshot for one index store captured before recovery rebuild.
/// This protects fail-closed recovery semantics if any rebuild step fails.
///

#[derive(Clone)]
struct IndexStoreSnapshot {
    handle: StoreHandle,
    entries: Vec<(RawIndexKey, RawIndexEntry)>,
}

/// Collect store handles in deterministic path order for stable rebuild behavior.
fn sorted_store_handles(db: &Db<impl CanisterKind>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    // StoreRegistry iteration is HashMap-backed and intentionally unordered.
    // Recovery semantics must remain deterministic, so sort explicitly by path.
    stores.sort_by(|(left, _), (right, _)| left.cmp(right));
    debug_assert!(
        stores.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "store registry iteration order must not affect semantic rebuild ordering",
    );

    stores
}

fn rebuild_secondary_indexes_in_place(
    db: &Db<impl CanisterKind>,
    stores: &[(&'static str, StoreHandle)],
) -> Result<(), InternalError> {
    // Phase 1: clear all index stores before deterministic full rebuild.
    for (_, handle) in stores {
        handle.with_index_mut(IndexStore::clear);
    }

    // Phase 2: rebuild index entries from authoritative row stores.
    for (store_path, handle) in stores {
        let rows = handle.with_data(|data_store| {
            data_store
                .iter()
                .map(|entry| (*entry.key(), entry.value()))
                .collect::<Vec<(RawDataKey, RawRow)>>()
        });

        for (raw_key, raw_row) in rows {
            let data_key = DataKey::try_from_raw(&raw_key).map_err(|err| {
                InternalError::store_corruption(format!(
                    "startup index rebuild failed: invalid data key in store '{store_path}' ({err})"
                ))
            })?;
            let hooks = db.runtime_hook_for_entity_name(data_key.entity_name().as_str())?;
            let row_op = CommitRowOp::new(
                hooks.entity_path,
                raw_key.as_bytes().to_vec(),
                None,
                Some(raw_row.as_bytes().to_vec()),
                (hooks.commit_schema_fingerprint)(),
            );
            let prepared = (hooks.prepare_row_commit)(db, &row_op).map_err(|err| {
                let message = format!(
                    "startup index rebuild failed: store='{}' entity='{}' ({})",
                    store_path, hooks.entity_path, err.message
                );

                err.with_message(message)
            })?;

            apply_index_mutations(prepared.index_ops);
        }
    }

    Ok(())
}

/// Apply index insert/remove operations exactly as prepared.
fn apply_index_mutations(index_ops: Vec<PreparedIndexMutation>) {
    for index_op in index_ops {
        index_op.store.with_borrow_mut(|store| {
            if let Some(value) = index_op.value {
                store.insert(index_op.key, value);
            } else {
                store.remove(&index_op.key);
            }
        });
    }
}

/// Restore every index store to its pre-rebuild snapshot after rebuild failure.
fn restore_index_store_snapshots(snapshots: Vec<IndexStoreSnapshot>) {
    for snapshot in snapshots {
        snapshot.handle.with_index_mut(|index_store| {
            index_store.clear();
            for (raw_key, raw_entry) in snapshot.entries {
                index_store.insert(raw_key, raw_entry);
            }
        });
    }
}
