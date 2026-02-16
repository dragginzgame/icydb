//! System-level commit recovery.
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing or rolling back a previously started
//! commit before any new operation proceeds.
//!
//! Important semantic notes:
//! - Recovery runs once at startup.
//! - Read and write paths both perform a cheap marker check and replay if needed.
//! - Reads must not proceed while a persisted partial commit marker is present.
//!
//! Invocation from read or mutation entrypoints is permitted only as an
//! unconditional invariant-restoration step. Recovery must not be
//! interleaved with read logic or mutation planning/apply phases.

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedIndexMutation, PreparedRowCommitOp,
            rollback_prepared_row_ops_reverse, snapshot_row_rollback,
            store::{commit_marker_present_fast, with_commit_store},
        },
        index::{RawIndexEntry, RawIndexKey},
        store::{DataKey, RawDataKey, RawRow, StoreHandle},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use std::sync::OnceLock;

static RECOVERED: OnceLock<()> = OnceLock::new();

/// Ensure global database invariants are restored before proceeding.
///
/// This function performs a **system recovery step**:
/// - It completes or rolls back any previously started commit.
/// - It leaves the database in a fully consistent state on return.
///
/// This function is:
/// - **Not part of mutation atomicity**
/// - **Mandatory before read execution**
/// - **Not conditional on read semantics**
///
/// It may be invoked at operation boundaries (including read or mutation
/// entrypoints), but must always complete **before** any operation-specific
/// planning, validation, or apply phase begins.
pub fn ensure_recovered(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    if RECOVERED.get().is_none() {
        return perform_recovery(db);
    }

    if commit_marker_present_fast()? {
        return perform_recovery(db);
    }

    Ok(())
}

/// Ensure recovery has been performed before any write operation proceeds.
///
/// Hybrid model:
/// - Startup recovery runs once.
/// - Writes perform a fast marker check and replay if a marker is present.
///
/// Recovery must be idempotent and safe to run multiple times.
/// All mutation entrypoints must call this before any commit boundary work.
pub fn ensure_recovered_for_write(
    db: &Db<impl crate::traits::CanisterKind>,
) -> Result<(), InternalError> {
    ensure_recovered(db)
}

fn perform_recovery(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    let marker = with_commit_store(|store| store.load())?;
    if let Some(marker) = marker {
        replay_recovery_row_ops(db, &marker.row_ops)?;
        with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        })?;
    }

    rebuild_secondary_indexes_from_rows(db)?;

    let _ = RECOVERED.set(());

    Ok(())
}

/// Replay marker row ops in order, rolling back on any preparation error.
///
/// Sequential replay is required for correctness when multiple row ops
/// touch the same index entry in one marker.
fn replay_recovery_row_ops(
    db: &Db<impl crate::traits::CanisterKind>,
    row_ops: &[CommitRowOp],
) -> Result<(), InternalError> {
    let mut rollbacks = Vec::<PreparedRowCommitOp>::with_capacity(row_ops.len());

    for row_op in row_ops {
        let prepared = match db.prepare_row_commit_op(row_op) {
            Ok(op) => op,
            Err(err) => {
                rollback_prepared_row_ops_reverse(rollbacks);
                return Err(err);
            }
        };

        rollbacks.push(snapshot_row_rollback(&prepared));
        prepared.apply();
    }

    Ok(())
}

#[derive(Clone)]
struct IndexStoreSnapshot {
    handle: StoreHandle,
    entries: Vec<(RawIndexKey, RawIndexEntry)>,
}

fn rebuild_secondary_indexes_from_rows(
    db: &Db<impl crate::traits::CanisterKind>,
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
            entries: handle.with_index(crate::db::index::IndexStore::entries),
        })
        .collect::<Vec<_>>();

    // Phase 2: clear and rebuild all index entries from authoritative data rows.
    let rebuild_result = rebuild_secondary_indexes_in_place(db, &stores);
    if let Err(err) = rebuild_result {
        // Phase 3: fail closed by restoring the exact pre-rebuild snapshot.
        restore_index_store_snapshots(snapshots);
        return Err(err);
    }

    Ok(())
}

fn sorted_store_handles(
    db: &Db<impl crate::traits::CanisterKind>,
) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.sort_by(|(left, _), (right, _)| left.cmp(right));
    stores
}

fn rebuild_secondary_indexes_in_place(
    db: &Db<impl crate::traits::CanisterKind>,
    stores: &[(&'static str, StoreHandle)],
) -> Result<(), InternalError> {
    for (_, handle) in stores {
        handle.with_index_mut(crate::db::index::IndexStore::clear);
    }

    for (store_path, handle) in stores {
        let rows = handle.with_data(|data_store| {
            data_store
                .iter()
                .map(|entry| (*entry.key(), entry.value()))
                .collect::<Vec<(RawDataKey, RawRow)>>()
        });

        for (raw_key, raw_row) in rows {
            let data_key = DataKey::try_from_raw(&raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!(
                        "startup index rebuild failed: invalid data key in store '{store_path}' ({err})"

                    ),
                )
            })?;

            let hooks = db.runtime_hook_for_entity_name(data_key.entity_name().as_str())?;
            let row_op = CommitRowOp::new(
                hooks.entity_path,
                raw_key.as_bytes().to_vec(),
                None,
                Some(raw_row.as_bytes().to_vec()),
            );
            let prepared = (hooks.prepare_row_commit)(db, &row_op).map_err(|err| {
                InternalError::new(
                    err.class,
                    err.origin,
                    format!(
                        "startup index rebuild failed: store='{}' entity='{}' ({})",
                        store_path, hooks.entity_path, err.message
                    ),
                )
            })?;

            apply_index_mutations(prepared.index_ops);
        }
    }

    Ok(())
}

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
