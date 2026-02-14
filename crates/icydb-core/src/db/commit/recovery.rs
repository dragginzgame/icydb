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
            CommitRowOp, PreparedRowCommitOp, snapshot_row_rollback,
            store::{commit_marker_present_fast, with_commit_store},
        },
    },
    error::InternalError,
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
                rollback_recovery_ops(rollbacks);
                return Err(err);
            }
        };

        rollbacks.push(snapshot_row_rollback(&prepared));
        prepared.apply();
    }

    Ok(())
}

/// Best-effort rollback for recovery replay errors.
fn rollback_recovery_ops(ops: Vec<PreparedRowCommitOp>) {
    for op in ops.into_iter().rev() {
        op.apply();
    }
}
