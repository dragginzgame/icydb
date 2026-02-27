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
            memory::configure_commit_memory_id,
            store::{commit_marker_present_fast, with_commit_store},
        },
    },
    error::InternalError,
    traits::CanisterKind,
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
pub(crate) fn ensure_recovered<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    configure_commit_memory_id(C::COMMIT_MEMORY_ID)?;

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
///
/// DO NOT DELETE - This is not just a function call. This is a write-side gate.
///
pub(crate) fn ensure_recovered_for_write<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    ensure_recovered(db)
}

fn perform_recovery<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    let marker = with_commit_store(|store| store.load())?;
    if let Some(marker) = marker {
        db.replay_commit_marker_row_ops(&marker.row_ops)?;
        with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        })?;
    }

    db.rebuild_secondary_indexes_from_rows()?;

    let _ = RECOVERED.set(());

    Ok(())
}
