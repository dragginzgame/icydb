//! Module: commit::recovery
//! Responsibility: run system-level marker replay/rebuild recovery gates before operations.
//! Does not own: marker storage encoding, mutation planning, or query semantics.
//! Boundary: db entrypoints -> commit::recovery -> commit::{replay,rebuild,store} (one-way).
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
            rebuild::rebuild_secondary_indexes_from_rows,
            replay::replay_commit_marker_row_ops,
            store::{commit_marker_present_fast, with_commit_store},
        },
        diagnostics::integrity_report_after_recovery,
    },
    error::{ErrorOrigin, InternalError},
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
    configure_commit_memory_id(C::COMMIT_MEMORY_ID)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    if RECOVERED.get().is_none() {
        return perform_recovery(db);
    }

    if commit_marker_present_fast().map_err(|err| err.with_origin(ErrorOrigin::Recovery))? {
        return perform_recovery(db);
    }

    Ok(())
}

fn perform_recovery<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    let marker = with_commit_store(|store| store.load())
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    let had_marker = marker.is_some();
    if let Some(marker) = marker {
        // Phase 1: replay persisted row operations while marker authority is active.
        replay_commit_marker_row_ops(db, &marker.row_ops)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 2: rebuild secondary indexes from authoritative data rows.
    rebuild_secondary_indexes_from_rows(db)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    // Phase 3: enforce post-recovery integrity before clearing marker authority.
    validate_recovery_integrity(db).map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    // Phase 4: clear marker only after replay + rebuild + integrity validation succeed.
    if had_marker {
        with_commit_store(super::store::CommitStore::clear_verified)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 5: authoritative rebuild succeeded, so every registered index is
    // query-visible again.
    db.mark_all_registered_index_stores_ready();

    let _ = RECOVERED.set(());

    Ok(())
}
// Fail closed if recovery leaves any index/data divergence findings.
fn validate_recovery_integrity<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    if !db.has_runtime_hooks() {
        return Ok(());
    }

    let report = integrity_report_after_recovery(db)?;
    let totals = report.totals();
    if totals.missing_index_entries() > 0
        || totals.divergent_index_entries() > 0
        || totals.orphan_index_references() > 0
    {
        return Err(InternalError::recovery_integrity_validation_failed(
            totals.missing_index_entries(),
            totals.divergent_index_entries(),
            totals.orphan_index_references(),
        ));
    }

    Ok(())
}
