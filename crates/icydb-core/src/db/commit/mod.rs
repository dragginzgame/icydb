//! Module: commit
//! Responsibility: durable commit-marker protocol and recovery authority boundaries.
//! Does not own: query planning, index encoding semantics, or predicate semantics.
//! Boundary: executor::mutation -> commit (one-way).
//!
//! Contract:
//! - `begin_commit` persists a marker that fully describes durable mutations.
//! - Durable correctness is owned by marker replay in recovery (row ops).
//! - In-process apply guards are best-effort cleanup only and are not authoritative.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every row mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays row ops.
//! Recovery replays row ops as recorded, not planner logic.

mod apply;
mod guard;
mod marker;
mod memory;
mod prepare;
mod prepared_op;
mod rebuild;
mod recovery;
mod replay;
mod rollback;
mod store;
#[cfg(test)]
mod tests;

use crate::error::InternalError;
#[cfg(test)]
use crate::testing::{TEST_MEMORY_RANGE_END, TEST_MEMORY_RANGE_START, test_commit_memory_id};
#[cfg(test)]
use canic_memory::api::MemoryApi;

///
/// Re-exports
///
pub(in crate::db) use guard::{
    CommitApplyGuard, CommitGuard, begin_commit, begin_commit_with_migration_state,
    begin_single_row_commit, finish_commit,
};
pub(in crate::db) use marker::{CommitIndexOp, CommitMarker, CommitRowOp, CommitSchemaFingerprint};
pub(in crate::db) use prepare::{
    prepare_row_commit_for_entity_with_structural_readers,
    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
};
pub(in crate::db) use prepared_op::{PreparedIndexMutation, PreparedRowCommitOp};
pub(in crate::db) use recovery::ensure_recovered;
pub(in crate::db) use rollback::rollback_prepared_row_ops_reverse;

/// Return true if a commit marker is currently persisted.
#[cfg(test)]
pub(in crate::db) fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

/// Clear the persisted commit marker in tests.
#[cfg(test)]
pub(in crate::db) fn clear_commit_marker_for_tests() -> Result<(), InternalError> {
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
}

/// Persist a raw commit marker in tests without running the normal begin-commit gate.
#[cfg(test)]
pub(in crate::db) fn persist_raw_commit_marker_for_tests(
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    let marker_payload = marker::encode_commit_marker_payload(marker)?;
    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        marker::COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )?;
    let control_slot_bytes =
        store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes, Vec::new())?;

    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
}

/// Load persisted migration-state bytes from the shared commit control slot.
pub(in crate::db) fn load_migration_state_bytes() -> Result<Option<Vec<u8>>, InternalError> {
    store::with_commit_store(|store| store.load_migration_state_bytes())
}

/// Clear persisted migration-state bytes from the shared commit control slot.
pub(in crate::db) fn clear_migration_state_bytes() -> Result<(), InternalError> {
    store::with_commit_store(store::CommitStore::clear_migration_state_bytes)
}

/// Initialize commit marker storage for tests.
///
/// Tests reserve a dedicated range and pin the commit marker slot to one
/// canonical id managed by `test_support`.
#[cfg(test)]
pub(in crate::db) fn init_commit_store_for_tests() -> Result<(), InternalError> {
    // Phase 1: bootstrap the reserved test range through the public memory API.
    MemoryApi::bootstrap_owner_range("icydb_test", TEST_MEMORY_RANGE_START, TEST_MEMORY_RANGE_END)
        .map_err(InternalError::commit_memory_registry_init_failed)?;

    // Phase 2: pin and register the explicit commit marker slot.
    memory::configure_commit_memory_id(test_commit_memory_id())?;

    // Phase 3: initialize the commit store in the configured slot.
    store::with_commit_store(|_| Ok(()))
}
