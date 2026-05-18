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

#[cfg(test)]
use crate::error::InternalError;
#[cfg(test)]
use crate::testing::test_commit_memory_id;

#[cfg(test)]
const TEST_COMMIT_STABLE_KEY: &str = "icydb.test.commit.v1";

///
/// Re-exports
///
pub(in crate::db) use guard::{
    CommitApplyGuard, CommitGuard, begin_commit, begin_single_row_commit, finish_commit,
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
    let control_slot_bytes = store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes)?;

    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
}

/// Initialize commit marker storage for tests.
///
/// Tests reserve a dedicated range and pin the commit marker slot to one
/// canonical id managed by `test_support`.
#[cfg(test)]
pub(in crate::db) fn init_commit_store_for_tests() -> Result<(), InternalError> {
    // Phase 1: pin the explicit commit marker slot. Core unit tests use a
    // test-memory backend because Canic's bootstrap seal is process-global
    // while Rust test bodies run in separate OS threads.
    memory::configure_commit_memory_id(test_commit_memory_id(), TEST_COMMIT_STABLE_KEY)?;

    // Phase 2: initialize the commit store in the configured slot.
    store::with_commit_store(|_| Ok(()))
}
