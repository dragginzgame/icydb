//! Module: commit
//! Responsibility: durable commit-marker protocol and recovery authority boundaries.
//! Does not own: query planning, index encoding semantics, or predicate semantics.
//! Boundary: executor::mutation -> commit (one-way).
//!
//! Contract:
//! - `begin_commit` persists a marker that fully describes durable mutations.
//! - Durable correctness is owned by marker-bound journal publication and recovery.
//! - In-process apply guards are best-effort cleanup only and are not authoritative.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every durable journal publication. After
//! the marker is persisted, executors must not re-derive durable semantics or
//! branch on entity/index contents. Recovery publishes the recorded journal
//! batches and rebuilds derived projections from current durable authority.

mod apply;
#[cfg(test)]
mod failpoint;
mod guard;
mod marker;
mod memory;
mod prepare;
mod prepared_op;
mod rebuild;
mod recovery;
mod rollback;
mod schema_publication;
mod store;
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::error::InternalError;
#[cfg(test)]
use crate::testing::test_commit_memory_id;

#[cfg(test)]
const TEST_COMMIT_STABLE_KEY: &str = "icydb.test.commit.v1";

#[cfg(test)]
pub(in crate::db) use failpoint::{
    CommitFailpoint, CommitFailpointFailureClass, CommitFailpointMode,
    CommitFailpointRecoveryAuthority, CommitFailpointSnapshotOracle,
    arm_commit_failpoint_for_tests, clear_commit_failpoint_for_tests,
};
///
/// Re-exports
///
pub(in crate::db) use guard::{CommitApplyGuard, CommitGuard, begin_commit, finish_commit};
#[cfg(test)]
pub(in crate::db) use marker::COMMIT_MARKER_FORMAT_VERSION_CURRENT;
#[cfg(feature = "sql")]
pub(in crate::db) use marker::commit_marker_payload_capacity_for_single_batch;
#[cfg(test)]
pub(in crate::db) use marker::reset_test_journal_sequence as reset_commit_marker_test_journal_sequence;
pub(in crate::db) use marker::{
    CommitIndexOp, CommitMarker, CommitRowOp, CommitSchemaFingerprint, MAX_COMMIT_BYTES,
    generate_commit_id,
};
pub(in crate::db) use memory::{
    CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation,
};
#[cfg(test)]
pub(in crate::db) use prepare::prepare_row_commit_for_entity_with_structural_readers;
pub(in crate::db) use prepare::{
    CommitPrepareContext, prepare_commit_context_for_entity_with_schema_fingerprint,
    prepare_commit_context_for_runtime_entity,
    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
    prepare_row_commit_with_context,
};
pub(in crate::db) use prepared_op::{PreparedIndexMutation, PreparedRowCommitOp};
#[cfg(test)]
pub(in crate::db) use recovery::clear_recovery_runtime_state_for_tests;
pub(in crate::db) use recovery::ensure_recovered;
#[cfg(test)]
pub(in crate::db::commit) use recovery::mark_schema_reconciliation_dirty_for_tests;
pub(in crate::db) use rollback::rollback_prepared_row_ops_reverse;
pub(in crate::db) use schema_publication::publish_accepted_schema_candidate;
pub(in crate::db) use schema_publication::publish_accepted_schema_candidate_with_derived_domains;
#[cfg(feature = "sql")]
pub(in crate::db) use schema_publication::publish_accepted_schema_candidate_with_user_index_domains;
#[cfg(test)]
#[cfg(feature = "sql")]
pub(in crate::db) use store::persisted_commit_marker_lengths_for_tests;
#[cfg(test)]
pub(in crate::db) use store::validate_commit_marker_envelope_for_tests;

/// Return whether one single-store journaled row-op prefix fits the current
/// durable commit-control window exactly as it would be encoded.
#[must_use]
#[cfg(feature = "sql")]
pub(in crate::db) fn journaled_row_ops_fit_commit_window(row_ops: &[CommitRowOp]) -> bool {
    let record_payload_bytes = row_ops.iter().fold(0usize, |bytes, row_op| {
        let record_bytes = match row_op.after.as_ref() {
            Some(after) => crate::db::journal::journal_row_put_record_payload_len(
                row_op.entity_path.len(),
                row_op.key.as_bytes().len(),
                after.len(),
            ),
            None => crate::db::journal::journal_row_delete_record_payload_len(
                row_op.entity_path.len(),
                row_op.key.as_bytes().len(),
            ),
        };
        bytes.saturating_add(record_bytes)
    });
    let Some(batch_bytes) = crate::db::journal::journal_batch_encoded_len_for_record_payloads(
        row_ops.len(),
        record_payload_bytes,
    ) else {
        return false;
    };
    let marker_payload_bytes = commit_marker_payload_capacity_for_single_batch(batch_bytes);

    store::commit_control_slot_encoded_len_for_marker_payload(marker_payload_bytes).is_some()
}

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
    })?;
    recovery::clear_recovery_in_progress_for_tests();

    Ok(())
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

    // Phase 2: direct commit tests initialize the current database format
    // without a registry-backed virginity proof; recovery tests exercise the
    // real admission gate separately.
    let allocation = memory::current_commit_memory_allocation()?;
    let control_memory = memory::commit_memory_handle(allocation)?;
    crate::db::database_format::initialize_current_database_control_for_tests(&control_memory);

    // Phase 3: initialize the commit store in the configured slot.
    store::with_commit_store(|_| Ok(()))
}
