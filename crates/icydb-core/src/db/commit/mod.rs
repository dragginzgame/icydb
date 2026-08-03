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

///
/// Re-exports
///
pub(in crate::db) use guard::{CommitApplyGuard, CommitGuard, begin_commit, finish_commit};
#[cfg(feature = "sql")]
pub(in crate::db) use marker::commit_marker_payload_capacity_for_single_batch;
#[cfg(test)]
pub(in crate::db) use marker::{
    COMMIT_MARKER_FORMAT_VERSION_CURRENT, decode_commit_marker_payload,
    encode_commit_marker_payload,
};
pub(in crate::db) use marker::{
    CommitMarker, CommitRowOp, CommitSchemaFingerprint, DatabaseControlOp, MAX_COMMIT_BYTES,
    generate_commit_id, generate_marker_batch_id,
};
pub(in crate::db) use memory::{
    CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation,
};
pub(in crate::db) use prepare::{
    CommitPrepareContext, CommitPrepareMode, prepare_commit_context_for_runtime_entity,
    prepare_row_commit_with_context,
};
pub(in crate::db) use prepared_op::{PreparedIndexMutation, PreparedRowCommitOp};
pub(in crate::db) use recovery::ensure_recovered;
#[cfg(test)]
pub(in crate::db) use recovery::forget_recovered_domain_for_tests;
pub(in crate::db) use rollback::rollback_prepared_row_ops_reverse;
pub(in crate::db) use schema_publication::publish_accepted_schema_candidate;
#[cfg(feature = "sql")]
pub(in crate::db) use schema_publication::publish_accepted_schema_candidate_with_user_index_domains;
pub(in crate::db) use schema_publication::publish_constraint_validation_job_with_candidate_index_entries;
pub(in crate::db) use schema_publication::{
    AcceptedSchemaPublication, publish_accepted_schema_candidates_with_application_record,
    publish_accepted_schema_candidates_with_database_control,
};
pub(in crate::db) use schema_publication::{
    publish_accepted_schema_candidate_with_constraint_validation_job,
    publish_accepted_schema_candidate_with_constraint_validation_job_removal,
    publish_constraint_validation_job, publish_generated_row_local_abort_with_application_record,
};
#[cfg(test)]
pub(in crate::db) use store::validate_commit_marker_envelope_for_tests;
pub(in crate::db) use store::{database_control_proof_identity, database_incarnation_id};

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
