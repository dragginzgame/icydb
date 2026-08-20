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
mod backlog_admission;
#[cfg(test)]
mod convergence_candidate;
mod guard;
mod marker;
mod memory;
mod prepare;
mod prepared_op;
mod recovery;
#[cfg(test)]
mod rollback;
mod schema_publication;
mod store;

pub(in crate::db) use backlog_admission::{
    BACKLOG_LIMITS, BacklogAdmission, ExactBacklogMeasurement, admit_backlog,
    current_database_backlog,
};
#[cfg(test)]
pub(in crate::db) use backlog_admission::{
    BacklogLimits, register_runtime_journal_tails_for_backlog,
};
///
/// Re-exports
///
#[cfg(test)]
pub(in crate::db) use guard::CommitApplyGuard;
#[doc(hidden)]
pub use guard::install_startup_recovery_wakeup;
pub(in crate::db) use guard::{
    CommitGuard, begin_commit, begin_mutation_progress_commit, finish_commit,
};
#[cfg(test)]
pub(in crate::db) use marker::{
    COMMIT_MARKER_FORMAT_VERSION_CURRENT, decode_commit_marker_payload,
    encode_commit_marker_payload,
};
pub(in crate::db) use marker::{
    CommitMarker, CommitRowOp, CommitSchemaFingerprint, DatabaseControlOp, MAX_COMMIT_BYTES,
    generate_commit_id, generate_marker_batch_id,
};
#[cfg(test)]
pub(in crate::db) use memory::current_commit_memory_allocation_if_configured;
pub(in crate::db) use memory::{
    CommitMemoryAllocation, commit_memory_handle, configure_commit_memory_id,
    current_commit_memory_allocation,
};
pub(in crate::db) use prepare::{
    CommitPrepareContext, CommitPrepareMode, prepare_commit_context_for_runtime_entity,
    prepare_row_commit_with_context,
};
pub(in crate::db) use prepared_op::{PreparedIndexMutation, PreparedRowCommitOp};
pub(in crate::db) use recovery::{
    RecoveryProgress, StartupRecoveryFailure, StartupRecoveryFailureAuthority,
    continue_recovery_with_failure_authority, ensure_recovery_admitted, startup_recovery_witness,
};
#[cfg(test)]
pub(in crate::db) use recovery::{
    continue_recovery, forget_recovered_domain_for_tests, mark_startup_recovery_complete_for_tests,
};
#[cfg(test)]
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
pub(in crate::db) use store::{
    CommitControlObservation, PersistedCommitControlObservation,
    apply_prepared_commit_control_replacement, cursor_authentication_key,
    database_control_proof_identity, database_incarnation_id, inspect_persisted_commit_control,
    next_database_commit_sequence, observe_commit_control, observe_commit_control_without_proof,
    prepare_commit_control_replacement,
};
pub(in crate::db) use store::{
    MAX_PERSISTED_STORE_ALLOCATIONS, PersistedStoreAllocation, PersistedStoreAllocationState,
    canonicalize_store_registry,
};
#[cfg(test)]
pub(in crate::db) use store::{
    initialize_current_commit_control_for_tests, retained_commit_marker_measurement_for_tests,
};
#[cfg(test)]
pub(in crate::db) use store::{
    persist_raw_commit_marker_for_tests, validate_commit_marker_envelope_for_tests,
};
