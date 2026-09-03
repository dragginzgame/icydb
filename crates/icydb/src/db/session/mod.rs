//! Module: db::session
//!
//! Responsibility: public session, typed-query, SQL, and structural-write facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

mod catalog;
pub(crate) mod generated;
mod integrity;
mod live_page;
mod prepared_query;
mod request;
#[cfg(feature = "sql")]
mod sql;
mod write;

use crate::traits::CanisterKind;

use icydb_core as core;
use std::{error::Error as StdError, fmt};

// re-exports
pub use integrity::IntegrityCheckError;
#[cfg(feature = "sql")]
pub use integrity::SqlIntegrityError;
pub use live_page::LivePageStep;
pub use prepared_query::{PreparedExactKeyOutput, PreparedLivePageCursor, PreparedLivePageOutput};
pub use request::{
    RequestExecutionFuture, RequestExecutionRoot, with_request_execution,
    with_request_execution_async, with_request_execution_root,
};
pub(crate) use write::OutputRowProjection;
pub use write::{
    BoundWriteEncoder, OutputRow, PreparedOutputRows, StructuralMutation, StructuralPatch,
    TrustedTypedWriteBatch, TypedAdapterError, TypedEntityAdapter, TypedEntityBinding,
    TypedOperationError, TypedRowAdapter, TypedWrite, TypedWriteAdapter, TypedWriteBatchResult,
    TypedWriteBatchResults, TypedWriteHandle, WriteCell,
};

/// Failure while capturing or executing one revision-strict exhaustive read.
#[derive(Debug)]
pub enum ExhaustiveReadError {
    /// Planning, admission, execution, or database authority failed.
    Database(crate::Error),
    /// The bounded source proof was invalid or changed.
    Revision(core::db::ReadSetRevisionError),
}

impl fmt::Display for ExhaustiveReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::Revision(error) => error.fmt(formatter),
        }
    }
}

impl StdError for ExhaustiveReadError {}

impl From<core::db::ExhaustiveReadError> for ExhaustiveReadError {
    fn from(error: core::db::ExhaustiveReadError) -> Self {
        match error {
            core::db::ExhaustiveReadError::Query(error) => Self::Database(error.into()),
            core::db::ExhaustiveReadError::Revision(error) => Self::Revision(error),
        }
    }
}
#[doc(hidden)]
pub use write::{TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType};

///
/// DbSession
///
/// Public facade for typed/dynamic query adaptation, optional SQL execution,
/// and structural mutation policy.
/// Wraps the core session and converts core results and errors into the
/// outward-facing `icydb` response surface.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    // ------------------------------------------------------------------
    // Session configuration
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn new(session: core::db::DbSession<C>) -> Self {
        Self { inner: session }
    }

    /// Run at most one bounded generated-startup recovery page.
    #[doc(hidden)]
    pub fn __drive_generated_startup_recovery_page(
        &self,
        stores: &'static std::thread::LocalKey<core::db::StoreRegistry>,
        submission_key: &str,
    ) -> Result<core::db::GeneratedStartupDriverStep, crate::Error> {
        core::db::drive_generated_startup_recovery_page(&self.inner, stores, submission_key)
            .map_err(Into::into)
    }

    #[must_use]
    pub fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    /// Execute one revision-tolerant bounded dynamic page.
    pub fn execute_live_page(
        &self,
        request: &crate::db::DynamicQuery,
        continuation: Option<&str>,
    ) -> Result<crate::db::LiveQueryPageOutput, crate::Error> {
        self.inner
            .execute_public_live_page(request, continuation)
            .map_err(Into::into)
    }

    /// Return exact visible cardinality without scanning rows.
    ///
    /// The request must contain only an optional strict equality or bounded
    /// `IN` filter over the leading field of an accepted unfiltered field-path
    /// user index. The index may have trailing fields. Unsupported shapes and
    /// unavailable exact-cardinality metadata fail closed.
    pub fn execute_exact_count(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<u64, crate::Error> {
        self.inner
            .execute_public_exact_count(request)
            .map_err(Into::into)
    }

    /// Execute one trusted revision-tolerant bounded dynamic page.
    pub fn execute_trusted_live_page(
        &self,
        request: &crate::db::DynamicQuery,
        continuation: Option<&str>,
    ) -> Result<crate::db::LiveQueryPageOutput, crate::Error> {
        self.inner
            .execute_trusted_live_page(request, continuation)
            .map_err(Into::into)
    }

    /// Capture one canonical proof for every physical store behind the named entities.
    pub fn capture_read_set_revision_proof(
        &self,
        entity_names: &[&str],
    ) -> Result<crate::db::ReadSetRevisionProof, ExhaustiveReadError> {
        self.inner
            .capture_read_set_revision_proof(entity_names)
            .map_err(Into::into)
    }

    /// Execute one ordinary revision-strict bounded dynamic page.
    pub fn execute_exhaustive_page(
        &self,
        request: &crate::db::DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&crate::db::ReadSetRevisionProof>,
    ) -> Result<crate::db::ExhaustiveQueryPageOutput, ExhaustiveReadError> {
        self.inner
            .execute_public_exhaustive_page(request, continuation, proof)
            .map_err(Into::into)
    }

    /// Execute one trusted revision-strict bounded dynamic page.
    pub fn execute_trusted_exhaustive_page(
        &self,
        request: &crate::db::DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&crate::db::ReadSetRevisionProof>,
    ) -> Result<crate::db::ExhaustiveQueryPageOutput, ExhaustiveReadError> {
        self.inner
            .execute_trusted_exhaustive_page(request, continuation, proof)
            .map_err(Into::into)
    }

    /// Persist one admitted fixed SQL mutation job before any target mutation.
    #[cfg(feature = "sql")]
    pub fn start_trusted_sql_mutation_job(
        &self,
        job_id: crate::db::MutationJobId,
        sql: &str,
    ) -> Result<crate::db::MutationJobState, crate::db::MutationJobError> {
        self.inner.start_trusted_sql_mutation_job(job_id, sql)
    }

    /// Load one retained durable mutation job.
    pub fn mutation_job_state(
        &self,
        job_id: crate::db::MutationJobId,
    ) -> Result<crate::db::MutationJobState, crate::db::MutationJobError> {
        self.inner.mutation_job_state(job_id)
    }

    /// Advance one durable mutation job through one bounded engine-owned step.
    #[cfg(feature = "sql")]
    pub fn advance_trusted_mutation_job(
        &self,
        request: &crate::db::MutationJobAdvanceRequest,
    ) -> Result<crate::db::MutationJobAdvanceReceipt, crate::db::MutationJobError> {
        self.inner.advance_trusted_mutation_job(request)
    }

    /// Idempotently remove one terminal mutation job after consuming its result.
    pub fn acknowledge_mutation_job(
        &self,
        job_id: crate::db::MutationJobId,
        expected_terminal_sequence: u64,
    ) -> Result<(), crate::db::MutationJobError> {
        self.inner
            .acknowledge_mutation_job(job_id, expected_terminal_sequence)
    }

    /// Idempotently remove one exact sequence-zero mutation job.
    ///
    /// Applications must authorize this trusted operation and allocate a fresh
    /// job identity for every later logical mutation.
    #[cfg(feature = "sql")]
    pub fn cancel_unadvanced_mutation_job(
        &self,
        job_id: crate::db::MutationJobId,
        expected_sequence: u64,
    ) -> Result<(), crate::db::MutationJobError> {
        self.inner
            .cancel_unadvanced_mutation_job(job_id, expected_sequence)
    }

    /// Return one complete bounded inventory of shared retained progress.
    ///
    /// Applications must authorize any public wrapper around this trusted
    /// operation; job identities are not bearer authority.
    pub fn progress_job_inventory(
        &self,
    ) -> Result<crate::db::ProgressJobInventory, crate::db::MutationJobError> {
        self.inner.progress_job_inventory()
    }

    /// Create one durable application-owned progress job.
    pub fn start_resumable_job(
        &self,
        job_id: crate::db::ResumableJobId,
        proof: crate::db::ReadSetRevisionProof,
        initial_application_state: Vec<u8>,
    ) -> Result<crate::db::ResumableJobState, crate::db::ResumableJobError> {
        self.inner
            .start_resumable_job(job_id, proof, initial_application_state)
    }

    /// Load one retained application-owned progress job.
    pub fn resumable_job_state(
        &self,
        job_id: crate::db::ResumableJobId,
    ) -> Result<crate::db::ResumableJobState, crate::db::ResumableJobError> {
        self.inner.resumable_job_state(job_id)
    }

    /// Idempotently remove one terminal job after consuming its result.
    pub fn acknowledge_resumable_job(
        &self,
        job_id: crate::db::ResumableJobId,
        expected_sequence: u64,
    ) -> Result<(), crate::db::ResumableJobError> {
        self.inner
            .acknowledge_resumable_job(job_id, expected_sequence)
    }

    /// Execute and atomically retain one proof-checked application page.
    pub fn compare_proof_and_advance<E>(
        &self,
        request: &crate::db::ResumableJobAdvanceRequest,
        operation: impl FnOnce(
            &crate::db::ResumableJobState,
        ) -> Result<crate::db::ResumableJobAdvance, E>,
    ) -> Result<crate::db::ResumableJobAdvanceReceipt, crate::db::CompareProofAndAdvanceError<E>>
    {
        self.inner.compare_proof_and_advance(request, operation)
    }

    /// Execute one ordinary entity-name-driven bounded grouped read.
    pub fn execute_public_dynamic_grouped_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::GroupedQueryOutput, crate::Error> {
        self.inner
            .execute_public_dynamic_grouped_query(request)
            .map_err(Into::into)
    }

    /// Execute one trusted entity-name-driven grouped read.
    pub fn execute_trusted_dynamic_grouped_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::GroupedQueryOutput, crate::Error> {
        self.inner
            .execute_trusted_dynamic_grouped_query(request)
            .map_err(Into::into)
    }

    pub(crate) fn execute_public_typed_exact_count(
        &self,
        binding: &TypedEntityBinding,
        request: &crate::db::DynamicQuery,
    ) -> Result<Option<u64>, crate::Error> {
        self.inner
            .execute_public_exact_count_for_typed_binding(binding.inner(), request)
            .map_err(Into::into)
    }

    pub(crate) fn execute_public_typed_exhaustive_page(
        &self,
        binding: &TypedEntityBinding,
        request: &crate::db::DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&crate::db::ReadSetRevisionProof>,
    ) -> Result<Option<crate::db::ExhaustiveQueryPageOutput>, ExhaustiveReadError> {
        self.inner
            .execute_public_exhaustive_page_for_typed_binding(
                binding.inner(),
                request,
                continuation,
                proof,
            )
            .map_err(Into::into)
    }

    pub(crate) fn execute_public_typed_dynamic_grouped_query(
        &self,
        binding: &TypedEntityBinding,
        request: &crate::db::DynamicQuery,
    ) -> Result<Option<crate::db::GroupedQueryOutput>, crate::Error> {
        self.inner
            .execute_public_dynamic_grouped_query_for_typed_binding(binding.inner(), request)
            .map_err(Into::into)
    }
}
