//! Module: db
//!
//! Responsibility: root subsystem wiring, façade re-exports, and accepted
//! runtime entity routing.
//! Does not own: feature semantics delegated to child modules (`query`, `executor`, etc.).
//! Boundary: top-level db API and internal orchestration entrypoints.

pub(crate) mod access;
pub(crate) mod catalog;
pub(crate) mod cursor;
pub(crate) mod diagnostics;
mod dynamic_write;
pub(crate) mod identity;
pub(crate) mod integrity;
mod mutation_job;
pub(crate) mod predicate;
pub(crate) mod query;
mod read_set;
pub(crate) mod registry;
pub(crate) mod response;
mod resumable_job;
pub(crate) mod runtime_entity_catalog;
pub(crate) mod scalar_expr;
pub(crate) mod schema;
pub(crate) mod session;
#[cfg(feature = "sql")]
pub(crate) mod sql;
mod startup;
pub(in crate::db) mod write_context;

pub(in crate::db) mod codec;
pub(in crate::db) mod commit;
pub(in crate::db) mod data;
pub(in crate::db) mod database_format;
pub(in crate::db) mod direction;
pub(in crate::db) mod executor;
pub(in crate::db) mod index;
pub(in crate::db) mod journal;
pub(in crate::db) mod key_taxonomy;
pub(in crate::db) mod numeric;
pub(in crate::db) mod ordered_overlay;
pub(in crate::db) mod positioned_overlay;
pub(in crate::db) mod relation;
pub(in crate::db) mod sql_shared;
#[cfg(test)]
pub(in crate::db) mod test_support;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        commit::{CommitRowOp, PreparedRowCommitOp, ensure_recovery_admitted},
        data::RawDataStoreKey,
        registry::StoreHandle,
        schema::ensure_schema_migration_ready_for_ordinary_operations,
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::{collections::BTreeSet, marker::PhantomData, thread::LocalKey};

pub use catalog::{
    EntityCatalogCounts, EntityCatalogDescription, MemoryCatalogDescription,
    StoreCatalogDescription,
};
#[doc(hidden)]
pub use codec::hex::encode_hex_lower;
#[doc(hidden)]
pub use commit::install_startup_recovery_wakeup;
pub use data::DataStore;
pub use diagnostics::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, SchemaStoreSnapshot, StorageReport,
    StoreSnapshotStorageMode,
};
pub use diagnostics::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionStats,
    ExecutionTrace,
};
#[doc(hidden)]
pub use dynamic_write::{
    DynamicMutation, DynamicStructuralPatch, DynamicTypedMutation, DynamicTypedStructuralPatch,
    DynamicWriteCell,
};
pub use dynamic_write::{
    DynamicMutationResult, DynamicTypedBindingError, DynamicTypedEntityBinding,
    TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType,
};
pub use executor::{ExecutionFamily, RouteExecutionMode};
pub use identity::{EntityName, IndexName};
pub use index::{IndexState, IndexStore};
pub use integrity::{
    DatabaseIncarnationId, DeepIntegrityPage, DeepIntegrityPageStatus, IntegrityAbortReceipt,
    IntegrityAbortStatus, IntegrityAuthorityClass, IntegrityAuthorityDiagnostic,
    IntegrityCheckRequest, IntegrityCheckResult, IntegrityDeepError, IntegrityEntityIdentity,
    IntegrityFinding, IntegrityFindingClass, IntegrityFindingKind, IntegrityJobError,
    IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt, IntegrityPendingTerminal,
    IntegrityPhase, IntegrityResourceDiagnostic, IntegritySeverity, IntegritySubmissionKey,
    IntegrityTerminalOutcome, IntegrityVerifierFamily, ProgressJobFamily, ProgressJobInventory,
    ProgressJobInventoryRecord, ProgressJobLifecycle, QuickIntegrityResult, QuickIntegrityStatus,
};
#[doc(hidden)]
pub use journal::JournalTailStore;
#[doc(hidden)]
pub use key_taxonomy::{
    CompositePrimaryKeyValue, CompositePrimaryKeyValueError, EntityKey, EntityKeyBytes,
    EntityKeyBytesError, KeyValueCodec, PrimaryKeyComponent, PrimaryKeyDecode, PrimaryKeyEncode,
    PrimaryKeyEncodeError, PrimaryKeyValue, ScalarRelationTargetKey,
    validate_entity_key_bytes_buffer,
};
pub use mutation_job::{
    MAX_MUTATION_JOB_CONTINUATION_BYTES, MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES,
    MAX_MUTATION_JOB_INTENT_BYTES, MAX_MUTATION_JOB_RECEIPT_BYTES, MAX_MUTATION_JOB_RECORD_BYTES,
    MAX_MUTATION_JOB_STEP_KEYS_SCANNED, MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
    MutationJobAdvanceReceipt, MutationJobAdvanceRequest, MutationJobError, MutationJobId,
    MutationJobIdempotencyKey, MutationJobPayloadKind, MutationJobPhase, MutationJobRestartReason,
    MutationJobState, MutationJobStatus, MutationJobTargetFailureReason,
};
pub use predicate::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, MissingRowPolicy, Predicate,
};
pub use query::DynamicQuery;
pub use query::builder::numeric_projection::{
    NumericProjectionExpr, RoundProjectionExpr, add, div, mul, round, round_expr, sub,
};
#[cfg(feature = "sql")]
pub use query::explain::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
pub use query::plan::validate::PlanError;
pub use query::{
    builder::{
        AggregateExpr, FieldRef, TextProjectionExpr, ValueProjectionExpr, avg, contains, count,
        count_by, ends_with, exists, first, last, left, length, lower, ltrim, max, max_by, min,
        min_by, position, replace, right, rtrim, starts_with, substring, substring_with_length,
        sum, trim, upper,
    },
    explain::{
        ExplainAccessCandidate, ExplainAccessDecision, ExplainAccessDecisionKind,
        ExplainEligibleAlternative, ExplainPlan, ExplainRejectedIndex, ExplainResidualSummary,
        ExplainSelectedAccess,
    },
    expr::{
        CollectionOperator, CompareOperator, FieldCompareOperator, FilterExpr, FilterValue,
        JunctionOperator, OrderExpr, OrderTerm, SetOperator, StateOperator, asc, desc, field,
    },
    intent::{IntentError, QueryError, QueryExecutionError},
    plan::{DeleteSpec, LoadSpec, OrderDirection, QueryMode},
    read_intent::ReadIntentKind,
    trace::TraceReuseEvent,
};
pub use read_set::{
    ExhaustiveReadError, MAX_READ_SET_PROOF_BYTES, MAX_READ_SET_PROOF_STORES, ReadSetRevisionError,
    ReadSetRevisionProof, ReadSetStoreIdentity, ReadSetStoreRevision,
};
pub use registry::{
    StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
    StoreCommitParticipation, StoreDurability, StoreRecoveryCapability, StoreRegistry,
    StoreRelationSourceCapability, StoreRelationTargetCapability, StoreRuntimeStorageCapabilities,
    StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
};
#[doc(hidden)]
pub use response::ExactKeyBatchProjectionOutput;
pub use response::{
    ExhaustiveQueryPageOutput, GroupedQueryOutput, GroupedRow, LiveQueryPageOutput,
    RowProjectionOutput, ScalarPageWork,
};
pub(in crate::db) use resumable_job::ResumableJobRecord;
pub use resumable_job::{
    CompareProofAndAdvanceError, MAX_RESUMABLE_JOB_CONTINUATION_BYTES,
    MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES, MAX_RESUMABLE_JOB_RECEIPT_BYTES,
    MAX_RESUMABLE_JOB_STATE_BYTES, ResumableJobAdvance, ResumableJobAdvanceReceipt,
    ResumableJobAdvanceRequest, ResumableJobAdvanceStatus, ResumableJobError, ResumableJobId,
    ResumableJobIdempotencyKey, ResumableJobState, ResumableJobStatus,
};
#[doc(hidden)]
pub use schema::validate_generated_constraint_name;
pub use schema::{
    ConstraintValidationProgressDescription, EntityConstraintDescription, EntityFieldDescription,
    EntityIdentityDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntitySchemaDescription, SchemaLiteralValidationReason, SchemaStore,
    SchemaValidationOperator, SqlColumnDefault, SqlColumnExtra, SqlColumnKey, SqlColumnSummary,
    SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput, ValidateError,
};
pub use schema::{
    SchemaApplicationStore, SchemaApplicationTarget, SchemaChangeJob, SchemaChangeJobId,
    SchemaChangeOutcome, SchemaChangeProgress, SchemaChangeProgressStatus, SchemaChangeReceipt,
    SchemaChangeValidationPhase,
};
#[cfg(feature = "migration")]
pub use schema::{
    SchemaMigrationCommand, SchemaMigrationEntityTransition, SchemaMigrationFinding,
    SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationReceipt,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
};
pub use session::{DbSession, RequestExecutionRoot};
#[doc(hidden)]
pub use session::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
#[cfg(feature = "sql")]
pub use session::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlIntegrityError, SqlStatementDispatch, SqlStatementResult,
    SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(feature = "sql")]
pub use sql::identifier::{
    identifier_last_segment, normalize_identifier_to_scope, split_qualified_identifier,
};
#[cfg(feature = "sql")]
pub use sql::lowering::LoweredSqlCommand;
pub use startup::{DatabaseStartupState, GeneratedStartupDriverStep, StartupFailureKind};
#[doc(hidden)]
pub use startup::{
    StartupFailure, clear_generated_startup_failure, drive_generated_startup_recovery_page,
    observe_generated_startup_state, record_generated_schema_startup_failure,
};
pub use write_context::MutationMode;

///
/// Db
/// A handle to the set of stores registered for a specific canister domain.
///

pub(crate) struct Db<C: CanisterKind> {
    store: &'static LocalKey<StoreRegistry>,
    request_scope: session::RequestExecutionScope,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    /// Construct a database handle over one sealed runtime store registry.
    #[must_use]
    pub(in crate::db) const fn new(
        store: &'static LocalKey<StoreRegistry>,
        request_scope: session::RequestExecutionScope,
    ) -> Self {
        Self {
            store,
            request_scope,
            _marker: PhantomData,
        }
    }

    pub(in crate::db) const fn request_execution_scope(&self) -> &session::RequestExecutionScope {
        &self.request_scope
    }

    /// Resolve one named store after enforcing startup recovery.
    pub(in crate::db) fn recovered_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        ensure_recovery_admitted(self)?;
        ensure_schema_migration_ready_for_ordinary_operations()?;

        self.store_handle(path)
    }

    // Resolve one named store without re-entering recovery.
    //
    // Internal commit/recovery paths already own recovery authority and must
    // not bounce back through ordinary admission, or they can recurse through
    // replay/rebuild preparation.
    pub(in crate::db) fn store_handle(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.with_store_registry(|registry| registry.try_get_store(path))
    }

    /// Ensure startup/in-progress commit recovery has been applied.
    pub(crate) fn ensure_recovered_state(&self) -> Result<(), InternalError> {
        ensure_recovery_admitted(self)?;
        ensure_schema_migration_ready_for_ordinary_operations()
    }

    /// Advance startup recovery by one durable bounded page in focused tests.
    #[cfg(test)]
    pub(crate) fn drive_startup_recovery_page(&self) -> Result<bool, InternalError> {
        commit::continue_recovery(self)
            .map(|progress| progress == commit::RecoveryProgress::Complete)
    }

    /// Advance startup recovery while retaining the exact persisted authority
    /// for a timer-discovered terminal failure.
    pub(in crate::db) fn drive_startup_recovery_page_with_failure_authority(
        &self,
    ) -> Result<bool, commit::StartupRecoveryFailure> {
        commit::continue_recovery_with_failure_authority(self)
            .map(|progress| progress == commit::RecoveryProgress::Complete)
    }

    /// Recover durable state for an explicit control-plane operation that is
    /// allowed to inspect a gated migration. This never clears or bypasses the
    /// gate for ordinary database work.
    pub(in crate::db) fn ensure_recovered_control_state(&self) -> Result<(), InternalError> {
        ensure_recovery_admitted(self)
    }

    /// Execute one closure against the registered store set.
    pub(crate) fn with_store_registry<R>(&self, f: impl FnOnce(&StoreRegistry) -> R) -> R {
        self.store.with(|reg| f(reg))
    }

    /// Resolve one stable in-process cache scope identifier for this store registry.
    ///
    /// Session-level SQL and structural query caches use this scope to share
    /// reusable artifacts across fresh `DbSession` values that point at the
    /// same generated canister store wiring without leaking entries across
    /// unrelated registries in tests or multi-canister host processes.
    #[must_use]
    pub(in crate::db) fn cache_scope_id(&self) -> usize {
        std::ptr::from_ref::<LocalKey<StoreRegistry>>(self.store) as usize
    }

    /// Mark every registered index store as fully rebuilt and query-visible.
    ///
    /// Recovery restores visibility only after rebuild and post-recovery
    /// integrity validation complete successfully.
    pub(in crate::db) fn mark_all_registered_index_stores_ready(
        &self,
    ) -> Result<(), InternalError> {
        self.with_store_registry(|registry| {
            for (_, handle) in registry.iter() {
                handle.mark_index_ready()?;
            }
            Ok::<(), InternalError>(())
        })?;
        Ok(())
    }

    /// Build one storage diagnostics report for registered stores/entities.
    pub(crate) fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, InternalError> {
        diagnostics::storage_report(self, name_to_path)
    }

    /// Build one storage diagnostics report using default entity-path labels.
    pub(crate) fn storage_report_default(&self) -> Result<StorageReport, InternalError> {
        diagnostics::storage_report_default(self)
    }

    // Rebuild one already-authorized marker effect without re-running current
    // accepted relation-target admission.
    pub(in crate::db) fn prepare_row_commit_op_for_replay(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        runtime_entity_catalog::prepare_row_commit(
            self,
            op,
            commit::CommitPrepareMode::RecoveryReplay,
        )
    }

    // Rebuild one complete batch against shared immutable accepted authority.
    pub(in crate::db) fn prepare_row_commit_batch_for_replay(
        &self,
        ops: &[CommitRowOp],
    ) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
        runtime_entity_catalog::prepare_row_commit_batch_for_replay(self, ops)
    }

    // Rebuild live derived state while candidate generations follow their
    // separate durable validation checkpoints.
    pub(in crate::db) fn prepare_row_commit_op_for_rebuild(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        runtime_entity_catalog::prepare_row_commit(
            self,
            op,
            commit::CommitPrepareMode::DerivedRebuild,
        )
    }

    // Validate relation constraints for delete-selected target keys.
    pub(in crate::db) fn validate_delete_relations_with_reader(
        &self,
        target_path: &str,
        deleted_target_keys: &BTreeSet<RawDataStoreKey>,
        source_reader: &dyn index::StructuralPrimaryRowReader,
    ) -> Result<(), InternalError> {
        runtime_entity_catalog::validate_delete_relations(
            self,
            target_path,
            deleted_target_keys,
            source_reader,
        )
    }
}

impl<C: CanisterKind> Db<C> {
    /// Return one deterministic list of registered runtime stores.
    #[must_use]
    pub(crate) fn runtime_store_catalog(&self) -> Vec<StoreCatalogDescription> {
        let mut stores = self.with_store_registry(|registry| {
            registry
                .iter()
                .map(|(store_path, handle)| {
                    StoreCatalogDescription::new(
                        store_path.to_string(),
                        handle
                            .storage_capabilities()
                            .storage_mode()
                            .as_str()
                            .to_string(),
                    )
                })
                .collect::<Vec<_>>()
        });
        icydb_schema::compact_sort_unstable_by(&mut stores, |left, right| {
            left.store_path().cmp(right.store_path())
        });
        stores
    }

    /// Return one deterministic list of registered stable-memory allocations.
    #[must_use]
    pub(crate) fn runtime_memory_catalog(&self) -> Vec<MemoryCatalogDescription> {
        let mut memory = self.with_store_registry(|registry| {
            registry
                .iter()
                .flat_map(|(store_path, handle)| {
                    [
                        handle.data_allocation(),
                        handle.index_allocation(),
                        handle.schema_allocation(),
                        handle.journal_allocation(),
                    ]
                    .into_iter()
                    .flatten()
                    .map(move |allocation| {
                        MemoryCatalogDescription::new(
                            allocation.stable_key().to_string(),
                            allocation.memory_id(),
                            store_path.to_string(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        });
        icydb_schema::compact_sort_unstable_by(&mut memory, |left, right| {
            left.memory_id()
                .cmp(&right.memory_id())
                .then_with(|| left.tag().cmp(right.tag()))
                .then_with(|| left.store_path().cmp(right.store_path()))
        });
        memory
    }

    // Resolve exactly one accepted runtime entity for a persisted tag.
    pub(in crate::db) fn accepted_runtime_entity_for_tag(
        &self,
        entity_tag: EntityTag,
    ) -> Result<runtime_entity_catalog::AcceptedRuntimeEntity, InternalError> {
        runtime_entity_catalog::accepted_runtime_entity_for_tag(self, entity_tag)
    }

    // Resolve exactly one accepted runtime entity for an immutable entity path.
    pub(in crate::db) fn accepted_runtime_entity_for_path(
        &self,
        entity_path: &str,
    ) -> Result<runtime_entity_catalog::AcceptedRuntimeEntity, InternalError> {
        runtime_entity_catalog::accepted_runtime_entity_for_path(self, entity_path)
    }

    // Enumerate deterministic accepted runtime entities across registered stores.
    pub(in crate::db) fn accepted_runtime_entities(
        &self,
    ) -> Result<Vec<runtime_entity_catalog::AcceptedRuntimeEntity>, InternalError> {
        runtime_entity_catalog::accepted_runtime_entities(self)
    }
}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        Self::new(self.store, self.request_scope.clone())
    }
}
