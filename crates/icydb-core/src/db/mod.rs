//! Module: db
//!
//! Responsibility: root subsystem wiring, façade re-exports, and entity
//! registration contracts.
//! Does not own: feature semantics delegated to child modules (`query`, `executor`, etc.).
//! Boundary: top-level db API and internal orchestration entrypoints.

pub(crate) mod access;
pub(crate) mod catalog;
pub(crate) mod cursor;
pub(crate) mod diagnostics;
mod dynamic_write;
pub(crate) mod entity_registration;
pub(crate) mod identity;
pub(crate) mod integrity;
#[cfg(feature = "diagnostics")]
pub(in crate::db) mod physical_access;
pub(crate) mod predicate;
pub(crate) mod query;
pub(crate) mod registry;
pub(crate) mod response;
pub(crate) mod scalar_expr;
pub(crate) mod schema;
pub(crate) mod session;
#[cfg(feature = "sql")]
pub(crate) mod sql;
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
pub(in crate::db) mod relation;
pub(in crate::db) mod sql_shared;
#[cfg(test)]
pub(in crate::db) mod test_support;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        commit::{CommitRowOp, PreparedRowCommitOp, ensure_recovered},
        data::RawDataStoreKey,
        registry::StoreHandle,
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
pub use data::DataStore;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use data::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
#[expect(unused_imports)]
pub(crate) use data::{StructuralReadMetrics, with_structural_read_metrics};
pub use diagnostics::{
    DataStoreSnapshot, EntitySnapshot, ExecutionAccessPathVariant, ExecutionMetrics,
    ExecutionOptimization, ExecutionStats, ExecutionTrace, IndexStoreSnapshot, SchemaStoreSnapshot,
    StorageReport, StoreSnapshotStorageMode,
};
#[doc(hidden)]
pub use dynamic_write::{DynamicMutation, DynamicStructuralPatch, DynamicWriteCell};
pub use dynamic_write::{
    DynamicMutationResult, DynamicTypedBindingError, DynamicTypedEntityBinding,
    DynamicTypedFieldBindingRequest, DynamicTypedFieldType,
};
pub use entity_registration::EntityRegistration;
pub use executor::{ExecutionFamily, RouteExecutionMode};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use executor::{RowCheckMetrics, with_row_check_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
#[expect(unused_imports)]
pub(crate) use executor::{RowCheckMetrics, with_row_check_metrics};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use executor::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
#[expect(unused_imports)]
pub(crate) use executor::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
pub use identity::{EntityName, IndexName};
pub use index::{IndexState, IndexStore};
pub use integrity::{
    DatabaseIncarnationId, DeepIntegrityPage, DeepIntegrityPageStatus, IntegrityAbortReceipt,
    IntegrityAbortStatus, IntegrityAuthorityClass, IntegrityAuthorityDiagnostic,
    IntegrityCheckRequest, IntegrityCheckResult, IntegrityDeepError, IntegrityEntityIdentity,
    IntegrityFinding, IntegrityFindingClass, IntegrityFindingKind, IntegrityJobError,
    IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt, IntegrityPendingTerminal,
    IntegrityPhase, IntegrityResourceDiagnostic, IntegritySeverity, IntegritySubmissionKey,
    IntegrityTerminalOutcome, IntegrityVerifierFamily, QuickIntegrityResult, QuickIntegrityStatus,
};
#[doc(hidden)]
pub use journal::JournalTailStore;
#[doc(hidden)]
pub use key_taxonomy::{
    CompositePrimaryKeyValue, CompositePrimaryKeyValueError, EntityKey, EntityKeyBytes,
    EntityKeyBytesError, KeyValueCodec, PrimaryKeyComponent, PrimaryKeyDecode, PrimaryKeyEncode,
    PrimaryKeyEncodeError, PrimaryKeyValue, ScalarRelationTargetKey,
    ScalarRelationTargetKeyMatchesDeclaredPrimitive, validate_entity_key_bytes_buffer,
};
pub use predicate::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, MissingRowPolicy, Predicate,
};
pub use query::builder::numeric_projection::{
    NumericProjectionExpr, RoundProjectionExpr, add, div, mul, round, round_expr, sub,
};
pub use query::plan::validate::PlanError;
#[cfg(feature = "sql")]
pub use query::{DynamicQuery, DynamicQueryResult};
pub use query::{
    builder::{
        AggregateExpr, FieldRef, TextProjectionExpr, ValueProjectionExpr, avg, contains, count,
        count_by, ends_with, exists, first, last, left, length, lower, ltrim, max, max_by, min,
        min_by, position, replace, right, rtrim, starts_with, substring, substring_with_length,
        sum, trim, upper,
    },
    explain::{
        ExplainAccessCandidate, ExplainAccessDecision, ExplainAccessDecisionKind,
        ExplainAggregateTerminalPlan, ExplainEligibleAlternative, ExplainExecutionDescriptor,
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
        ExplainExecutionOrderingSource, ExplainPlan, ExplainRejectedIndex, ExplainResidualSummary,
        ExplainSelectedAccess,
    },
    expr::{FilterExpr, FilterValue, OrderExpr, OrderTerm, asc, desc, field},
    intent::{
        AccessRequirementError, AccessRequirementViolation, IntentError, QueryError,
        QueryExecutionError, RequiredAccessPath,
    },
    plan::{DeleteSpec, LoadSpec, OrderDirection, QueryMode},
    read_intent::ReadIntentKind,
    trace::{QueryTracePlan, TraceExecutionFamily, TraceReuseEvent},
};
pub use registry::{
    StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
    StoreCommitParticipation, StoreDurability, StoreRecoveryCapability, StoreRegistry,
    StoreRelationSourceCapability, StoreRelationTargetCapability, StoreRuntimeStorageCapabilities,
    StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
};
pub use response::GroupedRow;
#[doc(hidden)]
pub use schema::validate_generated_constraint_name;
pub use schema::{
    ConstraintValidationProgressDescription, EntityConstraintDescription, EntityFieldDescription,
    EntityIndexDescription, EntityRelationCardinality, EntityRelationDescription,
    EntitySchemaCheckDescription, EntitySchemaDescription, SchemaLiteralValidationReason,
    SchemaStore, SchemaValidationOperator, ValidateError,
};
pub use schema::{
    SchemaApplicationStore, SchemaApplicationTarget, SchemaChangeFailure, SchemaChangeJob,
    SchemaChangeJobId, SchemaChangeOutcome, SchemaChangeProgress, SchemaChangeProgressStatus,
    SchemaChangeReceipt, SchemaChangeValidationPhase,
};
#[cfg(not(feature = "sql"))]
pub use session::DbSession;
#[cfg(feature = "sql")]
pub use session::{
    DbSession, SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlIntegrityError, SqlStatementDispatch, SqlStatementResult,
    SqlStatementShellSurface, SqlStatementSurface, TrustedResumableUpdateContinuation,
    TrustedResumableUpdatePhase, TrustedResumableUpdateReceipt,
    TrustedResumableUpdateRestartReason, sql_statement_dispatch, sql_statement_entity_name,
    sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(feature = "diagnostics")]
pub use session::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use session::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use session::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "sql")]
pub use sql::identifier::{
    identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
    split_qualified_identifier,
};
#[cfg(feature = "sql")]
pub use sql::lowering::LoweredSqlCommand;
pub use write_context::MutationMode;

///
/// Db
/// A handle to the set of stores registered for a specific canister domain.
///

pub(crate) struct Db<C: CanisterKind> {
    store: &'static LocalKey<StoreRegistry>,
    entity_registrations: &'static [EntityRegistration<C>],
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    /// Construct a database handle with explicit generated entity registrations.
    #[must_use]
    pub(crate) const fn new_with_registrations(
        store: &'static LocalKey<StoreRegistry>,
        entity_registrations: &'static [EntityRegistration<C>],
    ) -> Self {
        Self {
            store,
            entity_registrations,
            _marker: PhantomData,
        }
    }

    /// Resolve one named store after enforcing startup recovery.
    pub(in crate::db) fn recovered_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        ensure_recovered(self)?;

        self.store_handle(path)
    }

    // Resolve one named store without re-entering recovery.
    //
    // Internal commit/recovery paths already own recovery authority and must
    // not bounce back through `ensure_recovered`, or they can recurse through
    // replay/rebuild preparation.
    pub(in crate::db) fn store_handle(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.with_store_registry(|registry| registry.try_get_store(path))
    }

    /// Ensure startup/in-progress commit recovery has been applied.
    pub(crate) fn ensure_recovered_state(&self) -> Result<(), InternalError> {
        ensure_recovered(self)
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
    pub(in crate::db) fn mark_all_registered_index_stores_ready(&self) {
        self.with_store_registry(|registry| {
            for (_, handle) in registry.iter() {
                handle.mark_index_ready();
            }
        });
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

    pub(in crate::db) fn prepare_row_commit_op(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        entity_registration::prepare_row_commit_with_registration(
            self,
            self.entity_registrations,
            op,
        )
    }

    // Rebuild live derived state while candidate generations follow their
    // separate durable validation checkpoints.
    pub(in crate::db) fn prepare_row_commit_op_for_rebuild(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        entity_registration::prepare_row_commit_with_registration_for_rebuild(
            self,
            self.entity_registrations,
            op,
        )
    }

    // Validate relation constraints for delete-selected target keys.
    pub(crate) fn validate_delete_relations(
        &self,
        target_path: &str,
        deleted_target_keys: &BTreeSet<RawDataStoreKey>,
    ) -> Result<(), InternalError> {
        entity_registration::validate_delete_relations_with_registrations(
            self,
            self.entity_registrations,
            target_path,
            deleted_target_keys,
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
        stores.sort_by(|left, right| left.store_path().cmp(right.store_path()));
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
        memory.sort_by(|left, right| {
            left.memory_id()
                .cmp(&right.memory_id())
                .then_with(|| left.tag().cmp(right.tag()))
                .then_with(|| left.store_path().cmp(right.store_path()))
        });
        memory
    }

    // Resolve exactly one model-free runtime registration for a persisted tag.
    pub(in crate::db) fn runtime_registration_for_entity_tag(
        &self,
        entity_tag: EntityTag,
    ) -> Result<entity_registration::EntityRuntimeRegistration<C>, InternalError> {
        entity_registration::resolve_runtime_registration_by_tag(
            self,
            self.entity_registrations,
            entity_tag,
        )
    }

    // Resolve exactly one model-free runtime registration for an entity path.
    pub(in crate::db) fn runtime_registration_for_entity_path(
        &self,
        entity_path: &str,
    ) -> Result<entity_registration::EntityRuntimeRegistration<C>, InternalError> {
        entity_registration::resolve_runtime_registration_by_path(
            self,
            self.entity_registrations,
            entity_path,
        )
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}
