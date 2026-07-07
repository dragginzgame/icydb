//! Module: db
//!
//! Responsibility: root subsystem wiring, façade re-exports, and runtime hook contracts.
//! Does not own: feature semantics delegated to child modules (`query`, `executor`, etc.).
//! Boundary: top-level db API and internal orchestration entrypoints.

pub(crate) mod access;
pub(crate) mod catalog;
pub(crate) mod cursor;
pub(crate) mod diagnostics;
pub(crate) mod identity;
#[cfg(feature = "diagnostics")]
pub(in crate::db) mod physical_access;
pub(crate) mod predicate;
pub(crate) mod query;
pub(crate) mod registry;
pub(crate) mod response;
pub(crate) mod runtime_hooks;
pub(crate) mod scalar_expr;
pub(crate) mod schema;
pub(crate) mod session;
#[cfg(feature = "sql")]
pub(crate) mod sql;

pub(in crate::db) mod codec;
pub(in crate::db) mod commit;
pub(in crate::db) mod data;
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
        executor::Context,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
    types::EntityTag,
};
use std::{collections::BTreeSet, marker::PhantomData, thread::LocalKey};

pub use catalog::{
    EntityCatalogCounts, EntityCatalogDescription, MemoryCatalogDescription,
    StoreCatalogDescription,
};
#[doc(hidden)]
pub use codec::hex::encode_hex_lower;
pub use cursor::{decode_cursor, encode_cursor};
pub use runtime_hooks::EntityRuntimeHooks;
// These hidden helper re-exports remain public so the crate-root `__macro`
// boundary can route generated code through one stable path without widening
// the normal `db` facade contract.
pub use data::{DataStore, PersistedRow, SlotReader, SlotWriter, StructuralPatch};
#[doc(hidden)]
pub use data::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef,
    decode_persisted_many_slot_payload_by_meta, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload_by_kind, decode_persisted_option_slot_payload_by_meta,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload_by_kind,
    decode_persisted_slot_payload_by_meta, decode_persisted_structured_many_slot_payload,
    decode_persisted_structured_slot_payload, decode_slot_into_runtime_value,
    encode_persisted_many_slot_payload_by_meta, encode_persisted_option_scalar_slot_payload,
    encode_persisted_option_slot_payload_by_meta, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload_by_kind, encode_persisted_slot_payload_by_meta,
    encode_persisted_structured_many_slot_payload, encode_persisted_structured_slot_payload,
    encode_runtime_value_into_slot,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use data::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
#[expect(unused_imports)]
pub(crate) use data::{StructuralReadMetrics, with_structural_read_metrics};
pub use diagnostics::{
    DataStoreSnapshot, EntitySnapshot, ExecutionAccessPathVariant, ExecutionMetrics,
    ExecutionOptimization, ExecutionStats, ExecutionTrace, IndexStoreSnapshot, IntegrityReport,
    IntegrityStoreSnapshot, IntegrityTotals, SchemaStoreSnapshot, StorageReport,
    StoreSnapshotStorageMode,
};
#[doc(hidden)]
pub use executor::EntityAuthority;
pub use executor::MutationMode;
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
#[doc(hidden)]
pub use journal::JournalTailStore;
#[doc(hidden)]
pub use key_taxonomy::{
    CompositePrimaryKeyValue, CompositePrimaryKeyValueError, PrimaryKeyComponent, PrimaryKeyValue,
};
pub use predicate::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, MissingRowPolicy, Predicate,
    UnsupportedQueryFeature,
};
#[doc(hidden)]
pub use predicate::{
    parse_generated_index_predicate_sql, validate_generated_index_predicate_fields,
};
pub use query::builder::numeric_projection::{
    NumericProjectionExpr, RoundProjectionExpr, add, div, mul, round, round_expr, sub,
};
pub use query::plan::validate::PlanError;
pub use query::{
    admission::{
        QueryAdmissionAccessKind, QueryAdmissionDecision, QueryAdmissionGroupedSummary,
        QueryAdmissionLane, QueryAdmissionOrdering, QueryAdmissionPlanShape,
        QueryAdmissionRejection, QueryAdmissionResidualFilter, QueryAdmissionSummary,
        QueryBoundKind, QueryMaterializationSummary,
    },
    api::ResponseCardinalityExt,
    builder::{
        AggregateExpr, FieldRef, TextProjectionExpr, ValueProjectionExpr, avg, contains, count,
        count_by, ends_with, exists, first, last, left, length, lower, ltrim, max, max_by, min,
        min_by, position, replace, right, rtrim, starts_with, substring, substring_with_length,
        sum, trim, upper,
    },
    explain::{
        ExplainAccessCandidateV1, ExplainAccessDecisionKind, ExplainAccessDecisionV1,
        ExplainAggregateTerminalPlan, ExplainEligibleAlternativeV1, ExplainExecutionDescriptor,
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
        ExplainExecutionOrderingSource, ExplainPlan, ExplainRejectedIndexV1,
        ExplainResidualSummaryV1, ExplainSelectedAccessV1,
    },
    expr::{FilterExpr, FilterValue, OrderExpr, OrderTerm, asc, desc, field},
    fluent::{
        delete::FluentDeleteQuery,
        load::{FluentLoadQuery, LoadQueryResult, PartialWindowLoadQuery},
    },
    intent::{
        AccessRequirementError, AccessRequirementViolation, CompiledQuery, IntentError,
        PlannedQuery, Query, QueryError, QueryExecutionError, RequiredAccessPath,
    },
    plan::{DeleteSpec, LoadSpec, OrderDirection, QueryMode},
    read_intent::{AdminBatchRequest, ReadIntentKind},
    trace::{QueryTracePlan, TraceExecutionFamily, TraceReuseArtifactClass, TraceReuseEvent},
};
pub use registry::{
    StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
    StoreCommitParticipation, StoreDurability, StoreLiveValidationCapability,
    StoreRecoveryCapability, StoreRegistry, StoreRelationSourceCapability,
    StoreRelationTargetCapability, StoreRuntimeStorageCapabilities, StoreRuntimeStorageMode,
    StoreSchemaMetadataCapability,
};
pub use response::{
    EntityResponse, GroupedRow, PagedGroupedExecution, PagedGroupedExecutionWithTrace,
    PagedLoadExecution, PagedLoadExecutionWithTrace, ProjectedRow, ProjectionResponse,
    Response as RowResponse, ResponseError, ResponseRow, Row, WriteBatchResponse,
};
pub use schema::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaCheckDescription,
    EntitySchemaDescription, SchemaLiteralValidationReason, SchemaStore, SchemaValidationOperator,
    ValidateError,
};
#[cfg(not(feature = "sql"))]
pub use session::DbSession;
#[cfg(feature = "sql")]
pub use session::{
    DbSession, SqlAdminBulkDeletePlan, SqlAdminBulkUpdatePlan, SqlDdlExecutionStatus,
    SqlDdlMutationKind, SqlDdlPreparationReport, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
    SqlDeletePolicyRejection, SqlDeletePolicyReport, SqlDeleteStatementClassification,
    SqlPublicBoundedDeletePlan, SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyDeletePlan,
    SqlPublicPrimaryKeyUpdatePlan, SqlSessionCurrentDeletePlan, SqlSessionCurrentUpdatePlan,
    SqlStatementDispatch, SqlStatementResult, SqlStatementShellSurface, SqlStatementSurface,
    SqlUpdateAssignmentPolicy, SqlUpdateExposurePolicy, SqlUpdatePolicyContext,
    SqlUpdatePolicyRejection, SqlUpdatePolicyReport, SqlUpdateStatementClassification,
    SqlValidatedDeletePlan, SqlValidatedUpdatePlan, SqlWriteExecutionBounds, SqlWriteOrderProof,
    SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteStatementShape, SqlWriteWhereProof,
    classify_sql_delete_policy, classify_sql_update_policy, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(feature = "diagnostics")]
pub use session::{
    DirectDataRowAttribution, FluentTerminalExecutionAttribution, GroupedCountAttribution,
    GroupedExecutionAttribution, KernelRowAttribution, QueryExecutionAttribution,
    ScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use session::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
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

/// Hidden generated-code alias for borrowed structural map entry payload slices.
#[doc(hidden)]
pub type GeneratedStructuralMapPayloadSlices<'a> = Vec<(&'a [u8], &'a [u8])>;

/// Hidden generated-code alias for one decoded enum payload frame.
#[doc(hidden)]
pub type GeneratedStructuralEnumPayload<'a> = (String, Option<String>, Option<&'a [u8]>);

/// Hidden generated-code helper for canonical structural text payload framing.
#[doc(hidden)]
#[must_use]
pub(crate) fn encode_generated_structural_text_payload_bytes(value: &str) -> Vec<u8> {
    data::encode_value_storage_text(value)
}

/// Hidden generated-code helper for canonical structural list payload framing.
#[doc(hidden)]
#[must_use]
pub(crate) fn encode_generated_structural_list_payload_bytes(items: &[&[u8]]) -> Vec<u8> {
    data::encode_value_storage_list_item_slices(items)
}

/// Hidden generated-code helper for canonical structural map payload framing.
#[doc(hidden)]
#[must_use]
pub(crate) fn encode_generated_structural_map_payload_bytes(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    data::encode_value_storage_map_entry_slices(entries)
}

/// Hidden generated-code helper for canonical structural enum payload framing.
#[doc(hidden)]
#[must_use]
pub(crate) fn encode_generated_structural_enum_payload_bytes(
    variant: &str,
    path: Option<&str>,
    payload: Option<&[u8]>,
) -> Vec<u8> {
    data::encode_enum(variant, path, payload)
}

/// Hidden generated-code helper for structural text payload decoding.
#[doc(hidden)]
pub(crate) fn decode_generated_structural_text_payload_bytes(
    raw_bytes: &[u8],
) -> Result<String, InternalError> {
    data::decode_value_storage_text(raw_bytes).map_err(InternalError::persisted_row_decode_failed)
}

/// Hidden generated-code helper for structural list payload decoding.
#[doc(hidden)]
pub(crate) fn decode_generated_structural_list_payload_bytes(
    raw_bytes: &[u8],
) -> Result<Vec<&[u8]>, InternalError> {
    data::decode_value_storage_list_item_slices(raw_bytes)
        .map_err(InternalError::persisted_row_decode_failed)
}

/// Hidden generated-code helper for structural map payload decoding.
#[doc(hidden)]
pub(crate) fn decode_generated_structural_map_payload_bytes(
    raw_bytes: &[u8],
) -> Result<GeneratedStructuralMapPayloadSlices<'_>, InternalError> {
    data::decode_value_storage_map_entry_slices(raw_bytes)
        .map_err(InternalError::persisted_row_decode_failed)
}

/// Hidden generated-code helper for structural enum payload decoding.
#[doc(hidden)]
pub(crate) fn decode_generated_structural_enum_payload_bytes(
    raw_bytes: &[u8],
) -> Result<GeneratedStructuralEnumPayload<'_>, InternalError> {
    data::decode_enum(raw_bytes).map_err(InternalError::persisted_row_decode_failed)
}

/// Hidden generated-code helper for persisted structured payload decode errors.
#[doc(hidden)]
pub(crate) fn generated_persisted_structured_payload_decode_failed(
    detail: impl Sized,
) -> InternalError {
    InternalError::persisted_row_decode_failed(detail)
}

///
/// Db
/// A handle to the set of stores registered for a specific canister domain.
///

pub(crate) struct Db<C: CanisterKind> {
    store: &'static LocalKey<StoreRegistry>,
    entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    /// Construct a db handle without per-entity runtime hooks.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn new(store: &'static LocalKey<StoreRegistry>) -> Self {
        Self::new_with_hooks(store, &[])
    }

    /// Construct a db handle with explicit per-entity runtime hook wiring.
    #[must_use]
    pub(crate) const fn new_with_hooks(
        store: &'static LocalKey<StoreRegistry>,
        entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    ) -> Self {
        #[cfg(debug_assertions)]
        {
            let _ = crate::db::runtime_hooks::debug_assert_unique_runtime_hook_tags(
                entity_runtime_hooks,
            );
        }

        Self {
            store,
            entity_runtime_hooks,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) const fn context<E>(&self) -> Context<'_, E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Context::new(self)
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

    /// Build one named-store resolver for executor/runtime helpers.
    #[must_use]
    pub(in crate::db) fn store_resolver(&self) -> executor::StoreResolver<'_> {
        executor::StoreResolver::new(self)
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

    /// Build one integrity scan report for registered stores/entities.
    pub(crate) fn integrity_report(&self) -> Result<IntegrityReport, InternalError> {
        diagnostics::integrity_report(self)
    }

    pub(in crate::db) fn prepare_row_commit_op(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        runtime_hooks::prepare_row_commit_with_hook(self, self.entity_runtime_hooks, op)
    }

    // Validate strong relation constraints for delete-selected target keys.
    pub(crate) fn validate_delete_strong_relations(
        &self,
        target_path: &str,
        deleted_target_keys: &BTreeSet<RawDataStoreKey>,
    ) -> Result<(), InternalError> {
        runtime_hooks::validate_delete_strong_relations_with_hooks(
            self,
            self.entity_runtime_hooks,
            target_path,
            deleted_target_keys,
        )
    }
}

impl<C: CanisterKind> Db<C> {
    /// Return whether this db has any registered runtime hook callbacks.
    #[must_use]
    pub(crate) const fn has_runtime_hooks(&self) -> bool {
        runtime_hooks::has_runtime_hooks(self.entity_runtime_hooks)
    }

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

    // Resolve exactly one runtime hook for a persisted entity tag.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_tag(
        &self,
        entity_tag: EntityTag,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        runtime_hooks::resolve_runtime_hook_by_tag(self.entity_runtime_hooks, entity_tag)
    }

    // Resolve exactly one runtime hook for a persisted entity path.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_path(
        &self,
        entity_path: &str,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        runtime_hooks::resolve_runtime_hook_by_path(self.entity_runtime_hooks, entity_path)
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}
