//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod query;
mod response;
#[cfg(feature = "sql")]
mod sql;
///
/// TESTS
///
#[cfg(all(test, feature = "sql"))]
mod tests;
mod write;

#[cfg(feature = "sql")]
use crate::db::{IndexState, QueryError, query::plan::VisibleIndexes};
use crate::{
    db::{
        Db, EntityFieldDescription, EntityRuntimeHooks, EntitySchemaDescription, FluentDeleteQuery,
        FluentLoadQuery, IntegrityReport, MissingRowPolicy, PersistedRow, Query, StorageReport,
        StoreCatalogDescription, StoreRegistry, WriteBatchResponse,
        commit::CommitSchemaFingerprint,
        executor::{DeleteExecutor, EntityAuthority, LoadExecutor, SaveExecutor},
        schema::{
            AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, AcceptedRowDecodeContract,
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, SchemaInfo, SchemaVersion,
            accepted_commit_schema_fingerprint, accepted_schema_cache_fingerprint,
            describe_entity_fields, describe_entity_fields_with_persisted_schema,
            describe_entity_model, describe_entity_model_with_persisted_schema,
            ensure_accepted_schema_snapshot, show_indexes_for_model,
            show_indexes_for_model_with_runtime_state,
            show_indexes_for_schema_info_with_runtime_state,
        },
    },
    error::InternalError,
    metrics::sink::{ExecKind, MetricsSink, record_exec_error_for_path, with_metrics_sink},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
};
use std::{
    cell::{OnceCell, RefCell},
    collections::HashMap,
    thread::LocalKey,
};

#[cfg(feature = "diagnostics")]
pub use query::{
    DirectDataRowAttribution, FluentTerminalExecutionAttribution, GroupedCountAttribution,
    GroupedExecutionAttribution, KernelRowAttribution, QueryExecutionAttribution,
    ScalarAggregateAttribution,
};
pub(in crate::db) use response::finalize_scalar_paged_execution;
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
#[cfg(feature = "sql")]
pub(in crate::db) use response::sql_grouped_cursor_from_bytes;
#[cfg(feature = "sql")]
pub use sql::{
    SqlAdminBulkDeletePlan, SqlAdminBulkUpdatePlan, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
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
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics};

///
/// DbSession
///
/// Session-scoped database handle with policy (debug, metrics) and execution routing.
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
    metrics: Option<&'static dyn MetricsSink>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogRuntimeCounterSnapshot {
    pub schema_info_projections: u64,
    pub persisted_schema_decodes: u64,
    pub generated_compatible_row_layout_proofs: u64,
    pub latest_by_entity_calls: u64,
    pub visible_index_projections: u64,
}

#[derive(Clone, Debug)]
struct AcceptedSchemaQueryCacheEntry {
    snapshot: AcceptedSchemaSnapshot,
    identity: AcceptedCatalogIdentity,
}

pub(in crate::db) type AcceptedSaveContract = (
    AcceptedRowDecodeContract,
    AcceptedRowDecodeContract,
    SchemaInfo,
    CommitSchemaFingerprint,
);

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedSchemaCatalogContext {
    snapshot: AcceptedSchemaSnapshot,
    identity: AcceptedCatalogIdentity,
    schema_info: OnceCell<SchemaInfo>,
}

impl AcceptedSchemaCatalogContext {
    #[must_use]
    pub(in crate::db) const fn new(
        snapshot: AcceptedSchemaSnapshot,
        identity: AcceptedCatalogIdentity,
    ) -> Self {
        Self {
            snapshot,
            identity,
            schema_info: OnceCell::new(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn snapshot(&self) -> &AcceptedSchemaSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub(in crate::db) const fn schema_version(&self) -> SchemaVersion {
        self.identity.accepted_schema_version()
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> CommitSchemaFingerprint {
        self.identity.accepted_schema_fingerprint()
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint_method_version(&self) -> u8 {
        self.identity.fingerprint_method_version()
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity
    }

    fn debug_assert_matches_entity<E>(&self)
    where
        E: EntityKind,
    {
        debug_assert_eq!(self.identity.entity_tag(), E::ENTITY_TAG);
        debug_assert_eq!(self.identity.entity_path(), E::PATH);
        debug_assert_eq!(self.identity.store_path(), E::Store::PATH);
    }

    pub(in crate::db) fn accepted_entity_authority_for<E>(
        &self,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();

        self.accepted_entity_authority_for_schema_info::<E>(schema_info)
    }

    fn accepted_entity_authority_for_schema_info<E>(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind,
    {
        self.debug_assert_matches_entity::<E>();
        let authority = EntityAuthority::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH);
        let (accepted_row_layout, row_proof) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &self.snapshot,
                authority.model(),
            )?;
        let row_decode_contract = accepted_row_layout.row_decode_contract();

        Ok(
            authority.with_accepted_row_decode_contract(
                row_proof,
                row_decode_contract,
                schema_info,
            ),
        )
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_entity_authority_and_schema_info_for<E>(
        &self,
    ) -> Result<(EntityAuthority, SchemaInfo), InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();
        let authority = self.accepted_entity_authority_for_schema_info::<E>(schema_info.clone())?;

        Ok((authority, schema_info))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_or_provided_entity_authority_and_schema_info_for<E>(
        &self,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(EntityAuthority, SchemaInfo), InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();
        let authority = match accepted_authority {
            Some(authority) => authority.clone(),
            None => self.accepted_entity_authority_for_schema_info::<E>(schema_info.clone())?,
        };

        Ok((authority, schema_info))
    }

    #[must_use]
    pub(in crate::db) fn accepted_schema_info_for<E>(&self) -> SchemaInfo
    where
        E: EntityKind,
    {
        self.debug_assert_matches_entity::<E>();
        self.schema_info
            .get_or_init(|| {
                SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(
                    E::MODEL,
                    &self.snapshot,
                    true,
                )
            })
            .clone()
    }
}

pub(in crate::db) fn accepted_save_contract_for_descriptor<E>(
    accepted_schema: &AcceptedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<AcceptedSaveContract, InternalError>
where
    E: EntityKind,
{
    let row_decode_contract = descriptor.row_decode_contract();
    let mutation_row_decode_contract = row_decode_contract.clone();
    let schema_info = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, accepted_schema);
    let schema_fingerprint = accepted_commit_schema_fingerprint(accepted_schema)?;

    Ok((
        row_decode_contract,
        mutation_row_decode_contract,
        schema_info,
        schema_fingerprint,
    ))
}

thread_local! {
    // Query-side SQL/fluent cache setup needs accepted runtime schema authority,
    // but repeated read calls should not reload the stable schema snapshot just
    // to prove an already-warmed cache key. SQL DDL publication invalidates this
    // heap cache before the next query observes the new accepted schema.
    static ACCEPTED_SCHEMA_QUERY_CACHES: RefCell<HashMap<(usize, &'static str), AcceptedSchemaQueryCacheEntry>> =
        RefCell::new(HashMap::default());
}

impl<C: CanisterKind> DbSession<C> {
    /// Construct one session facade for a database handle.
    #[must_use]
    pub(crate) const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    /// Construct one session facade from store registry and runtime hooks.
    #[must_use]
    pub const fn new_with_hooks(
        store: &'static LocalKey<StoreRegistry>,
        entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    ) -> Self {
        Self::new(Db::new_with_hooks(store, entity_runtime_hooks))
    }

    #[cfg(test)]
    pub(in crate::db) fn reset_accepted_catalog_runtime_counters_for_tests() {
        crate::db::schema::reset_accepted_schema_info_projection_count_for_tests();
        crate::db::schema::reset_persisted_schema_snapshot_decode_count_for_tests();
        crate::db::schema::reset_generated_compatible_row_layout_proof_count_for_tests();
        crate::db::schema::reset_latest_raw_snapshots_by_entity_call_count_for_tests();
        query::reset_visible_index_projection_count_for_tests();
    }

    #[cfg(test)]
    pub(in crate::db) fn accepted_catalog_runtime_counter_snapshot_for_tests()
    -> AcceptedCatalogRuntimeCounterSnapshot {
        AcceptedCatalogRuntimeCounterSnapshot {
            schema_info_projections:
                crate::db::schema::accepted_schema_info_projection_count_for_tests(),
            persisted_schema_decodes:
                crate::db::schema::persisted_schema_snapshot_decode_count_for_tests(),
            generated_compatible_row_layout_proofs:
                crate::db::schema::generated_compatible_row_layout_proof_count_for_tests(),
            latest_by_entity_calls:
                crate::db::schema::latest_raw_snapshots_by_entity_call_count_for_tests(),
            visible_index_projections: query::visible_index_projection_count_for_tests(),
        }
    }

    /// Enable debug execution behavior where supported by executors.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    /// Attach one metrics sink for all session-executed operations.
    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.metrics = Some(sink);
        self
    }

    // Shared fluent load wrapper construction keeps the session boundary in
    // one place when load entry points differ only by missing-row policy.
    const fn fluent_load_query<E>(&self, consistency: MissingRowPolicy) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery::new(self, Query::new(consistency))
    }

    // Shared fluent delete wrapper construction keeps the delete-mode handoff
    // explicit at the session boundary instead of reassembling the same query
    // shell in each public entry point.
    fn fluent_delete_query<E>(&self, consistency: MissingRowPolicy) -> FluentDeleteQuery<'_, E>
    where
        E: PersistedRow<Canister = C>,
    {
        FluentDeleteQuery::new(self, Query::new(consistency).delete())
    }

    fn with_metrics<T>(&self, f: impl FnOnce() -> T) -> T {
        if let Some(sink) = self.metrics {
            with_metrics_sink(sink, f)
        } else {
            f()
        }
    }

    // Shared save-facade wrapper keeps metrics wiring and response shaping uniform.
    fn execute_save_with<E, T, R>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<T, InternalError>,
        map: impl FnOnce(T) -> R,
    ) -> Result<R, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (contract, schema_info, schema_fingerprint) = match self
            .with_metrics(|| self.ensure_generated_compatible_accepted_save_schema::<E>())
        {
            Ok(authority) => authority,
            Err(error) => {
                self.with_metrics(|| record_exec_error_for_path(ExecKind::Save, E::PATH, &error));

                return Err(error);
            }
        };
        let value = self.with_metrics(|| {
            op(self.save_executor::<E>(contract, schema_info, schema_fingerprint))
        })?;

        Ok(map(value))
    }

    // Execute save work after the caller has already proven that the accepted
    // row contract is generated-compatible. SQL and structural writes use this
    // after their pre-staging schema guard so mutation staging and save
    // execution do not rerun schema-store reconciliation in the same statement.
    fn execute_save_with_checked_accepted_row_contract<E, T, R>(
        &self,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        op: impl FnOnce(SaveExecutor<E>) -> Result<T, InternalError>,
        map: impl FnOnce(T) -> R,
    ) -> Result<R, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let value = self.with_metrics(|| {
            op(self.save_executor::<E>(
                accepted_row_decode_contract,
                accepted_schema_info,
                accepted_schema_fingerprint,
            ))
        })?;

        Ok(map(value))
    }

    // Shared save-facade wrappers keep response shape explicit at call sites.
    fn execute_save_entity<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<E, InternalError>,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, std::convert::identity)
    }

    fn execute_save_batch<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<Vec<E>, InternalError>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, WriteBatchResponse::new)
    }

    // ---------------------------------------------------------------------
    // Query entry points (public, fluent)
    // ---------------------------------------------------------------------

    /// Start a fluent load query with default missing-row policy (`Ignore`).
    #[must_use]
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        self.fluent_load_query(MissingRowPolicy::Ignore)
    }

    /// Start a fluent load query with explicit missing-row policy.
    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        self.fluent_load_query(consistency)
    }

    /// Start a fluent delete query with default missing-row policy (`Ignore`).
    #[must_use]
    pub fn delete<E>(&self) -> FluentDeleteQuery<'_, E>
    where
        E: PersistedRow<Canister = C>,
    {
        self.fluent_delete_query(MissingRowPolicy::Ignore)
    }

    /// Start a fluent delete query with explicit missing-row policy.
    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> FluentDeleteQuery<'_, E>
    where
        E: PersistedRow<Canister = C>,
    {
        self.fluent_delete_query(consistency)
    }

    /// Return one constant scalar row equivalent to SQL `SELECT 1`.
    ///
    /// This terminal bypasses query planning and access routing entirely.
    #[must_use]
    pub const fn select_one(&self) -> Value {
        Value::Int64(1)
    }

    /// Return one stable, human-readable index listing for the entity schema.
    ///
    /// Output format mirrors SQL-style introspection:
    /// - `PRIMARY KEY (field) [state=ready] [origin=generated]`
    /// - `INDEX name (field_a, field_b) [state=ready] [origin=generated]`
    /// - `UNIQUE INDEX name (field_a, field_b) [state=ready] [origin=generated]`
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: EntityKind<Canister = C>,
    {
        self.show_indexes_for_store_model(E::Store::PATH, E::MODEL)
    }

    /// Return one stable, human-readable index listing for one schema model.
    ///
    /// This model-only helper is schema-owned and intentionally does not
    /// attach runtime lifecycle state because it does not carry store
    /// placement context.
    #[must_use]
    pub fn show_indexes_for_model(&self, model: &'static EntityModel) -> Vec<String> {
        show_indexes_for_model(model)
    }

    /// Return one stable, human-readable index listing for the accepted schema.
    ///
    /// Unlike `show_indexes`, this fallible live-schema helper reflects
    /// accepted DDL-created indexes as well as compiled schema indexes.
    pub fn try_show_indexes<E>(&self) -> Result<Vec<String>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let schema = self.accepted_schema_info_for_entity::<E>()?;

        Ok(self.show_indexes_for_store_schema_info(E::Store::PATH, &schema))
    }

    // Return one stable, human-readable index listing for one resolved
    // store/model pair, attaching the current runtime lifecycle state when the
    // registry can resolve the backing store handle.
    pub(in crate::db) fn show_indexes_for_store_model(
        &self,
        store_path: &str,
        model: &'static EntityModel,
    ) -> Vec<String> {
        let runtime_state = self
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state());

        show_indexes_for_model_with_runtime_state(model, runtime_state)
    }

    // Return one stable, human-readable index listing for one resolved
    // store/accepted-schema pair, attaching the current runtime lifecycle state
    // when the registry can resolve the backing store handle.
    pub(in crate::db) fn show_indexes_for_store_schema_info(
        &self,
        store_path: &str,
        schema: &SchemaInfo,
    ) -> Vec<String> {
        let runtime_state = self
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state());

        show_indexes_for_schema_info_with_runtime_state(schema, runtime_state)
    }

    /// Return one stable generated-model list of field descriptors.
    ///
    /// This infallible Rust metadata helper intentionally reports the compiled
    /// schema model. Use `try_show_columns` for the accepted persisted-schema
    /// view used by SQL and diagnostics surfaces.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: EntityKind<Canister = C>,
    {
        self.show_columns_for_model(E::MODEL)
    }

    /// Return one stable generated-model list of field descriptors.
    #[must_use]
    pub fn show_columns_for_model(
        &self,
        model: &'static EntityModel,
    ) -> Vec<EntityFieldDescription> {
        describe_entity_fields(model)
    }

    /// Return field descriptors using the accepted persisted schema snapshot.
    ///
    /// This fallible variant is intended for SQL and diagnostics surfaces that
    /// can report schema reconciliation failures. The infallible
    /// `show_columns` helper remains generated-model based.
    pub fn try_show_columns<E>(&self) -> Result<Vec<EntityFieldDescription>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let snapshot = self.ensure_accepted_schema_snapshot::<E>()?;

        Ok(describe_entity_fields_with_persisted_schema(&snapshot))
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    ///
    /// # Panics
    ///
    /// Panics if the runtime session cannot read its registered entity catalog.
    /// Use `try_show_entities` when the caller can report the internal error.
    #[must_use]
    pub fn show_entities(&self) -> Vec<crate::db::EntityCatalogDescription> {
        self.try_show_entities().expect("session invariant")
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    pub fn try_show_entities(
        &self,
    ) -> Result<Vec<crate::db::EntityCatalogDescription>, InternalError> {
        self.db.runtime_entity_catalog()
    }

    /// Return one stable list of runtime-registered stores.
    #[must_use]
    pub fn show_stores(&self) -> Vec<StoreCatalogDescription> {
        self.db.runtime_store_catalog()
    }

    /// Return one stable list of runtime-registered stable-memory allocations.
    #[must_use]
    pub fn show_memory(&self) -> Vec<crate::db::MemoryCatalogDescription> {
        self.db.runtime_memory_catalog()
    }

    // Resolve the exact secondary-index set that is visible to planner-owned
    // query planning for one recovered store and accepted schema pair.
    #[cfg(feature = "sql")]
    fn visible_indexes_for_store_accepted_schema(
        &self,
        store_path: &str,
        schema_info: &SchemaInfo,
    ) -> Result<VisibleIndexes<'static>, QueryError> {
        // Phase 1: resolve the recovered store state once at the session
        // boundary so query/executor planning does not reopen lifecycle checks.
        let store = self
            .db
            .recovered_store(store_path)
            .map_err(QueryError::execute)?;
        let state = store.index_state();
        if state != IndexState::Ready {
            return Ok(VisibleIndexes::none());
        }
        debug_assert_eq!(state, IndexState::Ready);

        // Phase 2: planner-visible indexes are accepted schema contracts once
        // the recovered store is query-visible.
        let visible_indexes = VisibleIndexes::accepted_schema_visible(schema_info);
        debug_assert!(visible_indexes.accepted_field_path_contracts_are_consistent());
        debug_assert!(visible_indexes.accepted_expression_contracts_are_consistent());
        debug_assert_eq!(
            visible_indexes.accepted_expression_index_count(),
            Some(visible_indexes.accepted_expression_indexes().len()),
        );

        Ok(visible_indexes)
    }

    /// Return one generated-model schema description for the entity.
    ///
    /// This is a typed `DESCRIBE`-style introspection surface consumed by
    /// developer tooling and pre-EXPLAIN debugging when a non-failing compiled
    /// schema view is required.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: EntityKind<Canister = C>,
    {
        self.describe_entity_model(E::MODEL)
    }

    /// Return one generated-model schema description for one schema model.
    #[must_use]
    pub fn describe_entity_model(&self, model: &'static EntityModel) -> EntitySchemaDescription {
        describe_entity_model(model)
    }

    /// Return a schema description using the accepted persisted schema snapshot.
    ///
    /// This is the live-schema counterpart to `describe_entity`. It is fallible
    /// because loading accepted schema authority can fail if startup
    /// reconciliation rejects the stored metadata.
    pub fn try_describe_entity<E>(&self) -> Result<EntitySchemaDescription, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let snapshot = self.ensure_accepted_schema_snapshot::<E>()?;

        Ok(describe_entity_model_with_persisted_schema(
            E::MODEL,
            &snapshot,
        ))
    }

    // Ensure and return the accepted schema snapshot for one generated entity.
    // This may write the first snapshot for an empty store; otherwise it loads
    // the latest stored snapshot and applies the current exact-match policy.
    fn ensure_accepted_schema_snapshot<E>(&self) -> Result<AcceptedSchemaSnapshot, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)
        })
    }

    // Load the current accepted schema snapshot for read/query paths without
    // rerunning generated proposal reconciliation on every cold query call.
    // Startup and write paths still own reconciliation; the fallback only keeps
    // first-use test stores and freshly initialized local stores functional.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_query<E>(
        &self,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = (self.db.cache_scope_id(), E::PATH);
        if let Some(entry) =
            ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| cache.borrow().get(&cache_key).cloned())
        {
            return Ok(AcceptedSchemaCatalogContext::new(
                entry.snapshot,
                entry.identity,
            ));
        }

        let snapshot = self.load_accepted_schema_snapshot_for_query::<E>()?;
        let fingerprint = accepted_schema_cache_fingerprint(&snapshot)?;
        let identity = AcceptedCatalogIdentity::new(
            E::ENTITY_TAG,
            E::PATH,
            E::Store::PATH,
            snapshot.persisted_snapshot().version(),
            fingerprint,
        );
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().insert(
                cache_key,
                AcceptedSchemaQueryCacheEntry {
                    snapshot: snapshot.clone(),
                    identity,
                },
            );
        });

        Ok(AcceptedSchemaCatalogContext::new(snapshot, identity))
    }

    pub(in crate::db::session) fn accepted_catalog_snapshot_selection_for_query<E>(
        &self,
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            schema_store.latest_catalog_identity(E::ENTITY_TAG, E::PATH, E::Store::PATH)
        })
    }

    pub(in crate::db::session) fn accepted_schema_catalog_context_from_selection<E>(
        &self,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = (self.db.cache_scope_id(), E::PATH);
        if let Some(entry) =
            ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| cache.borrow().get(&cache_key).cloned())
            && entry.identity == selection.identity()
        {
            return Ok(Some(AcceptedSchemaCatalogContext::new(
                entry.snapshot,
                entry.identity,
            )));
        }

        let snapshot = selection.decode_verified()?;
        if snapshot.persisted_snapshot().fields().len() != E::MODEL.fields().len() {
            return Ok(None);
        }
        let context = AcceptedSchemaCatalogContext::new(snapshot.clone(), selection.identity());

        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().insert(
                cache_key,
                AcceptedSchemaQueryCacheEntry {
                    snapshot,
                    identity: selection.identity(),
                },
            );
        });

        Ok(Some(context))
    }

    #[cfg(feature = "sql")]
    fn invalidate_accepted_schema_query_cache_for_entity<E>(&self)
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = (self.db.cache_scope_id(), E::PATH);
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().remove(&cache_key);
        });
    }

    fn load_accepted_schema_snapshot_for_query<E>(
        &self,
    ) -> Result<AcceptedSchemaSnapshot, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            if let Some(snapshot) = schema_store.latest_persisted_snapshot(E::ENTITY_TAG)? {
                let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
                if AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                    &accepted,
                    E::MODEL,
                )
                .is_ok()
                {
                    return Ok(accepted);
                }
            }

            ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)
        })
    }

    // Build the accepted schema-info projection for one typed entity. Fluent
    // terminal adapters use this before constructing slot-bound descriptors so
    // field slot authority comes from the accepted schema snapshot.
    pub(in crate::db) fn accepted_schema_info_for_entity<E>(
        &self,
    ) -> Result<SchemaInfo, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let catalog = self.accepted_schema_catalog_context_for_query::<E>()?;

        Ok(catalog.accepted_schema_info_for::<E>())
    }

    // Derive typed executor authority from an accepted snapshot the caller
    // already loaded, avoiding a second schema-store pass in SQL write/select
    // adapters that need both write descriptors and read selector authority.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_entity_authority_for_schema<E>(
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        EntityAuthority::from_accepted_schema_for_type::<E>(accepted_schema)
    }

    // Ensure accepted schema metadata is safe for write paths that still encode
    // rows through generated field contracts. Returning only the snapshot keeps
    // SQL write type checks unchanged while the schema-runtime contract guard
    // rejects unsupported layout or payload drift before mutation staging.
    fn ensure_generated_compatible_accepted_save_schema<E>(
        &self,
    ) -> Result<
        (
            AcceptedRowDecodeContract,
            SchemaInfo,
            CommitSchemaFingerprint,
        ),
        InternalError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let accepted_schema = self.ensure_accepted_schema_snapshot::<E>()?;
        let (accepted_row_layout, _) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &accepted_schema,
                E::MODEL,
            )?;
        let (row_decode_contract, _, schema_info, schema_fingerprint) =
            accepted_save_contract_for_descriptor::<E>(&accepted_schema, &accepted_row_layout)?;

        Ok((row_decode_contract, schema_info, schema_fingerprint))
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, InternalError> {
        self.db.storage_report(name_to_path)
    }

    /// Build one point-in-time storage report using default entity-path labels.
    pub fn storage_report_default(&self) -> Result<StorageReport, InternalError> {
        self.db.storage_report_default()
    }

    /// Build one point-in-time integrity scan report for observability endpoints.
    pub fn integrity_report(&self) -> Result<IntegrityReport, InternalError> {
        self.db.integrity_report()
    }

    // ---------------------------------------------------------------------
    // Low-level executors (crate-internal; execution primitives)
    // ---------------------------------------------------------------------

    #[must_use]
    pub(in crate::db) const fn load_executor<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(in crate::db) const fn delete_executor<E>(&self) -> DeleteExecutor<E>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        DeleteExecutor::new(self.db)
    }

    #[must_use]
    pub(in crate::db) const fn save_executor<E>(
        &self,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
    ) -> SaveExecutor<E>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        SaveExecutor::new_with_accepted_contract(
            self.db,
            self.debug,
            accepted_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        )
    }
}
