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

use crate::{
    db::{
        Db, EntityFieldDescription, EntityRuntimeHooks, EntitySchemaDescription, FluentDeleteQuery,
        FluentLoadQuery, IndexState, IntegrityReport, MissingRowPolicy, PersistedRow, Query,
        QueryError, StorageReport, StoreRegistry, WriteBatchResponse,
        executor::{DeleteExecutor, EntityAuthority, LoadExecutor, SaveExecutor},
        query::plan::VisibleIndexes,
        schema::{
            AcceptedRowLayoutRuntimeDescriptor, AcceptedSchemaSnapshot, describe_entity_fields,
            describe_entity_fields_with_persisted_schema, describe_entity_model,
            describe_entity_model_with_persisted_schema, ensure_accepted_schema_snapshot,
            show_indexes_for_model, show_indexes_for_model_with_runtime_state,
        },
    },
    error::InternalError,
    metrics::sink::{ExecKind, MetricsSink, record_exec_error_for_path, with_metrics_sink},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
};
use std::thread::LocalKey;

#[cfg(feature = "diagnostics")]
pub use query::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    QueryExecutionAttribution,
};
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
pub(in crate::db) use response::{finalize_scalar_paged_execution, sql_grouped_cursor_from_bytes};
#[cfg(feature = "sql")]
pub use sql::SqlStatementResult;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
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
        if let Err(error) =
            self.with_metrics(|| self.ensure_generated_compatible_accepted_schema_snapshot::<E>())
        {
            self.with_metrics(|| record_exec_error_for_path(ExecKind::Save, E::PATH, &error));

            return Err(error);
        }

        self.execute_save_with_checked_accepted_schema(op, map)
    }

    // Execute save work after the caller has already proven that the accepted
    // schema is generated-compatible. SQL writes use this after their
    // pre-staging schema guard so mutation staging and save execution do not
    // run duplicate schema-store reconciliation in the same statement.
    fn execute_save_with_checked_accepted_schema<E, T, R>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<T, InternalError>,
        map: impl FnOnce(T) -> R,
    ) -> Result<R, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let value = self.with_metrics(|| op(self.save_executor::<E>()))?;

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
        Value::Int(1)
    }

    /// Return one stable, human-readable index listing for the entity schema.
    ///
    /// Output format mirrors SQL-style introspection:
    /// - `PRIMARY KEY (field)`
    /// - `INDEX name (field_a, field_b)`
    /// - `UNIQUE INDEX name (field_a, field_b)`
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

    /// Return one stable list of runtime-registered entity names.
    #[must_use]
    pub fn show_entities(&self) -> Vec<String> {
        self.db.runtime_entity_names()
    }

    /// Return one stable list of runtime-registered entity names.
    ///
    /// `SHOW TABLES` is only an alias for `SHOW ENTITIES`, so the typed
    /// metadata surface keeps the same alias relationship.
    #[must_use]
    pub fn show_tables(&self) -> Vec<String> {
        self.show_entities()
    }

    // Resolve the exact secondary-index set that is visible to planner-owned
    // query planning for one recovered store/model pair.
    fn visible_indexes_for_store_model(
        &self,
        store_path: &str,
        model: &'static EntityModel,
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

        // Phase 2: planner-visible indexes are exactly the model-owned index
        // declarations once the recovered store is query-visible.
        Ok(VisibleIndexes::planner_visible(model.indexes()))
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
        self.ensure_accepted_schema_snapshot_for_authority(EntityAuthority::for_type::<E>())
    }

    // Ensure and return the accepted schema snapshot from already-resolved
    // structural entity authority. SQL and fluent shared-plan cache paths use
    // this shape after lowering has erased the concrete entity type.
    fn ensure_accepted_schema_snapshot_for_authority(
        &self,
        authority: EntityAuthority,
    ) -> Result<AcceptedSchemaSnapshot, InternalError> {
        let store = self.db.recovered_store(authority.store_path())?;

        store.with_schema_mut(|schema_store| {
            ensure_accepted_schema_snapshot(
                schema_store,
                authority.entity_tag(),
                authority.entity_path(),
                authority.model(),
            )
        })
    }

    // Ensure accepted schema metadata and derive the execution authority that
    // consumes it. Keeping the pair together prevents session call sites from
    // mixing a live-schema fingerprint with a generated-only row layout.
    fn ensure_accepted_schema_snapshot_and_authority(
        &self,
        authority: EntityAuthority,
    ) -> Result<(AcceptedSchemaSnapshot, EntityAuthority), InternalError> {
        let accepted_schema = self.ensure_accepted_schema_snapshot_for_authority(authority)?;
        let accepted_row_layout =
            AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted_schema)?;
        let row_shape =
            accepted_row_layout.generated_compatible_row_shape_for_model(authority.model())?;
        let authority = authority.with_generated_compatible_row_shape(row_shape);

        Ok((accepted_schema, authority))
    }

    // Ensure accepted schema metadata is safe for write paths that still encode
    // rows through generated field contracts. Returning only the snapshot keeps
    // SQL write type checks unchanged while the schema-runtime descriptor guard
    // rejects unsupported layout or payload drift before mutation staging.
    fn ensure_generated_compatible_accepted_schema_snapshot<E>(
        &self,
    ) -> Result<AcceptedSchemaSnapshot, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let accepted_schema = self.ensure_accepted_schema_snapshot::<E>()?;
        let accepted_row_layout =
            AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted_schema)?;
        accepted_row_layout.generated_compatible_row_shape_for_model(E::MODEL)?;

        Ok(accepted_schema)
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
    pub(in crate::db) const fn save_executor<E>(&self) -> SaveExecutor<E>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        SaveExecutor::new(self.db, self.debug)
    }
}
