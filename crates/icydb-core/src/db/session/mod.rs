//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod query;
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
        Db, EntityFieldDescription, EntitySchemaDescription, FluentDeleteQuery, FluentLoadQuery,
        IndexState, IntegrityReport, MigrationPlan, MigrationRunOutcome, MissingRowPolicy,
        PersistedRow, Query, QueryError, StorageReport, StoreRegistry, WriteBatchResponse,
        commit::EntityRuntimeHooks,
        data::DataKey,
        executor::{DeleteExecutor, LoadExecutor, SaveExecutor},
        query::plan::VisibleIndexes,
        schema::{
            describe_entity_model, show_indexes_for_model,
            show_indexes_for_model_with_runtime_state,
        },
    },
    error::InternalError,
    metrics::sink::{MetricsSink, with_metrics_sink},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
};
#[cfg(feature = "sql")]
use std::cell::OnceCell;
use std::thread::LocalKey;

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub use sql::SqlQueryExecutionAttribution;
#[cfg(feature = "sql")]
pub use sql::SqlStatementResult;
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
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
    #[cfg(feature = "sql")]
    sql_compiled_command_cache: OnceCell<std::cell::RefCell<sql::SqlCompiledCommandCache>>,
}

impl<C: CanisterKind> DbSession<C> {
    /// Construct one session facade for a database handle.
    #[must_use]
    pub(crate) const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
            #[cfg(feature = "sql")]
            sql_compiled_command_cache: OnceCell::new(),
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
        FluentLoadQuery::new(self, Query::new(MissingRowPolicy::Ignore))
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
        FluentLoadQuery::new(self, Query::new(consistency))
    }

    /// Start a fluent delete query with default missing-row policy (`Ignore`).
    #[must_use]
    pub fn delete<E>(&self) -> FluentDeleteQuery<'_, E>
    where
        E: PersistedRow<Canister = C>,
    {
        FluentDeleteQuery::new(self, Query::new(MissingRowPolicy::Ignore).delete())
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
        FluentDeleteQuery::new(self, Query::new(consistency).delete())
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
        let runtime_state = self.try_index_state_for_store_path(store_path);

        show_indexes_for_model_with_runtime_state(model, runtime_state)
    }

    /// Return one stable list of field descriptors for the entity schema.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: EntityKind<Canister = C>,
    {
        self.show_columns_for_model(E::MODEL)
    }

    /// Return one stable list of field descriptors for one schema model.
    #[must_use]
    pub fn show_columns_for_model(
        &self,
        model: &'static EntityModel,
    ) -> Vec<EntityFieldDescription> {
        describe_entity_model(model).fields().to_vec()
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

    // Best-effort runtime state lookup for metadata surfaces.
    // SHOW INDEXES should stay readable even if one store handle is missing
    // from the registry, so this helper falls back to the pure schema-owned
    // listing instead of turning metadata inspection into an execution error.
    fn try_index_state_for_store_path(&self, store_path: &str) -> Option<IndexState> {
        self.db
            .with_store_registry(|registry| registry.try_get_store(store_path).ok())
            .map(|store| store.index_state())
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

    /// Return one structured schema description for the entity.
    ///
    /// This is a typed `DESCRIBE`-style introspection surface consumed by
    /// developer tooling and pre-EXPLAIN debugging.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: EntityKind<Canister = C>,
    {
        self.describe_entity_model(E::MODEL)
    }

    /// Return one structured schema description for one schema model.
    #[must_use]
    pub fn describe_entity_model(&self, model: &'static EntityModel) -> EntitySchemaDescription {
        describe_entity_model(model)
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

    /// Execute one bounded migration run with durable internal cursor state.
    ///
    /// Migration progress is persisted internally so upgrades/restarts can
    /// resume from the last successful step without external cursor ownership.
    pub fn execute_migration_plan(
        &self,
        plan: &MigrationPlan,
        max_steps: usize,
    ) -> Result<MigrationRunOutcome, InternalError> {
        self.with_metrics(|| self.db.execute_migration_plan(plan, max_steps))
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

/// Remove one entity row from the authoritative data store only.
///
/// This hidden helper exists for stale-index test fixtures that need to keep
/// secondary/index state intact while deleting the base row bytes.
#[doc(hidden)]
pub fn debug_remove_entity_row_data_only<C, E>(
    session: &DbSession<C>,
    key: &E::Key,
) -> Result<bool, InternalError>
where
    C: CanisterKind,
    E: PersistedRow<Canister = C> + EntityValue,
{
    // Phase 1: resolve the store through the recovered session boundary so
    // the helper cannot mutate pre-recovery state.
    let store = session.db.recovered_store(E::Store::PATH)?;

    // Phase 2: remove only the raw row-store entry and compute the canonical
    // storage key that any surviving secondary memberships still point at.
    let data_key = DataKey::try_from_field_value(E::ENTITY_TAG, key)?;
    let raw_key = data_key.to_raw()?;
    let storage_key = data_key.storage_key();

    // Phase 3: preserve the secondary entries but mark any surviving raw
    // memberships as explicitly missing so stale-index fixtures can exercise
    // impossible-state behavior without lying about row existence.
    let removed = store.with_data_mut(|data| data.remove(&raw_key).is_some());
    if !removed {
        return Ok(false);
    }

    store.with_index_mut(|index| index.mark_memberships_missing_for_storage_key(storage_key))?;

    Ok(true)
}

/// Mark one recovered store index with one explicit lifecycle state.
///
/// This hidden helper exists for test fixtures that need to force one index
/// out of the `Ready` state while keeping all other lifecycle plumbing
/// unchanged.
#[doc(hidden)]
pub fn debug_mark_store_index_state<C>(
    session: &DbSession<C>,
    store_path: &str,
    state: IndexState,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve the recovered store so lifecycle mutation cannot
    // target pre-recovery state.
    let store = session.db.recovered_store(store_path)?;

    // Phase 2: apply the explicit lifecycle state directly to the index half
    // of the store pair so tests can observe the `Ready` gate in isolation.
    match state {
        IndexState::Building => store.mark_index_building(),
        IndexState::Ready => store.mark_index_ready(),
        IndexState::Dropping => store.mark_index_dropping(),
    }

    Ok(())
}
