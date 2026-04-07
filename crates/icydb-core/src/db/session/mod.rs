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
        IntegrityReport, MigrationPlan, MigrationRunOutcome, MissingRowPolicy, PersistedRow, Query,
        QueryError, StorageReport, StoreRegistry, WriteBatchResponse,
        commit::EntityRuntimeHooks,
        cursor::{decode_optional_cursor_token, decode_optional_grouped_cursor_token},
        data::DataKey,
        executor::{DeleteExecutor, LoadExecutor, SaveExecutor},
        schema::{describe_entity_model, show_indexes_for_model},
    },
    error::InternalError,
    metrics::sink::{MetricsSink, with_metrics_sink},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
};
use std::thread::LocalKey;

#[cfg(feature = "sql")]
pub use sql::{SqlDispatchResult, SqlParsedStatement, SqlStatementRoute};

// Decode one optional external cursor token and map decode failures into the
// query-plan cursor error boundary.
fn decode_optional_cursor_bytes(cursor_token: Option<&str>) -> Result<Option<Vec<u8>>, QueryError> {
    decode_optional_cursor_token(cursor_token).map_err(QueryError::from_cursor_plan_error)
}

// Decode one optional grouped continuation token through the existing cursor
// text boundary while preserving grouped-token ownership for grouped resume.
fn decode_optional_grouped_cursor(
    cursor_token: Option<&str>,
) -> Result<Option<crate::db::cursor::GroupedContinuationToken>, QueryError> {
    decode_optional_grouped_cursor_token(cursor_token).map_err(QueryError::from_cursor_plan_error)
}

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
        self.show_indexes_for_model(E::MODEL)
    }

    /// Return one stable, human-readable index listing for one schema model.
    #[must_use]
    pub fn show_indexes_for_model(&self, model: &'static EntityModel) -> Vec<String> {
        show_indexes_for_model(model)
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
        DeleteExecutor::new(self.db, self.debug)
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

    // Phase 2: remove only the raw row-store entry, leaving index state
    // untouched so stale-row fallback tests can exercise the fail-closed path.
    let data_key = DataKey::try_from_field_value(E::ENTITY_TAG, key)?;
    let raw_key = data_key.to_raw()?;

    Ok(store.with_data_mut(|data| data.remove(&raw_key).is_some()))
}
