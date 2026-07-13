//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod accepted_schema;
mod bounded_cache;
mod catalog;
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
        Db, EntityRuntimeHooks, FluentDeleteQuery, FluentLoadQuery, MissingRowPolicy, PersistedRow,
        Query, StoreRegistry, WriteBatchResponse,
        commit::CommitSchemaFingerprint,
        executor::{DeleteExecutor, LoadExecutor, SaveExecutor},
        schema::{AcceptedRowDecodeContract, SchemaInfo},
    },
    error::InternalError,
    metrics::sink::{ExecKind, MetricsSink, record_exec_error_for_path, with_metrics_sink},
    traits::{CanisterKind, EntityKind, EntityValue},
    value::Value,
};
use std::thread::LocalKey;

#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use accepted_schema::AcceptedCatalogRuntimeCounterSnapshot;
pub(in crate::db) use accepted_schema::AcceptedSchemaCatalogContext;
pub(in crate::db) use query::{
    AcceptedExecutionOutput, AcceptedIdValuesOutput, AcceptedOptionalValueOutput,
    AcceptedValuesOutput,
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
