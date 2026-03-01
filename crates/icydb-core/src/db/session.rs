//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

#[cfg(test)]
use crate::db::{DataStore, IndexStore};
use crate::{
    db::{
        Db, FluentDeleteQuery, FluentLoadQuery, MissingRowPolicy, PagedGroupedExecutionWithTrace,
        PagedLoadExecutionWithTrace, PlanError, Query, QueryError, Response, WriteBatchResponse,
        WriteResponse,
        cursor::CursorPlanError,
        decode_cursor,
        executor::{DeleteExecutor, ExecutablePlan, ExecutorPlanError, LoadExecutor, SaveExecutor},
        query::intent::QueryMode,
    },
    error::InternalError,
    obs::sink::{MetricsSink, with_metrics_sink},
    traits::{CanisterKind, EntityKind, EntityValue},
};

// Map executor-owned plan-surface failures into query-owned plan errors.
fn map_executor_plan_error(err: ExecutorPlanError) -> QueryError {
    QueryError::from(err.into_plan_error())
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
    pub const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
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
        E: EntityKind<Canister = C> + EntityValue,
    {
        let value = self.with_metrics(|| op(self.save_executor::<E>()))?;

        Ok(map(value))
    }

    // Shared save-facade wrappers keep response shape explicit at call sites.
    fn execute_save_entity<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<E, InternalError>,
    ) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, WriteResponse::new)
    }

    fn execute_save_batch<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<Vec<E>, InternalError>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, WriteBatchResponse::new)
    }

    fn execute_save_view<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<E::ViewType, InternalError>,
    ) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, std::convert::identity)
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
        E: EntityKind<Canister = C>,
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
        E: EntityKind<Canister = C>,
    {
        FluentDeleteQuery::new(self, Query::new(consistency).delete())
    }

    // ---------------------------------------------------------------------
    // Low-level executors (crate-internal; execution primitives)
    // ---------------------------------------------------------------------

    #[must_use]
    pub(crate) const fn load_executor<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn delete_executor<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn save_executor<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    // ---------------------------------------------------------------------
    // Query diagnostics / execution (internal routing)
    // ---------------------------------------------------------------------

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();

        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::Execute)
    }

    // Shared load-query terminal wrapper: build plan, run under metrics, map
    // execution errors into query-facing errors.
    pub(crate) fn execute_load_query_with<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, ExecutablePlan<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::Execute)
    }

    /// Execute one scalar paged load query and return optional continuation cursor plus trace.
    pub(crate) fn execute_load_query_paged_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate executable plan and reject grouped plans.
        let plan = query.plan()?.into_executable();
        if plan.as_inner().grouped_plan().is_some() {
            return Err(QueryError::Execute(
                InternalError::query_executor_invariant(
                    "grouped plans require execute_grouped(...)",
                ),
            ));
        }

        // Phase 2: decode external cursor token and validate it against plan surface.
        let cursor_bytes = match cursor_token {
            Some(token) => Some(decode_cursor(token).map_err(|reason| {
                QueryError::from(PlanError::from(
                    CursorPlanError::invalid_continuation_cursor(reason),
                ))
            })?),
            None => None,
        };
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

        // Phase 3: execute one traced page and encode outbound continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::Execute)?;
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_scalar() else {
                    return Err(QueryError::Execute(
                        InternalError::query_executor_invariant(
                            "scalar load pagination emitted grouped continuation token",
                        ),
                    ));
                };

                token.encode().map_err(|err| {
                    QueryError::Execute(InternalError::serialize_internal(format!(
                        "failed to serialize continuation cursor: {err}"
                    )))
                })
            })
            .transpose()?;

        Ok(PagedLoadExecutionWithTrace::new(
            page.items,
            next_cursor,
            trace,
        ))
    }

    /// Execute one grouped query page with optional grouped continuation cursor.
    ///
    /// This is the explicit grouped execution boundary; scalar load APIs reject
    /// grouped plans to preserve scalar response contracts.
    pub fn execute_grouped<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate executable plan and require grouped shape.
        let plan = query.plan()?.into_executable();
        if plan.as_inner().grouped_plan().is_none() {
            return Err(QueryError::Execute(
                InternalError::query_executor_invariant(
                    "execute_grouped requires grouped logical plans",
                ),
            ));
        }

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor_bytes = match cursor_token {
            Some(token) => Some(decode_cursor(token).map_err(|reason| {
                QueryError::from(PlanError::from(
                    CursorPlanError::invalid_continuation_cursor(reason),
                ))
            })?),
            None => None,
        };
        let cursor = plan
            .prepare_grouped_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

        // Phase 3: execute grouped page and encode outbound grouped continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_grouped_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::Execute)?;
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_grouped() else {
                    return Err(QueryError::Execute(
                        InternalError::query_executor_invariant(
                            "grouped pagination emitted scalar continuation token",
                        ),
                    ));
                };

                token.encode().map_err(|err| {
                    QueryError::Execute(InternalError::serialize_internal(format!(
                        "failed to serialize grouped continuation cursor: {err}"
                    )))
                })
            })
            .transpose()?;

        Ok(PagedGroupedExecutionWithTrace::new(
            page.rows,
            next_cursor,
            trace,
        ))
    }

    // ---------------------------------------------------------------------
    // High-level write API (public, intent-level)
    // ---------------------------------------------------------------------

    /// Insert one entity row.
    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.insert(entity))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_atomic(entities))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_non_atomic(entities))
    }

    /// Replace one existing entity row.
    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.replace(entity))
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_atomic(entities))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_non_atomic(entities))
    }

    /// Update one existing entity row.
    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.update(entity))
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_atomic(entities))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_non_atomic(entities))
    }

    /// Insert one view value and return the stored view.
    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.insert_view(view))
    }

    /// Replace one view value and return the stored view.
    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.replace_view(view))
    }

    /// Update one view value and return the stored view.
    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.update_view(view))
    }

    /// TEST ONLY: clear all registered data and index stores for this database.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn clear_stores_for_tests(&self) {
        self.db.with_store_registry(|reg| {
            // Test cleanup only: clearing all stores is set-like and does not
            // depend on registry iteration order.
            for (_, store) in reg.iter() {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
            }
        });
    }
}
