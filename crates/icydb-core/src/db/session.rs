// 3️⃣ Internal imports (implementation wiring)
#[cfg(test)]
use crate::db::{DataStore, IndexStore};
use crate::{
    db::{
        Db, FluentDeleteQuery, FluentLoadQuery, PagedGroupedExecutionWithTrace,
        PagedLoadExecutionWithTrace, PlanError, Query, QueryError, ReadConsistency, Response,
        WriteBatchResponse, WriteResponse,
        cursor::CursorPlanError,
        decode_cursor,
        executor::{DeleteExecutor, ExecutablePlan, ExecutorPlanError, LoadExecutor, SaveExecutor},
        query::intent::QueryMode,
    },
    error::InternalError,
    obs::sink::{MetricsSink, with_metrics_sink},
    traits::{CanisterKind, EntityKind, EntityValue},
    types::{Decimal, Id},
    value::Value,
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
    #[must_use]
    pub const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

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

    #[must_use]
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery::new(self, Query::new(ReadConsistency::MissingOk))
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery::new(self, Query::new(consistency))
    }

    #[must_use]
    pub fn delete<E>(&self) -> FluentDeleteQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentDeleteQuery::new(self, Query::new(ReadConsistency::MissingOk).delete())
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
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

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;

        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::Execute)
    }

    // Shared load-query terminal wrapper: build plan, run under metrics, map
    // execution errors into query-facing errors.
    fn execute_load_query_with<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, ExecutablePlan<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::Execute)
    }

    pub(crate) fn execute_load_query_count<E>(&self, query: &Query<E>) -> Result<u32, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_count(plan))
    }

    pub(crate) fn execute_load_query_exists<E>(&self, query: &Query<E>) -> Result<bool, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_exists(plan))
    }

    pub(crate) fn execute_load_query_min<E>(
        &self,
        query: &Query<E>,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_min(plan))
    }

    pub(crate) fn execute_load_query_max<E>(
        &self,
        query: &Query<E>,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_max(plan))
    }

    pub(crate) fn execute_load_query_min_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_min_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_max_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_max_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_nth_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        nth: usize,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_nth_by(plan, target_field, nth)
        })
    }

    pub(crate) fn execute_load_query_sum_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_sum_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_avg_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_avg_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_median_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_median_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_count_distinct_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<u32, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_count_distinct_by(plan, target_field)
        })
    }

    #[expect(clippy::type_complexity)]
    pub(crate) fn execute_load_query_min_max_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<(Id<E>, Id<E>)>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.aggregate_min_max_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_values_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.values_by(plan, target_field))
    }

    pub(crate) fn execute_load_query_take<E>(
        &self,
        query: &Query<E>,
        take_count: u32,
    ) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.take(plan, take_count))
    }

    pub(crate) fn execute_load_query_top_k_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.top_k_by(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_bottom_k_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.bottom_k_by(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_top_k_by_values<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.top_k_by_values(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_bottom_k_by_values<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.bottom_k_by_values(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_top_k_by_with_ids<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.top_k_by_with_ids(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_bottom_k_by_with_ids<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.bottom_k_by_with_ids(plan, target_field, take_count)
        })
    }

    pub(crate) fn execute_load_query_distinct_values_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.distinct_values_by(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_values_by_with_ids<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| {
            load.values_by_with_ids(plan, target_field)
        })
    }

    pub(crate) fn execute_load_query_first_value_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.first_value_by(plan, target_field))
    }

    pub(crate) fn execute_load_query_last_value_by<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<Option<Value>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.last_value_by(plan, target_field))
    }

    pub(crate) fn execute_load_query_first<E>(
        &self,
        query: &Query<E>,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_first(plan))
    }

    pub(crate) fn execute_load_query_last<E>(
        &self,
        query: &Query<E>,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(query, |load, plan| load.aggregate_last(plan))
    }

    pub(crate) fn execute_load_query_paged_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;
        if plan.as_inner().grouped_plan().is_some() {
            return Err(QueryError::Execute(
                InternalError::query_executor_invariant(
                    "grouped plans require execute_grouped(...)",
                ),
            ));
        }
        let cursor_bytes = match cursor_token {
            Some(token) => Some(decode_cursor(token).map_err(|reason| {
                QueryError::from(PlanError::from(
                    CursorPlanError::InvalidContinuationCursor { reason },
                ))
            })?),
            None => None,
        };
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

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
        let plan = query.plan()?;
        if plan.as_inner().grouped_plan().is_none() {
            return Err(QueryError::Execute(
                InternalError::query_executor_invariant(
                    "execute_grouped requires grouped logical plans",
                ),
            ));
        }
        let cursor_bytes = match cursor_token {
            Some(token) => Some(decode_cursor(token).map_err(|reason| {
                QueryError::from(PlanError::from(
                    CursorPlanError::InvalidContinuationCursor { reason },
                ))
            })?),
            None => None,
        };
        let cursor = plan
            .prepare_grouped_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

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

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.insert_view(view))
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.replace_view(view))
    }

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
