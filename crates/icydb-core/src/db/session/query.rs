//! Module: db::session::query
//! Responsibility: module-local ownership and contracts for db::session::query.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, EntityResponse, PagedGroupedExecutionWithTrace, PagedLoadExecutionWithTrace,
        PersistedRow, Query, QueryError, QueryTracePlan, TraceExecutionStrategy,
        access::AccessStrategy,
        cursor::CursorPlanError,
        executor::{ExecutablePlan, ExecutionStrategy, LoadExecutor},
        query::plan::QueryMode,
        session::decode_optional_cursor_bytes,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Validate that one execution strategy is admissible for scalar paged load
    // execution and fail closed on grouped/primary-key-only routes.
    fn ensure_scalar_paged_execution_strategy(
        strategy: ExecutionStrategy,
    ) -> Result<(), QueryError> {
        match strategy {
            ExecutionStrategy::PrimaryKey => Err(QueryError::invariant(
                CursorPlanError::cursor_requires_explicit_or_grouped_ordering_message(),
            )),
            ExecutionStrategy::Ordered => Ok(()),
            ExecutionStrategy::Grouped => Err(QueryError::invariant(
                "grouped plans require execute_grouped(...)",
            )),
        }
    }

    // Validate that one execution strategy is admissible for the grouped
    // execution surface.
    fn ensure_grouped_execution_strategy(strategy: ExecutionStrategy) -> Result<(), QueryError> {
        match strategy {
            ExecutionStrategy::Grouped => Ok(()),
            ExecutionStrategy::PrimaryKey | ExecutionStrategy::Ordered => Err(
                QueryError::invariant("execute_grouped requires grouped logical plans"),
            ),
        }
    }

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: compile typed intent into one executable plan contract.
        let mode = query.mode();
        let plan = query.plan()?.into_executable();

        // Phase 2: delegate execution to the shared compiled-plan entry path.
        self.execute_query_dyn(mode, plan)
    }

    /// Execute one scalar query from one pre-built executable contract.
    ///
    /// This is the shared compiled-plan entry boundary used by the typed
    /// `execute_query(...)` surface and adjacent query execution facades.
    pub(in crate::db) fn execute_query_dyn<E>(
        &self,
        mode: QueryMode,
        plan: ExecutablePlan<E>,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let result = match mode {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::execute)
    }

    // Shared load-query terminal wrapper: build plan, run under metrics, map
    // execution errors into query-facing errors.
    pub(in crate::db) fn execute_load_query_with<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, ExecutablePlan<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::execute)
    }

    /// Build one trace payload for a query without executing it.
    ///
    /// This lightweight surface is intended for developer diagnostics:
    /// plan hash, access strategy summary, and planner/executor route shape.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let compiled = query.plan()?;
        let explain = compiled.explain();
        let plan_hash = compiled.plan_hash_hex();

        let executable = compiled.into_executable();
        let access_strategy = AccessStrategy::from_plan(executable.access()).debug_summary();
        let execution_strategy = match query.mode() {
            QueryMode::Load(_) => Some(trace_execution_strategy(
                executable
                    .execution_strategy()
                    .map_err(QueryError::execute)?,
            )),
            QueryMode::Delete(_) => None,
        };

        Ok(QueryTracePlan::new(
            plan_hash,
            access_strategy,
            execution_strategy,
            explain,
        ))
    }

    /// Execute one scalar paged load query and return optional continuation cursor plus trace.
    pub(crate) fn execute_load_query_paged_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate executable plan and reject grouped plans.
        let plan = query.plan()?.into_executable();
        Self::ensure_scalar_paged_execution_strategy(
            plan.execution_strategy().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external cursor token and validate it against plan surface.
        let cursor_bytes = decode_optional_cursor_bytes(cursor_token)?;
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(QueryError::from_executor_plan_error)?;

        // Phase 3: execute one traced page and encode outbound continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::execute)?;
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_scalar() else {
                    return Err(QueryError::scalar_paged_emitted_grouped_continuation());
                };

                token.encode().map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize continuation cursor: {err}"
                    ))
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate executable plan and require grouped shape.
        let plan = query.plan()?.into_executable();
        Self::ensure_grouped_execution_strategy(
            plan.execution_strategy().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor_bytes = decode_optional_cursor_bytes(cursor_token)?;
        let cursor = plan
            .prepare_grouped_cursor(cursor_bytes.as_deref())
            .map_err(QueryError::from_executor_plan_error)?;

        // Phase 3: execute grouped page and encode outbound grouped continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_grouped_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::execute)?;
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_grouped() else {
                    return Err(QueryError::grouped_paged_emitted_scalar_continuation());
                };

                token.encode().map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize grouped continuation cursor: {err}"
                    ))
                })
            })
            .transpose()?;

        Ok(PagedGroupedExecutionWithTrace::new(
            page.rows,
            next_cursor,
            trace,
        ))
    }
}

const fn trace_execution_strategy(strategy: ExecutionStrategy) -> TraceExecutionStrategy {
    match strategy {
        ExecutionStrategy::PrimaryKey => TraceExecutionStrategy::PrimaryKey,
        ExecutionStrategy::Ordered => TraceExecutionStrategy::Ordered,
        ExecutionStrategy::Grouped => TraceExecutionStrategy::Grouped,
    }
}
