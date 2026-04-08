//! Module: db::session::query
//! Responsibility: module-local ownership and contracts for db::session::query.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, EntityResponse, GroupedTextCursorPageWithTrace, PagedGroupedExecutionWithTrace,
        PagedLoadExecutionWithTrace, PersistedRow, Query, QueryError, QueryTracePlan,
        TraceExecutionStrategy,
        access::AccessStrategy,
        cursor::{CursorPlanError, GroupedContinuationToken},
        diagnostics::ExecutionTrace,
        executor::{
            ExecutablePlan, ExecutionStrategy, GroupedCursorPage, LoadExecutor, PageCursor,
        },
        query::builder::aggregate::AggregateExpr,
        query::explain::{
            ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor, ExplainPlan,
        },
        query::intent::{CompiledQuery, PlannedQuery},
        query::plan::QueryMode,
        session::{decode_optional_cursor_bytes, decode_optional_grouped_cursor},
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
};

impl<C: CanisterKind> DbSession<C> {
    // Compile one typed query using only the indexes currently visible for the
    // query's recovered store.
    pub(in crate::db) fn compile_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<CompiledQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.plan_with_visible_indexes(&visible_indexes)
    }

    // Build one logical planned-query shell using only the indexes currently
    // visible for the query's recovered store.
    pub(in crate::db) fn planned_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<PlannedQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.planned_with_visible_indexes(&visible_indexes)
    }

    // Project one logical explain payload using only planner-visible indexes.
    pub(in crate::db) fn explain_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainPlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_with_visible_indexes(&visible_indexes)
    }

    // Hash one typed query plan using only the indexes currently visible for
    // the query's recovered store.
    pub(in crate::db) fn query_plan_hash_hex_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.plan_hash_hex_with_visible_indexes(&visible_indexes)
    }

    // Explain one scalar load execution shape using only planner-visible
    // indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_execution_with_visible_indexes(&visible_indexes)
    }

    // Render one scalar load execution descriptor as deterministic text using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_text_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_execution_text_with_visible_indexes(&visible_indexes)
    }

    // Render one scalar load execution descriptor as canonical JSON using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_json_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_execution_json_with_visible_indexes(&visible_indexes)
    }

    // Render one scalar load execution descriptor plus route diagnostics using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_verbose_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_execution_verbose_with_visible_indexes(&visible_indexes)
    }

    // Explain one scalar aggregate terminal using only planner-visible indexes
    // from the recovered store state.
    pub(in crate::db) fn explain_query_aggregate_terminal_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
        aggregate: AggregateExpr,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_aggregate_terminal_with_visible_indexes(&visible_indexes, aggregate)
    }

    // Explain one `bytes_by(field)` terminal using only planner-visible
    // indexes from the recovered store state.
    pub(in crate::db) fn explain_query_bytes_by_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let visible_indexes = self.visible_indexes_for_store_model(E::Store::PATH, E::MODEL)?;

        query.explain_bytes_by_with_visible_indexes(&visible_indexes, target_field)
    }

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
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();

        // Phase 2: delegate execution to the shared compiled-plan entry path.
        self.execute_query_dyn(mode, plan)
    }

    /// Execute one typed delete query and return only the affected-row count.
    #[doc(hidden)]
    pub fn execute_delete_count<E>(&self, query: &Query<E>) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: fail closed if the caller routes a non-delete query here.
        if !query.mode().is_delete() {
            return Err(QueryError::unsupported_query(
                "delete count execution requires delete query mode",
            ));
        }

        // Phase 2: compile typed delete intent into one executable plan contract.
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();

        // Phase 3: execute the shared delete core while skipping response-row materialization.
        self.with_metrics(|| self.delete_executor::<E>().execute_count(plan))
            .map_err(QueryError::execute)
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
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();

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
        let compiled = self.compile_query_with_visible_indexes(query)?;
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
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();
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
        let (page, trace) = self.execute_grouped_page_with_trace(query, cursor_token)?;
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

    /// Execute one grouped query page and return grouped rows plus an already-encoded text cursor.
    #[doc(hidden)]
    pub fn execute_grouped_text_cursor<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<GroupedTextCursorPageWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (page, trace) = self.execute_grouped_page_with_trace(query, cursor_token)?;
        let next_cursor = page
            .next_cursor
            .map(Self::encode_grouped_page_cursor_hex)
            .transpose()?;

        Ok((page.rows, next_cursor, trace))
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Execute the canonical grouped query core and return the raw grouped page
    // plus optional execution trace before outward cursor formatting.
    fn execute_grouped_page_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate executable plan and require grouped shape.
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();
        Self::ensure_grouped_execution_strategy(
            plan.execution_strategy().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor = decode_optional_grouped_cursor(cursor_token)?;
        let cursor = plan
            .prepare_grouped_cursor_token(cursor)
            .map_err(QueryError::from_executor_plan_error)?;

        // Phase 3: execute one grouped page while preserving the structural
        // grouped cursor payload for whichever outward cursor format the caller needs.
        self.with_metrics(|| {
            self.load_executor::<E>()
                .execute_grouped_paged_with_cursor_traced(plan, cursor)
        })
        .map_err(QueryError::execute)
    }

    // Encode one grouped page cursor directly to lowercase hex without
    // round-tripping through a temporary raw cursor byte vector.
    fn encode_grouped_page_cursor_hex(page_cursor: PageCursor) -> Result<String, QueryError> {
        let token: &GroupedContinuationToken = page_cursor
            .as_grouped()
            .ok_or_else(QueryError::grouped_paged_emitted_scalar_continuation)?;

        token.encode_hex().map_err(|err| {
            QueryError::serialize_internal(format!(
                "failed to serialize grouped continuation cursor: {err}"
            ))
        })
    }
}

const fn trace_execution_strategy(strategy: ExecutionStrategy) -> TraceExecutionStrategy {
    match strategy {
        ExecutionStrategy::PrimaryKey => TraceExecutionStrategy::PrimaryKey,
        ExecutionStrategy::Ordered => TraceExecutionStrategy::Ordered,
        ExecutionStrategy::Grouped => TraceExecutionStrategy::Grouped,
    }
}
