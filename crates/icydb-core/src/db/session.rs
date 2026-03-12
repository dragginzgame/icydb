//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

#[cfg(test)]
use crate::db::{DataStore, IndexStore};
use crate::{
    db::{
        Db, EntityResponse, EntitySchemaDescription, FluentDeleteQuery, FluentLoadQuery,
        MissingRowPolicy, PagedGroupedExecutionWithTrace, PagedLoadExecutionWithTrace, PlanError,
        ProjectionResponse, Query, QueryError, QueryTracePlan, StorageReport, StoreRegistry,
        TraceExecutionStrategy, WriteBatchResponse,
        access::AccessStrategy,
        commit::EntityRuntimeHooks,
        cursor::decode_optional_cursor_token,
        executor::{
            DeleteExecutor, ExecutablePlan, ExecutionStrategy, ExecutorPlanError, LoadExecutor,
            SaveExecutor,
        },
        query::{
            builder::aggregate::{AggregateExpr, avg, count, count_by, max_by, min_by, sum},
            explain::ExplainAggregateTerminalPlan,
            intent::IntentError,
            plan::{FieldSlot, QueryMode},
        },
        schema::{describe_entity_model, show_indexes_for_model},
        sql::lowering::{
            SqlCommand, SqlGlobalAggregateCommand, SqlGlobalAggregateTerminal, SqlLoweringError,
            compile_sql_command, compile_sql_global_aggregate_command,
        },
        sql::parser::SqlExplainMode,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    metrics::sink::{MetricsSink, with_metrics_sink},
    traits::{CanisterKind, EntityKind, EntityValue},
    value::Value,
};
use std::thread::LocalKey;

// Map executor-owned plan-surface failures into query-owned plan errors.
fn map_executor_plan_error(err: ExecutorPlanError) -> QueryError {
    match err {
        ExecutorPlanError::Cursor(err) => QueryError::from(PlanError::from(*err)),
    }
}

// Decode one optional external cursor token and map decode failures into the
// query-plan cursor error boundary.
fn decode_optional_cursor_bytes(cursor_token: Option<&str>) -> Result<Option<Vec<u8>>, QueryError> {
    decode_optional_cursor_token(cursor_token).map_err(|err| QueryError::from(PlanError::from(err)))
}

// Map SQL frontend parse/lowering failures into query-facing execution errors.
fn map_sql_lowering_error(err: SqlLoweringError) -> QueryError {
    match err {
        SqlLoweringError::Query(err) => err,
        SqlLoweringError::Parse(crate::db::sql::parser::SqlParseError::UnsupportedFeature {
            feature,
        }) => QueryError::execute(InternalError::query_unsupported_sql_feature(feature)),
        other => QueryError::execute(InternalError::classified(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            format!("SQL query is not executable in this release: {other}"),
        )),
    }
}

// Resolve one aggregate target field through planner slot contracts before
// aggregate terminal execution.
fn resolve_sql_aggregate_target_slot<E: EntityKind>(field: &str) -> Result<FieldSlot, QueryError> {
    FieldSlot::resolve(E::MODEL, field).ok_or_else(|| {
        QueryError::execute(crate::db::error::executor_unsupported(format!(
            "unknown aggregate target field: {field}",
        )))
    })
}

// Convert one lowered global SQL aggregate terminal into aggregate expression
// contracts used by aggregate explain execution descriptors.
fn sql_global_aggregate_terminal_to_expr<E: EntityKind>(
    terminal: &SqlGlobalAggregateTerminal,
) -> Result<AggregateExpr, QueryError> {
    match terminal {
        SqlGlobalAggregateTerminal::CountRows => Ok(count()),
        SqlGlobalAggregateTerminal::CountField(field) => {
            let _ = resolve_sql_aggregate_target_slot::<E>(field)?;

            Ok(count_by(field.as_str()))
        }
        SqlGlobalAggregateTerminal::SumField(field) => {
            let _ = resolve_sql_aggregate_target_slot::<E>(field)?;

            Ok(sum(field.as_str()))
        }
        SqlGlobalAggregateTerminal::AvgField(field) => {
            let _ = resolve_sql_aggregate_target_slot::<E>(field)?;

            Ok(avg(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MinField(field) => {
            let _ = resolve_sql_aggregate_target_slot::<E>(field)?;

            Ok(min_by(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MaxField(field) => {
            let _ = resolve_sql_aggregate_target_slot::<E>(field)?;

            Ok(max_by(field.as_str()))
        }
    }
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
        E: EntityKind<Canister = C> + EntityValue,
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
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, std::convert::identity)
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

    /// Build one typed query intent from one reduced SQL statement.
    ///
    /// This parser/lowering entrypoint is intentionally constrained to the
    /// executable subset wired in the current release.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let command = compile_sql_command::<E>(sql, MissingRowPolicy::Ignore)
            .map_err(map_sql_lowering_error)?;

        match command {
            SqlCommand::Query(query) => Ok(query),
            SqlCommand::Explain { .. } | SqlCommand::ExplainGlobalAggregate { .. } => {
                Err(QueryError::execute(InternalError::classified(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Query,
                    "query_from_sql does not accept EXPLAIN statements; use explain_sql(...)",
                )))
            }
        }
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement for entity `E`.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        if query.has_grouping() {
            return Err(QueryError::Intent(
                IntentError::GroupedRequiresExecuteGrouped,
            ));
        }

        self.execute_query(&query)
    }

    /// Execute one reduced SQL `SELECT` statement and return projection-shaped rows.
    ///
    /// This surface keeps `execute_sql(...)` backwards-compatible for callers
    /// that currently consume full entity rows.
    pub fn execute_sql_projection<E>(&self, sql: &str) -> Result<ProjectionResponse<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        if query.has_grouping() {
            return Err(QueryError::Intent(
                IntentError::GroupedRequiresExecuteGrouped,
            ));
        }

        match query.mode() {
            QueryMode::Load(_) => {
                self.execute_load_query_with(&query, |load, plan| load.execute_projection(plan))
            }
            QueryMode::Delete(_) => Err(QueryError::execute(InternalError::classified(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "execute_sql_projection only supports SELECT statements",
            ))),
        }
    }

    /// Execute one reduced SQL global aggregate `SELECT` statement.
    ///
    /// This entrypoint is intentionally constrained to one aggregate terminal
    /// shape per statement and preserves existing terminal semantics.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<Value, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let command = compile_sql_global_aggregate_command::<E>(sql, MissingRowPolicy::Ignore)
            .map_err(map_sql_lowering_error)?;

        match command.terminal() {
            SqlGlobalAggregateTerminal::CountRows => self
                .execute_load_query_with(command.query(), |load, plan| load.aggregate_count(plan))
                .map(|count| Value::Uint(u64::from(count))),
            SqlGlobalAggregateTerminal::CountField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                self.execute_load_query_with(command.query(), |load, plan| {
                    load.values_by_slot(plan, target_slot)
                })
                .map(|values| {
                    let count = values
                        .into_iter()
                        .filter(|value| !matches!(value, Value::Null))
                        .count();
                    Value::Uint(u64::try_from(count).unwrap_or(u64::MAX))
                })
            }
            SqlGlobalAggregateTerminal::SumField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                self.execute_load_query_with(command.query(), |load, plan| {
                    load.aggregate_sum_by_slot(plan, target_slot)
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::AvgField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                self.execute_load_query_with(command.query(), |load, plan| {
                    load.aggregate_avg_by_slot(plan, target_slot)
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::MinField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                let min_id = self.execute_load_query_with(command.query(), |load, plan| {
                    load.aggregate_min_by_slot(plan, target_slot)
                })?;

                match min_id {
                    Some(id) => self
                        .load::<E>()
                        .by_id(id)
                        .first_value_by(field)
                        .map(|value| value.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            SqlGlobalAggregateTerminal::MaxField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                let max_id = self.execute_load_query_with(command.query(), |load, plan| {
                    load.aggregate_max_by_slot(plan, target_slot)
                })?;

                match max_id {
                    Some(id) => self
                        .load::<E>()
                        .by_id(id)
                        .first_value_by(field)
                        .map(|value| value.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
        }
    }

    /// Execute one reduced SQL grouped `SELECT` statement and return grouped rows.
    pub fn execute_sql_grouped<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        if !query.has_grouping() {
            return Err(QueryError::execute(InternalError::classified(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "execute_sql_grouped requires grouped SQL query intent",
            )));
        }

        self.execute_grouped(&query, cursor_token)
    }

    /// Explain one reduced SQL statement for entity `E`.
    ///
    /// Supported modes:
    /// - `EXPLAIN ...` -> logical plan text
    /// - `EXPLAIN EXECUTION ...` -> execution descriptor text
    /// - `EXPLAIN JSON ...` -> logical plan canonical JSON
    pub fn explain_sql<E>(&self, sql: &str) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let command = compile_sql_command::<E>(sql, MissingRowPolicy::Ignore)
            .map_err(map_sql_lowering_error)?;

        match command {
            SqlCommand::Query(_) => Err(QueryError::execute(InternalError::classified(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "explain_sql requires an EXPLAIN statement",
            ))),
            SqlCommand::Explain { mode, query } => match mode {
                SqlExplainMode::Plan => Ok(query.explain()?.render_text_canonical()),
                SqlExplainMode::Execution => query.explain_execution_text(),
                SqlExplainMode::Json => Ok(query.explain()?.render_json_canonical()),
            },
            SqlCommand::ExplainGlobalAggregate { mode, command } => {
                Self::explain_sql_global_aggregate::<E>(mode, command)
            }
        }
    }

    // Render one EXPLAIN payload for constrained global aggregate SQL command.
    fn explain_sql_global_aggregate<E>(
        mode: SqlExplainMode,
        command: SqlGlobalAggregateCommand<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        match mode {
            SqlExplainMode::Plan => {
                // Keep explain validation parity with execution by requiring the
                // target field to resolve before returning explain output.
                let _ = sql_global_aggregate_terminal_to_expr::<E>(command.terminal())?;

                Ok(command.query().explain()?.render_text_canonical())
            }
            SqlExplainMode::Execution => {
                let aggregate = sql_global_aggregate_terminal_to_expr::<E>(command.terminal())?;
                let plan = Self::explain_load_query_terminal_with(command.query(), aggregate)?;

                Ok(plan.execution_node_descriptor().render_text_tree())
            }
            SqlExplainMode::Json => {
                // Keep explain validation parity with execution by requiring the
                // target field to resolve before returning explain output.
                let _ = sql_global_aggregate_terminal_to_expr::<E>(command.terminal())?;

                Ok(command.query().explain()?.render_json_canonical())
            }
        }
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
        show_indexes_for_model(E::MODEL)
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
        describe_entity_model(E::MODEL)
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, InternalError> {
        self.db.storage_report(name_to_path)
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
        E: EntityKind<Canister = C> + EntityValue,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(in crate::db) const fn save_executor<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    // ---------------------------------------------------------------------
    // Query diagnostics / execution (internal routing)
    // ---------------------------------------------------------------------

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();

        let result = match query.mode() {
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
        E: EntityKind<Canister = C> + EntityValue,
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

    /// Build one aggregate-terminal explain payload without executing the query.
    pub(crate) fn explain_load_query_terminal_with<E>(
        query: &Query<E>,
        aggregate: AggregateExpr,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        // Phase 1: build one compiled query once and project logical explain output.
        let compiled = query.plan()?;
        let query_explain = compiled.explain();
        let terminal = aggregate.kind();

        // Phase 2: derive the executor route label for this aggregate terminal.
        let executable = compiled.into_executable();
        let execution = executable.explain_aggregate_terminal_execution_descriptor(aggregate);

        Ok(ExplainAggregateTerminalPlan::new(
            query_explain,
            terminal,
            execution,
        ))
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
        match plan.execution_strategy().map_err(QueryError::execute)? {
            ExecutionStrategy::PrimaryKey => {
                return Err(QueryError::execute(
                    crate::db::error::query_executor_invariant(
                        "cursor pagination requires explicit or grouped ordering",
                    ),
                ));
            }
            ExecutionStrategy::Ordered => {}
            ExecutionStrategy::Grouped => {
                return Err(QueryError::execute(
                    crate::db::error::query_executor_invariant(
                        "grouped plans require execute_grouped(...)",
                    ),
                ));
            }
        }

        // Phase 2: decode external cursor token and validate it against plan surface.
        let cursor_bytes = decode_optional_cursor_bytes(cursor_token)?;
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

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
                    return Err(QueryError::execute(
                        crate::db::error::query_executor_invariant(
                            "scalar load pagination emitted grouped continuation token",
                        ),
                    ));
                };

                token.encode().map_err(|err| {
                    QueryError::execute(InternalError::serialize_internal(format!(
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
        if !matches!(
            plan.execution_strategy().map_err(QueryError::execute)?,
            ExecutionStrategy::Grouped
        ) {
            return Err(QueryError::execute(
                crate::db::error::query_executor_invariant(
                    "execute_grouped requires grouped logical plans",
                ),
            ));
        }

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor_bytes = decode_optional_cursor_bytes(cursor_token)?;
        let cursor = plan
            .prepare_grouped_cursor(cursor_bytes.as_deref())
            .map_err(map_executor_plan_error)?;

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
                    return Err(QueryError::execute(
                        crate::db::error::query_executor_invariant(
                            "grouped pagination emitted scalar continuation token",
                        ),
                    ));
                };

                token.encode().map_err(|err| {
                    QueryError::execute(InternalError::serialize_internal(format!(
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
    pub fn insert<E>(&self, entity: E) -> Result<E, InternalError>
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
    pub fn replace<E>(&self, entity: E) -> Result<E, InternalError>
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
    pub fn update<E>(&self, entity: E) -> Result<E, InternalError>
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

const fn trace_execution_strategy(strategy: ExecutionStrategy) -> TraceExecutionStrategy {
    match strategy {
        ExecutionStrategy::PrimaryKey => TraceExecutionStrategy::PrimaryKey,
        ExecutionStrategy::Ordered => TraceExecutionStrategy::Ordered,
        ExecutionStrategy::Grouped => TraceExecutionStrategy::Grouped,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            Db,
            commit::{ensure_recovered, init_commit_store_for_tests},
            cursor::CursorPlanError,
            data::DataStore,
            index::IndexStore,
            query::plan::expr::{Expr, ProjectionField},
            registry::StoreRegistry,
        },
        error::{ErrorClass, ErrorDetail, ErrorOrigin, QueryErrorDetail},
        model::field::FieldKind,
        testing::test_memory,
        traits::Path,
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};
    use std::cell::RefCell;

    crate::test_canister! {
        ident = SessionSqlCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = SessionSqlStore,
        canister = SessionSqlCanister,
    }

    thread_local! {
        static SESSION_SQL_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init(test_memory(160)));
        static SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(test_memory(161)));
        static SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
            let mut reg = StoreRegistry::new();
            reg.register_store(
                SessionSqlStore::PATH,
                &SESSION_SQL_DATA_STORE,
                &SESSION_SQL_INDEX_STORE,
            )
            .expect("SQL session test store registration should succeed");
            reg
        };
    }

    static SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new(&SESSION_SQL_STORE_REGISTRY);

    ///
    /// SessionSqlEntity
    ///
    /// Test entity used to lock end-to-end reduced SQL session behavior.
    ///

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct SessionSqlEntity {
        id: Ulid,
        name: String,
        age: u64,
    }

    crate::test_entity_schema! {
        ident = SessionSqlEntity,
        id = Ulid,
        id_field = id,
        entity_name = "SessionSqlEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text),
            ("age", FieldKind::Uint),
        ],
        indexes = [],
        store = SessionSqlStore,
        canister = SessionSqlCanister,
    }

    // Reset all session SQL fixture state between tests to preserve deterministic assertions.
    fn reset_session_sql_store() {
        init_commit_store_for_tests().expect("commit store init should succeed");
        ensure_recovered(&SESSION_SQL_DB).expect("write-side recovery should succeed");
        SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
        SESSION_SQL_INDEX_STORE.with(|store| store.borrow_mut().clear());
    }

    fn sql_session() -> DbSession<SessionSqlCanister> {
        DbSession::new(SESSION_SQL_DB)
    }

    // Assert query-surface cursor errors remain wrapped under QueryError::Plan(PlanError::Cursor).
    fn assert_query_error_is_cursor_plan(
        err: QueryError,
        predicate: impl FnOnce(&CursorPlanError) -> bool,
    ) {
        assert!(matches!(
            err,
            QueryError::Plan(plan_err)
                if matches!(
                    plan_err.as_ref(),
                    PlanError::Cursor(inner) if predicate(inner.as_ref())
                )
        ));
    }

    // Assert both session conversion paths preserve the same cursor-plan variant payload.
    fn assert_cursor_mapping_parity(
        build: impl Fn() -> CursorPlanError,
        predicate: impl Fn(&CursorPlanError) -> bool + Copy,
    ) {
        let mapped_via_executor = map_executor_plan_error(ExecutorPlanError::from(build()));
        assert_query_error_is_cursor_plan(mapped_via_executor, predicate);

        let mapped_via_plan = QueryError::from(PlanError::from(build()));
        assert_query_error_is_cursor_plan(mapped_via_plan, predicate);
    }

    // Assert SQL parser unsupported-feature labels remain preserved through
    // query-facing execution error detail payloads.
    fn assert_sql_unsupported_feature_detail(err: QueryError, expected_feature: &'static str) {
        let QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
            internal,
        )) = err
        else {
            panic!("expected query execution unsupported error variant");
        };

        assert_eq!(internal.class(), ErrorClass::Unsupported);
        assert_eq!(internal.origin(), ErrorOrigin::Query);
        assert!(
            matches!(
                internal.detail(),
                Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                    if *feature == expected_feature
            ),
            "unsupported SQL feature detail label should be preserved",
        );
    }

    fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 3] {
        [
            (
                "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
                "JOIN",
            ),
            (
                "SELECT \"name\" FROM SessionSqlEntity",
                "quoted identifiers",
            ),
            ("SELECT * FROM SessionSqlEntity alias", "table aliases"),
        ]
    }

    #[test]
    fn session_cursor_error_mapping_parity_boundary_arity() {
        assert_cursor_mapping_parity(
            || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
            |inner| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                        expected: 2,
                        found: 1
                    }
                )
            },
        );
    }

    #[test]
    fn session_cursor_error_mapping_parity_window_mismatch() {
        assert_cursor_mapping_parity(
            || CursorPlanError::continuation_cursor_window_mismatch(8, 3),
            |inner| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorWindowMismatch {
                        expected_offset: 8,
                        actual_offset: 3
                    }
                )
            },
        );
    }

    #[test]
    fn session_cursor_error_mapping_parity_decode_reason() {
        assert_cursor_mapping_parity(
            || {
                CursorPlanError::invalid_continuation_cursor(
                    crate::db::codec::cursor::CursorDecodeError::OddLength,
                )
            },
            |inner| {
                matches!(
                    inner,
                    CursorPlanError::InvalidContinuationCursor {
                        reason: crate::db::codec::cursor::CursorDecodeError::OddLength
                    }
                )
            },
        );
    }

    #[test]
    fn session_cursor_error_mapping_parity_primary_key_type_mismatch() {
        assert_cursor_mapping_parity(
            || {
                CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                    "id",
                    "ulid",
                    Some(crate::value::Value::Text("not-a-ulid".to_string())),
                )
            },
            |inner| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                        field,
                        expected,
                        value: Some(crate::value::Value::Text(value))
                    } if field == "id" && expected == "ulid" && value == "not-a-ulid"
                )
            },
        );
    }

    #[test]
    fn session_cursor_error_mapping_parity_matrix_preserves_cursor_variants() {
        // Keep one matrix-level canary test name so cross-module audit references remain stable.
        assert_cursor_mapping_parity(
            || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
            |inner| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                        expected: 2,
                        found: 1
                    }
                )
            },
        );
    }

    #[test]
    fn execute_sql_select_star_honors_order_limit_offset() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "older".to_string(),
                age: 37,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "younger".to_string(),
                age: 19,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql::<SessionSqlEntity>(
                "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 1",
            )
            .expect("SELECT * should execute");

        assert_eq!(response.count(), 1, "window should return one row");
        let row = response
            .iter()
            .next()
            .expect("windowed result should include one row");
        assert_eq!(
            row.entity_ref().name,
            "older",
            "ordered window should return the second age-ordered row",
        );
    }

    #[test]
    fn execute_sql_delete_honors_predicate_order_and_limit() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "first-minor".to_string(),
                age: 16,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "second-minor".to_string(),
                age: 17,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "adult".to_string(),
                age: 42,
            })
            .expect("seed insert should succeed");

        let deleted = session
            .execute_sql::<SessionSqlEntity>(
                "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
            )
            .expect("DELETE should execute");

        assert_eq!(deleted.count(), 1, "delete limit should remove one row");
        assert_eq!(
            deleted
                .iter()
                .next()
                .expect("deleted row should exist")
                .entity_ref()
                .age,
            16,
            "ordered delete should remove the youngest matching row first",
        );

        let remaining = session
            .load::<SessionSqlEntity>()
            .order_by("age")
            .execute()
            .expect("post-delete load should succeed");
        let remaining_ages = remaining
            .iter()
            .map(|row| row.entity_ref().age)
            .collect::<Vec<_>>();

        assert_eq!(
            remaining_ages,
            vec![17, 42],
            "delete window semantics should preserve non-deleted rows",
        );
    }

    #[test]
    fn query_from_sql_rejects_explain_statements() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .query_from_sql::<SessionSqlEntity>("EXPLAIN SELECT * FROM SessionSqlEntity")
            .expect_err("query_from_sql must reject EXPLAIN statements");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "query_from_sql EXPLAIN rejection must map to unsupported execution class",
        );
    }

    #[test]
    fn query_from_sql_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let err = session
                .query_from_sql::<SessionSqlEntity>(sql)
                .expect_err("unsupported SQL feature should fail through query_from_sql");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn execute_sql_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let err = session
                .execute_sql::<SessionSqlEntity>(sql)
                .expect_err("unsupported SQL feature should fail through execute_sql");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn execute_sql_projection_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let err = session
                .execute_sql_projection::<SessionSqlEntity>(sql)
                .expect_err("unsupported SQL feature should fail through execute_sql_projection");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn execute_sql_grouped_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let err = session
                .execute_sql_grouped::<SessionSqlEntity>(sql, None)
                .expect_err("unsupported SQL feature should fail through execute_sql_grouped");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn execute_sql_aggregate_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let err = session
                .execute_sql_aggregate::<SessionSqlEntity>(sql)
                .expect_err("unsupported SQL feature should fail through execute_sql_aggregate");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn explain_sql_preserves_parser_unsupported_feature_detail_labels() {
        reset_session_sql_store();
        let session = sql_session();

        for (sql, feature) in unsupported_sql_feature_cases() {
            let explain_sql = format!("EXPLAIN {sql}");
            let err = session
                .explain_sql::<SessionSqlEntity>(explain_sql.as_str())
                .expect_err("unsupported SQL feature should fail through explain_sql");
            assert_sql_unsupported_feature_detail(err, feature);
        }
    }

    #[test]
    fn query_from_sql_select_field_projection_lowers_to_scalar_field_selection() {
        reset_session_sql_store();
        let session = sql_session();

        let query = session
            .query_from_sql::<SessionSqlEntity>("SELECT name, age FROM SessionSqlEntity")
            .expect("field-list SQL query should lower");
        let projection = query
            .plan()
            .expect("field-list SQL plan should build")
            .projection_spec();
        let field_names = projection
            .fields()
            .map(|field| match field {
                ProjectionField::Scalar {
                    expr: Expr::Field(field),
                    alias: None,
                } => field.as_str().to_string(),
                other @ ProjectionField::Scalar { .. } => {
                    panic!("field-list SQL projection should lower to plain field exprs: {other:?}")
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
    }

    #[test]
    fn query_from_sql_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
        reset_session_sql_store();
        let session = sql_session();

        let query = session
            .query_from_sql::<SessionSqlEntity>(
                "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
            )
            .expect("grouped aggregate projection SQL query should lower");
        assert!(
            query.has_grouping(),
            "grouped aggregate SQL projection lowering should produce grouped query intent",
        );
    }

    #[test]
    fn execute_sql_select_field_projection_currently_returns_entity_shaped_rows() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "projected-row".to_string(),
                age: 29,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql::<SessionSqlEntity>(
                "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
            )
            .expect("field-list SQL projection should execute");
        let row = response
            .iter()
            .next()
            .expect("field-list SQL projection response should contain one row");

        assert_eq!(
            row.entity_ref().name,
            "projected-row",
            "field-list SQL projection should still return entity rows in this baseline",
        );
        assert_eq!(
            row.entity_ref().age,
            29,
            "field-list SQL projection should preserve full entity payload until projection response shaping is introduced",
        );
    }

    #[test]
    fn execute_sql_projection_select_field_list_returns_projection_shaped_rows() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "projection-surface".to_string(),
                age: 33,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql_projection::<SessionSqlEntity>(
                "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
            )
            .expect("projection SQL execution should succeed");
        let row = response
            .iter()
            .next()
            .expect("projection SQL response should contain one row");

        assert_eq!(response.count(), 1);
        assert_eq!(
            row.values(),
            [Value::Text("projection-surface".to_string())],
            "projection SQL response should carry only projected field values in declaration order",
        );
    }

    #[test]
    fn execute_sql_projection_select_star_returns_all_fields_in_model_order() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "projection-star".to_string(),
                age: 41,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql_projection::<SessionSqlEntity>(
                "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
            )
            .expect("projection SQL star execution should succeed");
        let row = response
            .iter()
            .next()
            .expect("projection SQL star response should contain one row");

        assert_eq!(response.count(), 1);
        assert_eq!(
            row.values().len(),
            3,
            "SELECT * projection response should include all model fields",
        );
        assert_eq!(row.values()[0], Value::Ulid(row.id().key()));
        assert_eq!(row.values()[1], Value::Text("projection-star".to_string()));
        assert_eq!(row.values()[2], Value::Uint(41));
    }

    #[test]
    fn execute_sql_select_schema_qualified_entity_executes() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "schema-qualified".to_string(),
                age: 41,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql::<SessionSqlEntity>(
                "SELECT * FROM public.SessionSqlEntity ORDER BY age ASC LIMIT 1",
            )
            .expect("schema-qualified entity SQL should execute");

        assert_eq!(response.len(), 1);
    }

    #[test]
    fn execute_sql_projection_select_table_qualified_fields_executes() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-projection".to_string(),
                age: 42,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql_projection::<SessionSqlEntity>(
                "SELECT SessionSqlEntity.name \
                 FROM SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 40 \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            )
            .expect("table-qualified projection SQL should execute");
        let row = response
            .iter()
            .next()
            .expect("table-qualified projection SQL response should contain one row");

        assert_eq!(response.count(), 1);
        assert_eq!(
            row.values(),
            [Value::Text("qualified-projection".to_string())]
        );
    }

    #[test]
    fn execute_sql_projection_rejects_delete_statements() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql_projection::<SessionSqlEntity>(
                "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            )
            .expect_err("projection SQL execution should reject delete statements");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "projection SQL delete usage should fail as unsupported",
        );
    }

    #[test]
    fn execute_sql_select_field_projection_unknown_field_fails_with_plan_error() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT missing_field FROM SessionSqlEntity")
            .expect_err("unknown projected fields should fail planner validation");

        assert!(
            matches!(err, QueryError::Plan(_)),
            "unknown projected fields should surface planner-domain query errors: {err:?}",
        );
    }

    #[test]
    fn execute_sql_rejects_aggregate_projection_in_current_slice() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
            .expect_err("global aggregate SQL projection should remain lowering-gated");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "global aggregate SQL projection should fail at reduced lowering boundary",
        );
    }

    #[test]
    fn execute_sql_rejects_table_alias_forms_in_reduced_parser() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity alias")
            .expect_err("table aliases should be rejected by reduced SQL parser");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "table alias usage should fail closed through unsupported SQL boundary",
        );
    }

    #[test]
    fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT \"name\" FROM SessionSqlEntity")
            .expect_err("quoted identifiers should be rejected by reduced SQL parser");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "quoted identifiers should fail closed through unsupported SQL boundary",
        );
    }

    #[test]
    fn execute_sql_select_distinct_star_executes() {
        reset_session_sql_store();
        let session = sql_session();

        let id_a = Ulid::generate();
        let id_b = Ulid::generate();
        session
            .insert(SessionSqlEntity {
                id: id_a,
                name: "distinct-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: id_b,
                name: "distinct-b".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql::<SessionSqlEntity>(
                "SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            )
            .expect("SELECT DISTINCT * should execute");
        assert_eq!(response.len(), 2);
    }

    #[test]
    fn execute_sql_projection_select_distinct_with_pk_field_list_executes() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "distinct-pk-a".to_string(),
                age: 25,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "distinct-pk-b".to_string(),
                age: 25,
            })
            .expect("seed insert should succeed");

        let response = session
            .execute_sql_projection::<SessionSqlEntity>(
                "SELECT DISTINCT id, age FROM SessionSqlEntity ORDER BY id ASC",
            )
            .expect("SELECT DISTINCT field-list with PK should execute");
        assert_eq!(response.len(), 2);
        assert_eq!(response[0].values().len(), 2);
    }

    #[test]
    fn execute_sql_rejects_distinct_without_pk_projection_in_current_slice() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT DISTINCT age FROM SessionSqlEntity")
            .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "distinct SQL gating should map to unsupported execution error boundary",
        );
    }

    #[test]
    fn execute_sql_aggregate_count_star_and_count_field_return_uint() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "aggregate-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "aggregate-b".to_string(),
                age: 32,
            })
            .expect("seed insert should succeed");

        let count_rows = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
            .expect("COUNT(*) SQL aggregate should execute");
        let count_field = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT COUNT(age) FROM SessionSqlEntity")
            .expect("COUNT(field) SQL aggregate should execute");
        assert_eq!(count_rows, Value::Uint(2));
        assert_eq!(count_field, Value::Uint(2));
    }

    #[test]
    fn execute_sql_aggregate_sum_with_table_qualified_field_executes() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-aggregate-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-aggregate-b".to_string(),
                age: 32,
            })
            .expect("seed insert should succeed");

        let sum = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT SUM(SessionSqlEntity.age) FROM SessionSqlEntity",
            )
            .expect("table-qualified aggregate SQL should execute");

        assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(52u64)));
    }

    #[test]
    fn execute_sql_aggregate_rejects_distinct_aggregate_qualifier() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT COUNT(DISTINCT age) FROM SessionSqlEntity",
            )
            .expect_err("aggregate DISTINCT qualifier should remain unsupported");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "aggregate DISTINCT qualifier should fail closed through unsupported SQL boundary",
        );
    }

    #[test]
    fn execute_sql_aggregate_sum_avg_min_max_return_expected_values() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "sumavg-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "sumavg-b".to_string(),
                age: 32,
            })
            .expect("seed insert should succeed");

        let sum = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT SUM(age) FROM SessionSqlEntity")
            .expect("SUM(field) SQL aggregate should execute");
        let avg = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT AVG(age) FROM SessionSqlEntity")
            .expect("AVG(field) SQL aggregate should execute");
        let min = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT MIN(age) FROM SessionSqlEntity")
            .expect("MIN(field) SQL aggregate should execute");
        let max = session
            .execute_sql_aggregate::<SessionSqlEntity>("SELECT MAX(age) FROM SessionSqlEntity")
            .expect("MAX(field) SQL aggregate should execute");
        let empty_sum = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT SUM(age) FROM SessionSqlEntity WHERE age < 0",
            )
            .expect("SUM(field) SQL aggregate empty-window execution should succeed");
        let empty_min = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT MIN(age) FROM SessionSqlEntity WHERE age < 0",
            )
            .expect("MIN(field) SQL aggregate empty-window execution should succeed");
        let empty_max = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT MAX(age) FROM SessionSqlEntity WHERE age < 0",
            )
            .expect("MAX(field) SQL aggregate empty-window execution should succeed");

        assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(52u64)));
        assert_eq!(avg, Value::Decimal(crate::types::Decimal::from(26u64)));
        assert_eq!(min, Value::Uint(20));
        assert_eq!(max, Value::Uint(32));
        assert_eq!(empty_sum, Value::Null);
        assert_eq!(empty_min, Value::Null);
        assert_eq!(empty_max, Value::Null);
    }

    #[test]
    fn execute_sql_aggregate_honors_order_limit_offset_window() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "window-a".to_string(),
                age: 10,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "window-b".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "window-c".to_string(),
                age: 30,
            })
            .expect("seed insert should succeed");

        let count = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
            )
            .expect("COUNT(*) SQL aggregate window execution should succeed");
        let sum = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 1 OFFSET 1",
            )
            .expect("SUM(field) SQL aggregate window execution should succeed");
        let avg = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2 OFFSET 1",
            )
            .expect("AVG(field) SQL aggregate window execution should succeed");

        assert_eq!(count, Value::Uint(2));
        assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(20u64)));
        assert_eq!(avg, Value::Decimal(crate::types::Decimal::from(25u64)));
    }

    #[test]
    fn execute_sql_aggregate_rejects_unsupported_aggregate_shapes() {
        reset_session_sql_store();
        let session = sql_session();

        for sql in [
            "SELECT age FROM SessionSqlEntity",
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        ] {
            let err = session
                .execute_sql_aggregate::<SessionSqlEntity>(sql)
                .expect_err("unsupported SQL aggregate shape should fail closed");
            assert!(
                matches!(
                    err,
                    QueryError::Execute(
                        crate::db::query::intent::QueryExecutionError::Unsupported(_)
                    )
                ),
                "unsupported SQL aggregate shape should map to unsupported execution error boundary: {sql}",
            );
        }
    }

    #[test]
    fn execute_sql_aggregate_rejects_unknown_target_field() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql_aggregate::<SessionSqlEntity>(
                "SELECT SUM(missing_field) FROM SessionSqlEntity",
            )
            .expect_err("unknown aggregate target field should fail");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "unknown aggregate target field should map to unsupported execution error boundary",
        );
    }

    #[test]
    fn execute_sql_projection_rejects_grouped_aggregate_sql() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql_projection::<SessionSqlEntity>(
                "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
            )
            .expect_err("projection SQL API should reject grouped aggregate SQL intent");

        assert!(
            matches!(
                err,
                QueryError::Intent(
                    crate::db::query::intent::IntentError::GroupedRequiresExecuteGrouped
                )
            ),
            "projection SQL API must reject grouped aggregate SQL with grouped-intent routing error",
        );
    }

    #[test]
    fn execute_sql_grouped_select_count_returns_grouped_aggregate_row() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "aggregate-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "aggregate-b".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "aggregate-c".to_string(),
                age: 32,
            })
            .expect("seed insert should succeed");

        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(
                "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
                None,
            )
            .expect("grouped SQL aggregate execution should succeed");

        assert!(
            execution.continuation_cursor().is_none(),
            "single-page grouped aggregate execution should not emit continuation cursor",
        );
        assert_eq!(execution.rows().len(), 2);
        assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
        assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
        assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
        assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
    }

    #[test]
    fn execute_sql_grouped_select_count_with_qualified_identifiers_executes() {
        reset_session_sql_store();
        let session = sql_session();

        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-group-a".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-group-b".to_string(),
                age: 20,
            })
            .expect("seed insert should succeed");
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: "qualified-group-c".to_string(),
                age: 32,
            })
            .expect("seed insert should succeed");

        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(
                "SELECT SessionSqlEntity.age, COUNT(*) \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 20 \
                 GROUP BY SessionSqlEntity.age \
                 ORDER BY SessionSqlEntity.age ASC LIMIT 10",
                None,
            )
            .expect("qualified grouped SQL aggregate execution should succeed");

        assert!(execution.continuation_cursor().is_none());
        assert_eq!(execution.rows().len(), 2);
        assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
        assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
        assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
        assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
    }

    #[test]
    fn execute_sql_grouped_rejects_scalar_sql_intent() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql_grouped::<SessionSqlEntity>("SELECT name FROM SessionSqlEntity", None)
            .expect_err("grouped SQL API should reject non-grouped SQL queries");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "grouped SQL API should fail closed for non-grouped SQL shapes",
        );
    }

    #[test]
    fn execute_sql_rejects_grouped_sql_intent_without_grouped_api() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>(
                "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
            )
            .expect_err("scalar SQL API should reject grouped SQL intent");

        assert!(
            matches!(
                err,
                QueryError::Intent(
                    crate::db::query::intent::IntentError::GroupedRequiresExecuteGrouped
                )
            ),
            "scalar SQL API must preserve grouped explicit-entrypoint contract",
        );
    }

    #[test]
    fn execute_sql_rejects_unsupported_group_by_projection_shape() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity GROUP BY age")
            .expect_err("group-by projection mismatch should fail closed");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "unsupported grouped SQL projection shapes should fail at reduced lowering boundary",
        );
    }

    #[test]
    fn explain_sql_execution_returns_descriptor_text() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            )
            .expect("EXPLAIN EXECUTION should succeed");

        assert!(
            explain.contains("node_id=0"),
            "execution explain output should include the root descriptor node id",
        );
        assert!(
            explain.contains("layer="),
            "execution explain output should include execution layer annotations",
        );
    }

    #[test]
    fn explain_sql_plan_returns_logical_plan_text() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            )
            .expect("EXPLAIN should succeed");

        assert!(
            explain.contains("mode=Load"),
            "logical explain text should include query mode projection",
        );
        assert!(
            explain.contains("access="),
            "logical explain text should include projected access shape",
        );
    }

    #[test]
    fn explain_sql_plan_grouped_qualified_identifiers_match_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT SessionSqlEntity.age, COUNT(*) \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21 \
                 GROUP BY SessionSqlEntity.age \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            )
            .expect("qualified grouped EXPLAIN plan SQL should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT age, COUNT(*) \
                 FROM SessionSqlEntity \
                 WHERE age >= 21 \
                 GROUP BY age \
                 ORDER BY age DESC LIMIT 2 OFFSET 1",
            )
            .expect("unqualified grouped EXPLAIN plan SQL should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified grouped identifiers should normalize to the same logical EXPLAIN plan output",
        );
    }

    #[test]
    fn explain_sql_execution_grouped_qualified_identifiers_match_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT SessionSqlEntity.age, COUNT(*) \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21 \
                 GROUP BY SessionSqlEntity.age \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            )
            .expect("qualified grouped EXPLAIN execution SQL should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT age, COUNT(*) \
                 FROM SessionSqlEntity \
                 WHERE age >= 21 \
                 GROUP BY age \
                 ORDER BY age DESC LIMIT 2 OFFSET 1",
            )
            .expect("unqualified grouped EXPLAIN execution SQL should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified grouped identifiers should normalize to the same execution EXPLAIN descriptor output",
        );
    }

    #[test]
    fn explain_sql_json_grouped_qualified_identifiers_match_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT SessionSqlEntity.age, COUNT(*) \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21 \
                 GROUP BY SessionSqlEntity.age \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            )
            .expect("qualified grouped EXPLAIN JSON SQL should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT age, COUNT(*) \
                 FROM SessionSqlEntity \
                 WHERE age >= 21 \
                 GROUP BY age \
                 ORDER BY age DESC LIMIT 2 OFFSET 1",
            )
            .expect("unqualified grouped EXPLAIN JSON SQL should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified grouped identifiers should normalize to the same EXPLAIN JSON output",
        );
    }

    #[test]
    fn explain_sql_plan_qualified_identifiers_match_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT * \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21 \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            )
            .expect("qualified EXPLAIN plan SQL should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT * \
                 FROM SessionSqlEntity \
                 WHERE age >= 21 \
                 ORDER BY age DESC LIMIT 1",
            )
            .expect("unqualified EXPLAIN plan SQL should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified identifiers should normalize to the same logical EXPLAIN plan output",
        );
    }

    #[test]
    fn explain_sql_execution_qualified_identifiers_match_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT SessionSqlEntity.name \
                 FROM SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21 \
                 ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            )
            .expect("qualified EXPLAIN execution SQL should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT name \
                 FROM SessionSqlEntity \
                 WHERE age >= 21 \
                 ORDER BY age DESC LIMIT 1",
            )
            .expect("unqualified EXPLAIN execution SQL should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified identifiers should normalize to the same execution EXPLAIN descriptor output",
        );
    }

    #[test]
    fn explain_sql_json_qualified_aggregate_matches_unqualified_output() {
        reset_session_sql_store();
        let session = sql_session();

        let qualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT SUM(SessionSqlEntity.age) \
                 FROM public.SessionSqlEntity \
                 WHERE SessionSqlEntity.age >= 21",
            )
            .expect("qualified global aggregate EXPLAIN JSON should succeed");
        let unqualified = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT SUM(age) FROM SessionSqlEntity WHERE age >= 21",
            )
            .expect("unqualified global aggregate EXPLAIN JSON should succeed");

        assert_eq!(
            qualified, unqualified,
            "qualified identifiers should normalize to the same global aggregate EXPLAIN JSON output",
        );
    }

    #[test]
    fn explain_sql_plan_select_distinct_star_marks_distinct_true() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            )
            .expect("EXPLAIN SELECT DISTINCT * should succeed");

        assert!(
            explain.contains("distinct=true"),
            "logical explain text should preserve scalar distinct intent",
        );
    }

    #[test]
    fn explain_sql_execution_select_distinct_star_returns_execution_descriptor_text() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            )
            .expect("EXPLAIN EXECUTION SELECT DISTINCT * should succeed");

        assert!(
            explain.contains("node_id=0"),
            "execution explain output should include the root descriptor node id",
        );
    }

    #[test]
    fn explain_sql_json_returns_logical_plan_json() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            )
            .expect("EXPLAIN JSON should succeed");

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "logical explain JSON should render one JSON object payload",
        );
        assert!(
            explain.contains("\"mode\":{\"type\":\"Load\""),
            "logical explain JSON should expose structured query mode metadata",
        );
        assert!(
            explain.contains("\"access\":"),
            "logical explain JSON should include projected access metadata",
        );
    }

    #[test]
    fn explain_sql_json_select_distinct_star_marks_distinct_true() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            )
            .expect("EXPLAIN JSON SELECT DISTINCT * should succeed");

        assert!(
            explain.contains("\"distinct\":true"),
            "logical explain JSON should preserve scalar distinct intent",
        );
    }

    #[test]
    fn explain_sql_json_delete_returns_logical_delete_mode() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN JSON DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            )
            .expect("EXPLAIN JSON DELETE should succeed");

        assert!(
            explain.contains("\"mode\":{\"type\":\"Delete\""),
            "logical explain JSON should expose delete query mode metadata",
        );
    }

    #[test]
    fn explain_sql_plan_global_aggregate_returns_logical_plan_text() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>("EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity")
            .expect("global aggregate SQL explain plan should succeed");

        assert!(
            explain.contains("mode=Load"),
            "global aggregate SQL explain plan should project logical load mode",
        );
        assert!(
            explain.contains("access="),
            "global aggregate SQL explain plan should include logical access projection",
        );
    }

    #[test]
    fn explain_sql_execution_global_aggregate_returns_execution_descriptor_text() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionSqlEntity",
            )
            .expect("global aggregate SQL explain execution should succeed");

        assert!(
            explain.contains("AggregateCount execution_mode="),
            "global aggregate SQL explain execution should include aggregate terminal node heading",
        );
        assert!(
            explain.contains("node_id=0"),
            "global aggregate SQL explain execution should include root node id",
        );
    }

    #[test]
    fn explain_sql_json_global_aggregate_returns_logical_plan_json() {
        reset_session_sql_store();
        let session = sql_session();

        let explain = session
            .explain_sql::<SessionSqlEntity>("EXPLAIN JSON SELECT COUNT(*) FROM SessionSqlEntity")
            .expect("global aggregate SQL explain json should succeed");

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "global aggregate SQL explain json should render one JSON object payload",
        );
        assert!(
            explain.contains("\"mode\":{\"type\":\"Load\""),
            "global aggregate SQL explain json should expose logical query mode metadata",
        );
    }

    #[test]
    fn explain_sql_global_aggregate_rejects_unknown_target_field() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .explain_sql::<SessionSqlEntity>(
                "EXPLAIN EXECUTION SELECT SUM(missing_field) FROM SessionSqlEntity",
            )
            .expect_err("global aggregate SQL explain should reject unknown target fields");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "global aggregate SQL explain should map unknown target field to unsupported execution error boundary",
        );
    }

    #[test]
    fn explain_sql_rejects_distinct_without_pk_projection_in_current_slice() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .explain_sql::<SessionSqlEntity>("EXPLAIN SELECT DISTINCT age FROM SessionSqlEntity")
            .expect_err("EXPLAIN SELECT DISTINCT without PK projection should remain fail-closed");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "unsupported DISTINCT explain shape should map to unsupported execution error boundary",
        );
    }

    #[test]
    fn explain_sql_rejects_non_explain_statements() {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .explain_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity")
            .expect_err("explain_sql must reject non-EXPLAIN statements");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "non-EXPLAIN input must fail as unsupported explain usage",
        );
    }
}
