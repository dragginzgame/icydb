use crate::{
    db::{
        DbSession, EntityResponse, MissingRowPolicy, PagedGroupedExecutionWithTrace,
        ProjectionResponse, Query, QueryError,
        query::{
            builder::aggregate::{AggregateExpr, avg, count, count_by, max_by, min_by, sum},
            intent::IntentError,
            plan::{FieldSlot, QueryMode},
        },
        sql::lowering::{
            SqlCommand, SqlGlobalAggregateCommand, SqlGlobalAggregateTerminal, SqlLoweringError,
            compile_sql_command, compile_sql_global_aggregate_command,
        },
        sql::parser::SqlExplainMode,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{CanisterKind, EntityKind, EntityValue},
    value::Value,
};

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

impl<C: CanisterKind> DbSession<C> {
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
}
