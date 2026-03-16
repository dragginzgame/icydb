use crate::{
    db::{
        DbSession, EntityFieldDescription, EntityResponse, EntitySchemaDescription,
        MissingRowPolicy, PagedGroupedExecutionWithTrace, ProjectionResponse, Query, QueryError,
        query::{
            builder::aggregate::{AggregateExpr, avg, count, count_by, max_by, min_by, sum},
            intent::IntentError,
            plan::{
                AggregateKind, FieldSlot, QueryMode,
                expr::{Expr, ProjectionField},
            },
        },
        sql::lowering::{
            SqlCommand, SqlGlobalAggregateCommand, SqlGlobalAggregateTerminal, SqlLoweringError,
            compile_sql_command, compile_sql_global_aggregate_command,
        },
        sql::parser::{SqlExplainMode, SqlExplainTarget, SqlStatement, parse_sql},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{CanisterKind, EntityKind, EntityValue},
    value::Value,
};

///
/// SqlStatementRoute
///
/// Canonical SQL statement routing metadata derived from reduced SQL parser output.
/// Carries surface kind (`Query` / `Explain` / `Describe` / `ShowIndexes` /
/// `ShowColumns` / `ShowEntities`) and canonical parsed entity identifier.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlStatementRoute {
    Query { entity: String },
    Explain { entity: String },
    Describe { entity: String },
    ShowIndexes { entity: String },
    ShowColumns { entity: String },
    ShowEntities,
}

impl SqlStatementRoute {
    /// Borrow the parsed SQL entity identifier for this statement.
    ///
    /// `SHOW ENTITIES` does not carry an entity identifier and returns an
    /// empty string for this accessor.
    #[must_use]
    pub const fn entity(&self) -> &str {
        match self {
            Self::Query { entity }
            | Self::Explain { entity }
            | Self::Describe { entity }
            | Self::ShowIndexes { entity }
            | Self::ShowColumns { entity } => entity.as_str(),
            Self::ShowEntities => "",
        }
    }

    /// Return whether this route targets the EXPLAIN surface.
    #[must_use]
    pub const fn is_explain(&self) -> bool {
        matches!(self, Self::Explain { .. })
    }

    /// Return whether this route targets the DESCRIBE surface.
    #[must_use]
    pub const fn is_describe(&self) -> bool {
        matches!(self, Self::Describe { .. })
    }

    /// Return whether this route targets the `SHOW INDEXES` surface.
    #[must_use]
    pub const fn is_show_indexes(&self) -> bool {
        matches!(self, Self::ShowIndexes { .. })
    }

    /// Return whether this route targets the `SHOW COLUMNS` surface.
    #[must_use]
    pub const fn is_show_columns(&self) -> bool {
        matches!(self, Self::ShowColumns { .. })
    }

    /// Return whether this route targets the `SHOW ENTITIES` surface.
    #[must_use]
    pub const fn is_show_entities(&self) -> bool {
        matches!(self, Self::ShowEntities)
    }
}

// Canonical reduced SQL lane kind used by session entrypoint gate checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlLaneKind {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

// Session SQL surfaces that enforce explicit wrong-lane fail-closed contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlSurface {
    QueryFrom,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

// Resolve one lowered SQL command to its canonical lane kind.
const fn sql_command_lane<E: EntityKind>(command: &SqlCommand<E>) -> SqlLaneKind {
    match command {
        SqlCommand::Query(_) => SqlLaneKind::Query,
        SqlCommand::Explain { .. } | SqlCommand::ExplainGlobalAggregate { .. } => {
            SqlLaneKind::Explain
        }
        SqlCommand::DescribeEntity => SqlLaneKind::Describe,
        SqlCommand::ShowIndexesEntity => SqlLaneKind::ShowIndexes,
        SqlCommand::ShowColumnsEntity => SqlLaneKind::ShowColumns,
        SqlCommand::ShowEntities => SqlLaneKind::ShowEntities,
    }
}

// Resolve one parser-derived SQL route to its canonical lane kind.
const fn sql_statement_route_lane(route: &SqlStatementRoute) -> SqlLaneKind {
    match route {
        SqlStatementRoute::Query { .. } => SqlLaneKind::Query,
        SqlStatementRoute::Explain { .. } => SqlLaneKind::Explain,
        SqlStatementRoute::Describe { .. } => SqlLaneKind::Describe,
        SqlStatementRoute::ShowIndexes { .. } => SqlLaneKind::ShowIndexes,
        SqlStatementRoute::ShowColumns { .. } => SqlLaneKind::ShowColumns,
        SqlStatementRoute::ShowEntities => SqlLaneKind::ShowEntities,
    }
}

// Render one deterministic unsupported-lane message for one SQL surface.
const fn unsupported_sql_lane_message(surface: SqlSurface, lane: SqlLaneKind) -> &'static str {
    match (surface, lane) {
        (SqlSurface::QueryFrom, SqlLaneKind::Explain) => {
            "query_from_sql does not accept EXPLAIN statements; use explain_sql(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Describe) => {
            "query_from_sql does not accept DESCRIBE statements; use describe_sql(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowIndexes) => {
            "query_from_sql does not accept SHOW INDEXES statements; use show_indexes_sql(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowColumns) => {
            "query_from_sql does not accept SHOW COLUMNS statements; use show_columns_sql(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowEntities) => {
            "query_from_sql does not accept SHOW ENTITIES/SHOW TABLES statements; use show_entities_sql(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Query) => {
            "query_from_sql requires one executable SELECT or DELETE statement"
        }
        (SqlSurface::Explain, SqlLaneKind::Describe) => {
            "explain_sql does not accept DESCRIBE statements; use describe_sql(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowIndexes) => {
            "explain_sql does not accept SHOW INDEXES statements; use show_indexes_sql(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowColumns) => {
            "explain_sql does not accept SHOW COLUMNS statements; use show_columns_sql(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowEntities) => {
            "explain_sql does not accept SHOW ENTITIES/SHOW TABLES statements; use show_entities_sql(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::Query | SqlLaneKind::Explain) => {
            "explain_sql requires an EXPLAIN statement"
        }
        (SqlSurface::Describe, _) => "describe_sql requires a DESCRIBE statement",
        (SqlSurface::ShowIndexes, _) => "show_indexes_sql requires a SHOW INDEXES statement",
        (SqlSurface::ShowColumns, _) => "show_columns_sql requires a SHOW COLUMNS statement",
        (SqlSurface::ShowEntities, _) => {
            "show_entities_sql requires a SHOW ENTITIES or SHOW TABLES statement"
        }
    }
}

// Build one unsupported execution error for wrong-lane SQL surface usage.
fn unsupported_sql_lane_error(surface: SqlSurface, lane: SqlLaneKind) -> QueryError {
    QueryError::execute(InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
        unsupported_sql_lane_message(surface, lane),
    ))
}

// Compile one reduced SQL statement with default lane behavior used by SQL surfaces.
fn compile_sql_command_ignore<E: EntityKind>(sql: &str) -> Result<SqlCommand<E>, QueryError> {
    compile_sql_command::<E>(sql, MissingRowPolicy::Ignore).map_err(map_sql_lowering_error)
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

// Map reduced SQL parse failures through the same query-facing classification
// policy used by SQL lowering entrypoints.
fn map_sql_parse_error(err: crate::db::sql::parser::SqlParseError) -> QueryError {
    map_sql_lowering_error(SqlLoweringError::Parse(err))
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

// Render one aggregate expression into a canonical projection column label.
fn projection_label_from_aggregate(aggregate: &AggregateExpr) -> String {
    let kind = match aggregate.kind() {
        AggregateKind::Count => "COUNT",
        AggregateKind::Sum => "SUM",
        AggregateKind::Avg => "AVG",
        AggregateKind::Exists => "EXISTS",
        AggregateKind::First => "FIRST",
        AggregateKind::Last => "LAST",
        AggregateKind::Min => "MIN",
        AggregateKind::Max => "MAX",
    };
    let distinct = if aggregate.is_distinct() {
        "DISTINCT "
    } else {
        ""
    };

    if let Some(field) = aggregate.target_field() {
        return format!("{kind}({distinct}{field})");
    }

    format!("{kind}({distinct}*)")
}

// Render one projection expression into a canonical output label.
fn projection_label_from_expr(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Aggregate(aggregate) => projection_label_from_aggregate(aggregate),
        Expr::Alias { name, .. } => name.as_str().to_string(),
        Expr::Literal(_) | Expr::Unary { .. } | Expr::Binary { .. } => {
            format!("expr_{ordinal}")
        }
    }
}

// Derive canonical projection column labels from one planned query projection spec.
fn projection_labels_from_query<E: EntityKind>(
    query: &Query<E>,
) -> Result<Vec<String>, QueryError> {
    let projection = query.plan()?.projection_spec();
    let mut labels = Vec::with_capacity(projection.len());

    for (ordinal, field) in projection.fields().enumerate() {
        match field {
            ProjectionField::Scalar {
                expr: _,
                alias: Some(alias),
            } => labels.push(alias.as_str().to_string()),
            ProjectionField::Scalar { expr, alias: None } => {
                labels.push(projection_label_from_expr(expr, ordinal));
            }
        }
    }

    Ok(labels)
}

impl<C: CanisterKind> DbSession<C> {
    /// Parse one reduced SQL statement into canonical routing metadata.
    ///
    /// This method is the SQL dispatch authority for entity/surface routing
    /// outside typed-entity lowering paths.
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, QueryError> {
        let statement = parse_sql(sql).map_err(map_sql_parse_error)?;
        match statement {
            SqlStatement::Select(select) => Ok(SqlStatementRoute::Query {
                entity: select.entity,
            }),
            SqlStatement::Delete(delete) => Ok(SqlStatementRoute::Query {
                entity: delete.entity,
            }),
            SqlStatement::Explain(explain) => match explain.statement {
                SqlExplainTarget::Select(select) => Ok(SqlStatementRoute::Explain {
                    entity: select.entity,
                }),
                SqlExplainTarget::Delete(delete) => Ok(SqlStatementRoute::Explain {
                    entity: delete.entity,
                }),
            },
            SqlStatement::Describe(describe) => Ok(SqlStatementRoute::Describe {
                entity: describe.entity,
            }),
            SqlStatement::ShowIndexes(show_indexes) => Ok(SqlStatementRoute::ShowIndexes {
                entity: show_indexes.entity,
            }),
            SqlStatement::ShowColumns(show_columns) => Ok(SqlStatementRoute::ShowColumns {
                entity: show_columns.entity,
            }),
            SqlStatement::ShowEntities(_) => Ok(SqlStatementRoute::ShowEntities),
        }
    }

    /// Execute one reduced SQL `DESCRIBE` statement for entity `E`.
    pub fn describe_sql<E>(&self, sql: &str) -> Result<EntitySchemaDescription, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let command = compile_sql_command_ignore::<E>(sql)?;
        let lane = sql_command_lane(&command);

        match command {
            SqlCommand::DescribeEntity => Ok(self.describe_entity::<E>()),
            SqlCommand::Query(_)
            | SqlCommand::Explain { .. }
            | SqlCommand::ExplainGlobalAggregate { .. }
            | SqlCommand::ShowIndexesEntity
            | SqlCommand::ShowColumnsEntity
            | SqlCommand::ShowEntities => {
                Err(unsupported_sql_lane_error(SqlSurface::Describe, lane))
            }
        }
    }

    /// Execute one reduced SQL `SHOW INDEXES` statement for entity `E`.
    pub fn show_indexes_sql<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let command = compile_sql_command_ignore::<E>(sql)?;
        let lane = sql_command_lane(&command);

        match command {
            SqlCommand::ShowIndexesEntity => Ok(self.show_indexes::<E>()),
            SqlCommand::Query(_)
            | SqlCommand::Explain { .. }
            | SqlCommand::ExplainGlobalAggregate { .. }
            | SqlCommand::DescribeEntity
            | SqlCommand::ShowColumnsEntity
            | SqlCommand::ShowEntities => {
                Err(unsupported_sql_lane_error(SqlSurface::ShowIndexes, lane))
            }
        }
    }

    /// Execute one reduced SQL `SHOW COLUMNS` statement for entity `E`.
    pub fn show_columns_sql<E>(&self, sql: &str) -> Result<Vec<EntityFieldDescription>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let command = compile_sql_command_ignore::<E>(sql)?;
        let lane = sql_command_lane(&command);

        match command {
            SqlCommand::ShowColumnsEntity => Ok(self.show_columns::<E>()),
            SqlCommand::Query(_)
            | SqlCommand::Explain { .. }
            | SqlCommand::ExplainGlobalAggregate { .. }
            | SqlCommand::DescribeEntity
            | SqlCommand::ShowIndexesEntity
            | SqlCommand::ShowEntities => {
                Err(unsupported_sql_lane_error(SqlSurface::ShowColumns, lane))
            }
        }
    }

    /// Execute one reduced SQL `SHOW ENTITIES`/`SHOW TABLES` statement.
    pub fn show_entities_sql(&self, sql: &str) -> Result<Vec<String>, QueryError> {
        let statement = self.sql_statement_route(sql)?;
        let lane = sql_statement_route_lane(&statement);
        if lane != SqlLaneKind::ShowEntities {
            return Err(unsupported_sql_lane_error(SqlSurface::ShowEntities, lane));
        }

        Ok(self.show_entities())
    }

    /// Build one typed query intent from one reduced SQL statement.
    ///
    /// This parser/lowering entrypoint is intentionally constrained to the
    /// executable subset wired in the current release.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let command = compile_sql_command_ignore::<E>(sql)?;
        let lane = sql_command_lane(&command);

        match command {
            SqlCommand::Query(query) => Ok(query),
            SqlCommand::Explain { .. }
            | SqlCommand::ExplainGlobalAggregate { .. }
            | SqlCommand::DescribeEntity
            | SqlCommand::ShowIndexesEntity
            | SqlCommand::ShowColumnsEntity
            | SqlCommand::ShowEntities => {
                Err(unsupported_sql_lane_error(SqlSurface::QueryFrom, lane))
            }
        }
    }

    /// Derive canonical projection column labels for one reduced SQL `SELECT` statement.
    pub fn sql_projection_columns<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let query = self.query_from_sql::<E>(sql)?;
        if query.has_grouping() {
            return Err(QueryError::Intent(
                IntentError::GroupedRequiresExecuteGrouped,
            ));
        }

        match query.mode() {
            QueryMode::Load(_) => projection_labels_from_query(&query),
            QueryMode::Delete(_) => Err(QueryError::execute(InternalError::classified(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "sql_projection_columns only supports SELECT statements",
            ))),
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
    /// This surface keeps `execute_sql(...)` stable for callers
    /// that consume full entity rows.
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
        let command = compile_sql_command_ignore::<E>(sql)?;
        let lane = sql_command_lane(&command);

        match command {
            SqlCommand::Query(_)
            | SqlCommand::DescribeEntity
            | SqlCommand::ShowIndexesEntity
            | SqlCommand::ShowColumnsEntity
            | SqlCommand::ShowEntities => {
                Err(unsupported_sql_lane_error(SqlSurface::Explain, lane))
            }
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
