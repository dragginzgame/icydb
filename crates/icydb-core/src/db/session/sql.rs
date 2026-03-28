use crate::{
    db::{
        DbSession, EntityFieldDescription, EntityResponse, EntitySchemaDescription,
        MissingRowPolicy, PagedGroupedExecutionWithTrace, PersistedRow, Query, QueryError,
        executor::{
            EntityAuthority, KernelRow, ScalarNumericFieldBoundaryRequest,
            ScalarProjectionBoundaryRequest, execute_sql_delete_projection_for_canister,
            execute_sql_projection_rows_for_canister,
        },
        query::{
            builder::aggregate::{AggregateExpr, avg, count, count_by, max_by, min_by, sum},
            intent::StructuralQuery,
            plan::{
                AggregateKind, FieldSlot,
                expr::{Expr, ProjectionField},
                resolve_aggregate_target_field_slot,
            },
        },
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSelectShape, LoweredSqlCommand, LoweredSqlLaneKind,
            LoweredSqlQuery, PreparedSqlStatement as CorePreparedSqlStatement,
            SqlGlobalAggregateCommandCore, SqlGlobalAggregateTerminal, apply_lowered_select_shape,
            bind_lowered_sql_delete_query_structural,
            bind_lowered_sql_explain_global_aggregate_structural, bind_lowered_sql_query,
            compile_sql_global_aggregate_command, lower_sql_command_from_prepared_statement,
            lowered_sql_command_lane, prepare_sql_statement,
            render_lowered_sql_explain_plan_or_json,
        },
        sql::parser::{
            SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlStatement, parse_sql,
        },
    },
    model::EntityModel,
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

///
/// SqlDispatchResult
///
/// Unified SQL dispatch payload returned by shared SQL lane execution.
///
#[derive(Debug)]
pub enum SqlDispatchResult {
    Projection {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
    },
    Explain(String),
    Describe(EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

///
/// SqlParsedStatement
///
/// Opaque parsed SQL statement envelope with stable route metadata.
/// This allows callers to parse once and reuse parsed authority across
/// route classification and typed dispatch lowering.
///
#[derive(Clone, Debug)]
pub struct SqlParsedStatement {
    statement: SqlStatement,
    route: SqlStatementRoute,
}

impl SqlParsedStatement {
    /// Borrow canonical route metadata for this parsed statement.
    #[must_use]
    pub const fn route(&self) -> &SqlStatementRoute {
        &self.route
    }

    /// Return whether this parsed statement is one delete-like query-surface shape.
    ///
    /// The generated canister query lane is intentionally narrower than the
    /// typed session SQL surface. It must reject both executable `DELETE` and
    /// `EXPLAIN DELETE` so query-only canister exports do not retain delete
    /// execution or delete-specific explain handling.
    #[must_use]
    pub const fn is_delete_like_query_surface(&self) -> bool {
        matches!(
            &self.statement,
            SqlStatement::Delete(_)
                | SqlStatement::Explain(SqlExplainStatement {
                    statement: SqlExplainTarget::Delete(_),
                    ..
                })
        )
    }

    // Prepare this parsed statement for one concrete entity route.
    fn prepare(
        &self,
        expected_entity: &'static str,
    ) -> Result<CorePreparedSqlStatement, QueryError> {
        prepare_sql_statement(self.statement.clone(), expected_entity)
            .map_err(QueryError::from_sql_lowering_error)
    }

    /// Lower this parsed statement into one shared query-lane shape.
    #[inline(never)]
    pub fn lower_query_lane_for_entity(
        &self,
        expected_entity: &'static str,
        primary_key_field: &str,
    ) -> Result<LoweredSqlCommand, QueryError> {
        let lowered = lower_sql_command_from_prepared_statement(
            self.prepare(expected_entity)?,
            primary_key_field,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let lane = lowered_sql_command_lane(&lowered);

        match lane {
            LoweredSqlLaneKind::Query | LoweredSqlLaneKind::Explain => Ok(lowered),
            LoweredSqlLaneKind::Describe
            | LoweredSqlLaneKind::ShowIndexes
            | LoweredSqlLaneKind::ShowColumns
            | LoweredSqlLaneKind::ShowEntities => {
                Err(QueryError::unsupported_query_lane_dispatch())
            }
        }
    }
}

///
/// SqlProjectionPayload
///
/// Generic-free row-oriented SQL projection payload carried across the shared
/// SQL dispatch surface.
/// Keeps SQL `SELECT` results structural so query-lane dispatch does not
/// rebuild typed response rows before rendering values.
///

#[derive(Debug)]
struct SqlProjectionPayload {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
}

impl SqlProjectionPayload {
    #[must_use]
    const fn new(columns: Vec<String>, rows: Vec<Vec<Value>>, row_count: u32) -> Self {
        Self {
            columns,
            rows,
            row_count,
        }
    }

    #[must_use]
    fn into_dispatch_result(self) -> SqlDispatchResult {
        SqlDispatchResult::Projection {
            columns: self.columns,
            rows: self.rows,
            row_count: self.row_count,
        }
    }
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
}

// Resolve one generic-free lowered SQL command to the session lane taxonomy.
const fn session_sql_lane(command: &LoweredSqlCommand) -> SqlLaneKind {
    match lowered_sql_command_lane(command) {
        LoweredSqlLaneKind::Query => SqlLaneKind::Query,
        LoweredSqlLaneKind::Explain => SqlLaneKind::Explain,
        LoweredSqlLaneKind::Describe => SqlLaneKind::Describe,
        LoweredSqlLaneKind::ShowIndexes => SqlLaneKind::ShowIndexes,
        LoweredSqlLaneKind::ShowColumns => SqlLaneKind::ShowColumns,
        LoweredSqlLaneKind::ShowEntities => SqlLaneKind::ShowEntities,
    }
}

// Render one deterministic unsupported-lane message for one SQL surface.
const fn unsupported_sql_lane_message(surface: SqlSurface, lane: SqlLaneKind) -> &'static str {
    match (surface, lane) {
        (SqlSurface::QueryFrom, SqlLaneKind::Explain) => {
            "query_from_sql does not accept EXPLAIN; use execute_sql_dispatch(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Describe) => {
            "query_from_sql does not accept DESCRIBE; use execute_sql_dispatch(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowIndexes) => {
            "query_from_sql does not accept SHOW INDEXES; use execute_sql_dispatch(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowColumns) => {
            "query_from_sql does not accept SHOW COLUMNS; use execute_sql_dispatch(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowEntities) => {
            "query_from_sql does not accept SHOW ENTITIES; use execute_sql_dispatch(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Query) => {
            "query_from_sql only accepts SELECT or DELETE"
        }
        (SqlSurface::Explain, SqlLaneKind::Describe) => {
            "explain_sql does not accept DESCRIBE; use execute_sql_dispatch(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowIndexes) => {
            "explain_sql does not accept SHOW INDEXES; use execute_sql_dispatch(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowColumns) => {
            "explain_sql does not accept SHOW COLUMNS; use execute_sql_dispatch(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowEntities) => {
            "explain_sql does not accept SHOW ENTITIES; use execute_sql_dispatch(...)"
        }
        (SqlSurface::Explain, SqlLaneKind::Query | SqlLaneKind::Explain) => {
            "explain_sql requires EXPLAIN"
        }
    }
}

// Resolve one parsed reduced SQL statement to canonical surface route metadata.
fn sql_statement_route_from_statement(statement: &SqlStatement) -> SqlStatementRoute {
    match statement {
        SqlStatement::Select(select) => SqlStatementRoute::Query {
            entity: select.entity.clone(),
        },
        SqlStatement::Delete(delete) => SqlStatementRoute::Query {
            entity: delete.entity.clone(),
        },
        SqlStatement::Explain(explain) => match &explain.statement {
            SqlExplainTarget::Select(select) => SqlStatementRoute::Explain {
                entity: select.entity.clone(),
            },
            SqlExplainTarget::Delete(delete) => SqlStatementRoute::Explain {
                entity: delete.entity.clone(),
            },
        },
        SqlStatement::Describe(describe) => SqlStatementRoute::Describe {
            entity: describe.entity.clone(),
        },
        SqlStatement::ShowIndexes(show_indexes) => SqlStatementRoute::ShowIndexes {
            entity: show_indexes.entity.clone(),
        },
        SqlStatement::ShowColumns(show_columns) => SqlStatementRoute::ShowColumns {
            entity: show_columns.entity.clone(),
        },
        SqlStatement::ShowEntities(_) => SqlStatementRoute::ShowEntities,
    }
}

// Resolve one aggregate target field through planner slot contracts before
// aggregate terminal execution.
fn resolve_sql_aggregate_target_slot_with_model(
    model: &'static EntityModel,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    resolve_aggregate_target_field_slot(model, field)
}

fn resolve_sql_aggregate_target_slot<E: EntityKind>(field: &str) -> Result<FieldSlot, QueryError> {
    resolve_sql_aggregate_target_slot_with_model(E::MODEL, field)
}

// Convert one lowered global SQL aggregate terminal into aggregate expression
// contracts used by aggregate explain execution descriptors.
fn sql_global_aggregate_terminal_to_expr_with_model(
    model: &'static EntityModel,
    terminal: &SqlGlobalAggregateTerminal,
) -> Result<AggregateExpr, QueryError> {
    match terminal {
        SqlGlobalAggregateTerminal::CountRows => Ok(count()),
        SqlGlobalAggregateTerminal::CountField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(count_by(field.as_str()))
        }
        SqlGlobalAggregateTerminal::SumField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(sum(field.as_str()))
        }
        SqlGlobalAggregateTerminal::AvgField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(avg(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MinField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(min_by(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MaxField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

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

// Derive canonical projection column labels from one structural query projection spec.
fn projection_labels_from_structural_query(
    query: &StructuralQuery,
) -> Result<Vec<String>, QueryError> {
    let projection = query.build_plan()?.projection_spec(query.model());
    Ok(projection_labels_from_projection_spec(&projection))
}

// Render canonical projection labels from one projection spec regardless of
// whether the caller arrived from a typed or structural query shell.
fn projection_labels_from_projection_spec(
    projection: &crate::db::query::plan::expr::ProjectionSpec,
) -> Vec<String> {
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

    labels
}

// Derive canonical full-entity projection labels in declared model order.
fn projection_labels_from_entity_model(model: &'static EntityModel) -> Vec<String> {
    model
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect()
}

// Materialize structural kernel rows into canonical SQL projection rows at the
// session boundary instead of inside executor delete paths.
fn sql_projection_rows_from_kernel_rows(rows: Vec<KernelRow>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_slots()
                .into_iter()
                .map(|value| value.unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

impl LoweredSqlCommand {
    /// Render this lowered SQL command through the shared EXPLAIN surface for
    /// one concrete model authority.
    #[inline(never)]
    pub fn explain_for_model(&self, model: &'static EntityModel) -> Result<String, QueryError> {
        // First validate lane selection once on the shared lowered-command path
        // so explain callers do not rebuild lane guards around the same shape.
        let lane = session_sql_lane(self);
        if lane != SqlLaneKind::Explain {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::Explain,
                lane,
            )));
        }

        // Then prefer the structural renderer because plan/json explain output
        // can stay generic-free all the way to the final render step.
        if let Some(rendered) =
            render_lowered_sql_explain_plan_or_json(self, model, MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)?
        {
            return Ok(rendered);
        }

        // Structural global aggregate explain is the remaining explain-only
        // shape that still needs dedicated aggregate descriptor rendering.
        if let Some((mode, command)) = bind_lowered_sql_explain_global_aggregate_structural(
            self,
            model,
            MissingRowPolicy::Ignore,
        ) {
            return explain_sql_global_aggregate_structural(mode, command);
        }

        Err(QueryError::unsupported_query(
            "shared EXPLAIN dispatch could not classify the lowered SQL command shape",
        ))
    }
}

// Render one EXPLAIN payload for constrained global aggregate SQL command
// entirely through structural query and descriptor authority.
#[inline(never)]
fn explain_sql_global_aggregate_structural(
    mode: SqlExplainMode,
    command: SqlGlobalAggregateCommandCore,
) -> Result<String, QueryError> {
    let model = command.query().model();

    match mode {
        SqlExplainMode::Plan => {
            let _ = sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

            Ok(command
                .query()
                .build_plan()?
                .explain_with_model(model)
                .render_text_canonical())
        }
        SqlExplainMode::Execution => {
            let aggregate =
                sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;
            let plan = command.query().explain_aggregate_terminal(aggregate)?;

            Ok(plan.execution_node_descriptor().render_text_tree())
        }
        SqlExplainMode::Json => {
            let _ = sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

            Ok(command
                .query()
                .build_plan()?
                .explain_with_model(model)
                .render_json_canonical())
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Lower one parsed SQL statement into the shared query lane and bind the
    // resulting lowered query shape onto one typed query owner exactly once.
    fn bind_sql_query_lane_from_parsed<E>(
        parsed: &SqlParsedStatement,
    ) -> Result<(LoweredSqlQuery, Query<E>), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let lowered =
            parsed.lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key.name)?;
        let lane = session_sql_lane(&lowered);
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                lane,
            )));
        };
        let typed = bind_lowered_sql_query::<E>(query.clone(), MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        Ok((query, typed))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        let columns = projection_labels_from_structural_query(&query)?;
        let projected = execute_sql_projection_rows_for_canister(
            &self.db,
            self.debug,
            authority,
            query.build_plan()?,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    // Execute one typed SQL delete query while keeping the row payload on the
    // typed delete executor boundary that still owns non-runtime-hook delete
    // commit-window application.
    fn execute_typed_sql_delete<E>(&self, query: &Query<E>) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();
        let deleted = self
            .with_metrics(|| self.delete_executor::<E>().execute_sql_projection(plan))
            .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows);

        Ok(SqlProjectionPayload::new(
            projection_labels_from_entity_model(E::MODEL),
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    // Validate that one SQL-derived query intent matches the grouped/scalar
    // execution surface that is about to consume it.
    fn ensure_sql_query_grouping<E>(query: &Query<E>, grouped: bool) -> Result<(), QueryError>
    where
        E: EntityKind,
    {
        match (grouped, query.has_grouping()) {
            (true, true) | (false, false) => Ok(()),
            (false, true) => Err(QueryError::grouped_requires_execute_grouped()),
            (true, false) => Err(QueryError::unsupported_query(
                "execute_sql_grouped requires grouped SQL query intent",
            )),
        }
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path.
    #[inline(never)]
    fn execute_lowered_sql_dispatch_select_core(
        &self,
        select: &LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let structural = apply_lowered_select_shape(
            StructuralQuery::new(authority.model(), MissingRowPolicy::Ignore),
            select.clone(),
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        self.execute_structural_sql_projection(structural, authority)
            .map(SqlProjectionPayload::into_dispatch_result)
    }

    /// Parse one reduced SQL statement and return one reusable parsed envelope.
    ///
    /// This method is the SQL parse authority for dynamic route selection.
    pub fn parse_sql_statement(&self, sql: &str) -> Result<SqlParsedStatement, QueryError> {
        let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;
        let route = sql_statement_route_from_statement(&statement);

        Ok(SqlParsedStatement { statement, route })
    }

    /// Parse one reduced SQL statement into canonical routing metadata.
    ///
    /// This method is the SQL dispatch authority for entity/surface routing
    /// outside typed-entity lowering paths.
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, QueryError> {
        let parsed = self.parse_sql_statement(sql)?;

        Ok(parsed.route().clone())
    }

    /// Build one typed query intent from one reduced SQL statement.
    ///
    /// This parser/lowering entrypoint is intentionally constrained to the
    /// executable subset wired in the current release.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let parsed = self.parse_sql_statement(sql)?;
        let (_, query) = Self::bind_sql_query_lane_from_parsed::<E>(&parsed)?;

        Ok(query)
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement for entity `E`.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        Self::ensure_sql_query_grouping(&query, false)?;

        self.execute_query(&query)
    }

    /// Execute one reduced SQL global aggregate `SELECT` statement.
    ///
    /// This entrypoint is intentionally constrained to one aggregate terminal
    /// shape per statement and preserves existing terminal semantics.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let command = compile_sql_global_aggregate_command::<E>(sql, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        match command.terminal() {
            SqlGlobalAggregateTerminal::CountRows => self
                .execute_load_query_with(command.query(), |load, plan| {
                    load.execute_scalar_terminal_request(
                        plan,
                        crate::db::executor::ScalarTerminalBoundaryRequest::Count,
                    )?
                    .into_count()
                })
                .map(|count| Value::Uint(u64::from(count))),
            SqlGlobalAggregateTerminal::CountField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_scalar_projection_boundary(
                        plan,
                        target_slot,
                        ScalarProjectionBoundaryRequest::Values,
                    )?
                    .into_values()
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
                    load.execute_numeric_field_boundary(
                        plan,
                        target_slot,
                        ScalarNumericFieldBoundaryRequest::Sum,
                    )
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::AvgField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_numeric_field_boundary(
                        plan,
                        target_slot,
                        ScalarNumericFieldBoundaryRequest::Avg,
                    )
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::MinField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
                let min_id = self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_scalar_terminal_request(
                        plan,
                        crate::db::executor::ScalarTerminalBoundaryRequest::IdBySlot {
                            kind: AggregateKind::Min,
                            target_field: target_slot,
                        },
                    )?
                    .into_id()
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
                    load.execute_scalar_terminal_request(
                        plan,
                        crate::db::executor::ScalarTerminalBoundaryRequest::IdBySlot {
                            kind: AggregateKind::Max,
                            target_field: target_slot,
                        },
                    )?
                    .into_id()
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        Self::ensure_sql_query_grouping(&query, true)?;

        self.execute_grouped(&query, cursor_token)
    }

    /// Execute one reduced SQL statement into one unified SQL dispatch payload.
    pub fn execute_sql_dispatch<E>(&self, sql: &str) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        self.execute_sql_dispatch_parsed::<E>(&parsed)
    }

    /// Execute one parsed reduced SQL statement into one unified SQL payload.
    pub fn execute_sql_dispatch_parsed<E>(
        &self,
        parsed: &SqlParsedStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => {
                let (query, typed_query) = Self::bind_sql_query_lane_from_parsed::<E>(parsed)?;

                Self::ensure_sql_query_grouping(&typed_query, false)?;

                match query {
                    LoweredSqlQuery::Select(select) => self
                        .execute_lowered_sql_dispatch_select_core(
                            &select,
                            EntityAuthority::for_type::<E>(),
                        ),
                    LoweredSqlQuery::Delete(_) => self.execute_typed_sql_delete(&typed_query),
                }
            }
            SqlStatementRoute::Explain { .. } => {
                let lowered = lower_sql_command_from_prepared_statement(
                    parsed.prepare(E::MODEL.name())?,
                    E::MODEL.primary_key.name,
                )
                .map_err(QueryError::from_sql_lowering_error)?;

                lowered
                    .explain_for_model(E::MODEL)
                    .map(SqlDispatchResult::Explain)
            }
            SqlStatementRoute::Describe { .. } => {
                Ok(SqlDispatchResult::Describe(self.describe_entity::<E>()))
            }
            SqlStatementRoute::ShowIndexes { .. } => {
                Ok(SqlDispatchResult::ShowIndexes(self.show_indexes::<E>()))
            }
            SqlStatementRoute::ShowColumns { .. } => {
                Ok(SqlDispatchResult::ShowColumns(self.show_columns::<E>()))
            }
            SqlStatementRoute::ShowEntities => {
                Ok(SqlDispatchResult::ShowEntities(self.show_entities()))
            }
        }
    }

    // Execute one lowered SQL DELETE command through the shared structural
    // delete projection path.
    fn execute_lowered_sql_dispatch_delete_core(
        &self,
        delete: &LoweredBaseQueryShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let structural = bind_lowered_sql_delete_query_structural(
            authority.model(),
            delete.clone(),
            MissingRowPolicy::Ignore,
        );
        let deleted = execute_sql_delete_projection_for_canister(
            &self.db,
            authority,
            structural.build_plan()?,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows);

        Ok(SqlProjectionPayload::new(
            projection_labels_from_entity_model(authority.model()),
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    /// Execute one already-lowered shared SQL query shape for resolved authority.
    #[doc(hidden)]
    pub fn execute_lowered_sql_dispatch_query_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        self.execute_lowered_sql_dispatch_query_core(lowered, authority)
    }

    /// Execute one already-lowered shared SQL `SELECT` shape for resolved authority.
    ///
    /// This narrower boundary exists specifically for generated canister query
    /// surfaces that must not retain delete execution when the public SQL
    /// export is intentionally query-only.
    #[doc(hidden)]
    pub fn execute_lowered_sql_dispatch_select_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let Some(query) = lowered.query() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                session_sql_lane(lowered),
            )));
        };

        match query {
            LoweredSqlQuery::Select(select) => {
                self.execute_lowered_sql_dispatch_select_core(select, authority)
            }
            LoweredSqlQuery::Delete(_) => Err(QueryError::unsupported_query(
                "generated SQL query dispatch requires lowered SELECT",
            )),
        }
    }

    // Execute one lowered SQL query command through the shared structural core
    // and delegate only true typed DELETE fallback to the caller.
    fn execute_lowered_sql_dispatch_query_core(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let Some(query) = lowered.query() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                session_sql_lane(lowered),
            )));
        };

        match query {
            LoweredSqlQuery::Select(select) => {
                self.execute_lowered_sql_dispatch_select_core(select, authority)
            }
            LoweredSqlQuery::Delete(delete) => {
                self.execute_lowered_sql_dispatch_delete_core(delete, authority)
            }
        }
    }
}
