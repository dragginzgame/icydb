//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate},
        query::{
            builder::aggregate::{avg, count, count_by, max_by, min_by, sum},
            intent::{Query, QueryError, StructuralQuery},
            plan::ExpressionOrderTerm,
        },
        sql::identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers,
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlExplainMode,
            SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol,
            SqlOrderDirection, SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement,
            SqlStatement, SqlTextFunctionCall,
        },
    },
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// LoweredSqlCommand
///
/// Generic-free SQL command shape after reduced SQL parsing and entity-route
/// normalization.
/// This keeps statement-shape lowering shared across entities before typed
/// `Query<E>` binding happens at the execution boundary.
///
#[derive(Clone, Debug)]
pub struct LoweredSqlCommand(LoweredSqlCommandInner);

#[derive(Clone, Debug)]
enum LoweredSqlCommandInner {
    Query(LoweredSqlQuery),
    Explain {
        mode: SqlExplainMode,
        query: LoweredSqlQuery,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        command: LoweredSqlGlobalAggregateCommand,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

///
/// SqlCommand
///
/// Test-only typed SQL command shell over the shared lowered SQL surface.
/// Runtime dispatch now consumes `LoweredSqlCommand` directly, but lowering
/// tests still validate typed binding behavior on this local envelope.
///
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum SqlCommand<E: EntityKind> {
    Query(Query<E>),
    Explain {
        mode: SqlExplainMode,
        query: Query<E>,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        command: SqlGlobalAggregateCommand<E>,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

impl LoweredSqlCommand {
    #[must_use]
    pub(in crate::db) const fn query(&self) -> Option<&LoweredSqlQuery> {
        match &self.0 {
            LoweredSqlCommandInner::Query(query) => Some(query),
            LoweredSqlCommandInner::Explain { .. }
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities => None,
        }
    }
}

///
/// LoweredSqlQuery
///
/// Generic-free executable SQL query shape prepared before typed query binding.
/// Select and delete lowering stay shared until the final `Query<E>` build.
///
#[derive(Clone, Debug)]
pub(crate) enum LoweredSqlQuery {
    Select(LoweredSelectShape),
    Delete(LoweredBaseQueryShape),
}

impl LoweredSqlQuery {
    // Report whether this lowered query carries grouped execution semantics.
    pub(crate) const fn has_grouping(&self) -> bool {
        match self {
            Self::Select(select) => select.has_grouping(),
            Self::Delete(_) => false,
        }
    }
}

///
/// SqlGlobalAggregateTerminal
///
/// Global SQL aggregate terminals currently executable through dedicated
/// aggregate SQL entrypoints.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlGlobalAggregateTerminal {
    CountRows,
    CountField(String),
    SumField(String),
    AvgField(String),
    MinField(String),
    MaxField(String),
}

///
/// LoweredSqlGlobalAggregateCommand
///
/// Generic-free global aggregate command shape prepared before typed query
/// binding.
/// This keeps aggregate SQL lowering shared across entities until the final
/// execution boundary converts the base query shape into `Query<E>`.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSqlGlobalAggregateCommand {
    query: LoweredBaseQueryShape,
    terminal: SqlGlobalAggregateTerminal,
}

///
/// LoweredSqlAggregateShape
///
/// Locally validated aggregate-call shape used by SQL lowering to avoid
/// duplicating `(SqlAggregateKind, field)` validation across lowering lanes.
///

enum LoweredSqlAggregateShape {
    CountRows,
    CountField(String),
    FieldTarget {
        kind: SqlAggregateKind,
        field: String,
    },
}

///
/// SqlGlobalAggregateCommand
///
/// Lowered global SQL aggregate command carrying base query shape plus terminal.
///

#[derive(Debug)]
pub(crate) struct SqlGlobalAggregateCommand<E: EntityKind> {
    query: Query<E>,
    terminal: SqlGlobalAggregateTerminal,
}

impl<E: EntityKind> SqlGlobalAggregateCommand<E> {
    /// Borrow the lowered base query shape for aggregate execution.
    #[must_use]
    pub(crate) const fn query(&self) -> &Query<E> {
        &self.query
    }

    /// Borrow the lowered aggregate terminal.
    #[must_use]
    pub(crate) const fn terminal(&self) -> &SqlGlobalAggregateTerminal {
        &self.terminal
    }
}

///
/// SqlGlobalAggregateCommandCore
///
/// Generic-free lowered global aggregate command bound onto the structural
/// query surface.
/// This keeps global aggregate EXPLAIN on the shared query/explain path until
/// a typed boundary is strictly required.
///

#[derive(Debug)]
pub(crate) struct SqlGlobalAggregateCommandCore {
    query: StructuralQuery,
    terminal: SqlGlobalAggregateTerminal,
}

impl SqlGlobalAggregateCommandCore {
    /// Borrow the structural query payload for aggregate explain/execution.
    #[must_use]
    pub(in crate::db) const fn query(&self) -> &StructuralQuery {
        &self.query
    }

    /// Borrow the lowered aggregate terminal.
    #[must_use]
    pub(in crate::db) const fn terminal(&self) -> &SqlGlobalAggregateTerminal {
        &self.terminal
    }
}

///
/// SqlLoweringError
///
/// SQL frontend lowering failures before planner validation/execution.
///

#[derive(Debug, ThisError)]
pub(crate) enum SqlLoweringError {
    #[error("{0}")]
    Parse(#[from] crate::db::sql::parser::SqlParseError),

    #[error("{0}")]
    Query(#[from] QueryError),

    #[error("SQL entity '{sql_entity}' does not match requested entity type '{expected_entity}'")]
    EntityMismatch {
        sql_entity: String,
        expected_entity: &'static str,
    },

    #[error(
        "unsupported SQL SELECT projection; supported forms are SELECT *, field lists, or grouped aggregate shapes"
    )]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT")]
    UnsupportedSelectDistinct,

    #[error("unsupported SQL GROUP BY projection shape")]
    UnsupportedSelectGroupBy,

    #[error("unsupported SQL HAVING shape")]
    UnsupportedSelectHaving,
}

impl SqlLoweringError {
    /// Construct one entity-mismatch SQL lowering error.
    fn entity_mismatch(sql_entity: impl Into<String>, expected_entity: &'static str) -> Self {
        Self::EntityMismatch {
            sql_entity: sql_entity.into(),
            expected_entity,
        }
    }

    /// Construct one unsupported SELECT projection SQL lowering error.
    const fn unsupported_select_projection() -> Self {
        Self::UnsupportedSelectProjection
    }

    /// Construct one unsupported SELECT DISTINCT SQL lowering error.
    const fn unsupported_select_distinct() -> Self {
        Self::UnsupportedSelectDistinct
    }

    /// Construct one unsupported SELECT GROUP BY shape SQL lowering error.
    const fn unsupported_select_group_by() -> Self {
        Self::UnsupportedSelectGroupBy
    }

    /// Construct one unsupported SELECT HAVING shape SQL lowering error.
    const fn unsupported_select_having() -> Self {
        Self::UnsupportedSelectHaving
    }
}

///
/// PreparedSqlStatement
///
/// SQL statement envelope after entity-scope normalization and
/// entity-match validation for one target entity descriptor.
///
/// This pre-lowering contract is entity-agnostic and reusable across
/// dynamic SQL route branches before typed `Query<E>` binding.
///

#[derive(Clone, Debug)]
pub(crate) struct PreparedSqlStatement {
    statement: SqlStatement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoweredSqlLaneKind {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

/// Parse and lower one SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    compile_sql_command_from_statement::<E>(statement, consistency)
}

/// Lower one parsed SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command_from_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let prepared = prepare_sql_statement(statement, E::MODEL.name())?;
    compile_sql_command_from_prepared_statement::<E>(prepared, consistency)
}

/// Lower one prepared SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command_from_prepared_statement<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let lowered = lower_sql_command_from_prepared_statement(prepared, E::MODEL.primary_key.name)?;

    bind_lowered_sql_command::<E>(lowered, consistency)
}

/// Lower one prepared SQL statement into one shared generic-free command shape.
#[inline(never)]
pub(crate) fn lower_sql_command_from_prepared_statement(
    prepared: PreparedSqlStatement,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    lower_prepared_statement(prepared.statement, primary_key_field)
}

pub(crate) const fn lowered_sql_command_lane(command: &LoweredSqlCommand) -> LoweredSqlLaneKind {
    match command.0 {
        LoweredSqlCommandInner::Query(_) => LoweredSqlLaneKind::Query,
        LoweredSqlCommandInner::Explain { .. }
        | LoweredSqlCommandInner::ExplainGlobalAggregate { .. } => LoweredSqlLaneKind::Explain,
        LoweredSqlCommandInner::DescribeEntity => LoweredSqlLaneKind::Describe,
        LoweredSqlCommandInner::ShowIndexesEntity => LoweredSqlLaneKind::ShowIndexes,
        LoweredSqlCommandInner::ShowColumnsEntity => LoweredSqlLaneKind::ShowColumns,
        LoweredSqlCommandInner::ShowEntities => LoweredSqlLaneKind::ShowEntities,
    }
}

/// Return whether one parsed SQL statement is an executable constrained global
/// aggregate shape owned by the dedicated aggregate lane.
pub(in crate::db) fn is_sql_global_aggregate_statement(statement: &SqlStatement) -> bool {
    let SqlStatement::Select(statement) = statement else {
        return false;
    };

    is_sql_global_aggregate_select(statement)
}

// Detect one constrained global aggregate select shape without widening any
// non-aggregate SQL surface onto the dedicated aggregate execution lane.
fn is_sql_global_aggregate_select(statement: &SqlSelectStatement) -> bool {
    if statement.distinct || !statement.group_by.is_empty() || !statement.having.is_empty() {
        return false;
    }

    lower_global_aggregate_terminal(statement.projection.clone()).is_ok()
}

/// Render one lowered EXPLAIN command through the shared structural SQL path.
#[inline(never)]
pub(crate) fn render_lowered_sql_explain_plan_or_json(
    lowered: &LoweredSqlCommand,
    model: &'static crate::model::entity::EntityModel,
    consistency: MissingRowPolicy,
) -> Result<Option<String>, SqlLoweringError> {
    let LoweredSqlCommandInner::Explain { mode, query } = &lowered.0 else {
        return Ok(None);
    };

    let query = bind_lowered_sql_query_structural(model, query.clone(), consistency)?;
    let rendered = match mode {
        SqlExplainMode::Plan | SqlExplainMode::Json => {
            let plan = query.build_plan()?;
            let explain = plan.explain_with_model(model);

            match mode {
                SqlExplainMode::Plan => explain.render_text_canonical(),
                SqlExplainMode::Json => explain.render_json_canonical(),
                SqlExplainMode::Execution => unreachable!("execution mode handled above"),
            }
        }
        SqlExplainMode::Execution => query.explain_execution_text()?,
    };

    Ok(Some(rendered))
}

/// Bind one lowered global aggregate EXPLAIN shape onto the structural query
/// surface when the explain command carries that specialized form.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_structural(
    lowered: &LoweredSqlCommand,
    model: &'static crate::model::entity::EntityModel,
    consistency: MissingRowPolicy,
) -> Option<(SqlExplainMode, SqlGlobalAggregateCommandCore)> {
    let LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command } = &lowered.0 else {
        return None;
    };

    Some((
        *mode,
        bind_lowered_sql_global_aggregate_command_structural(model, command.clone(), consistency),
    ))
}

/// Bind one shared generic-free SQL command shape to the typed query surface.
#[cfg(test)]
pub(crate) fn bind_lowered_sql_command<E: EntityKind>(
    lowered: LoweredSqlCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match lowered.0 {
        LoweredSqlCommandInner::Query(query) => Ok(SqlCommand::Query(bind_lowered_sql_query::<E>(
            query,
            consistency,
        )?)),
        LoweredSqlCommandInner::Explain { mode, query } => Ok(SqlCommand::Explain {
            mode,
            query: bind_lowered_sql_query::<E>(query, consistency)?,
        }),
        LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command } => {
            Ok(SqlCommand::ExplainGlobalAggregate {
                mode,
                command: bind_lowered_sql_global_aggregate_command::<E>(command, consistency),
            })
        }
        LoweredSqlCommandInner::DescribeEntity => Ok(SqlCommand::DescribeEntity),
        LoweredSqlCommandInner::ShowIndexesEntity => Ok(SqlCommand::ShowIndexesEntity),
        LoweredSqlCommandInner::ShowColumnsEntity => Ok(SqlCommand::ShowColumnsEntity),
        LoweredSqlCommandInner::ShowEntities => Ok(SqlCommand::ShowEntities),
    }
}

/// Prepare one parsed SQL statement for one expected entity route.
#[inline(never)]
pub(crate) fn prepare_sql_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<PreparedSqlStatement, SqlLoweringError> {
    let statement = prepare_statement(statement, expected_entity)?;

    Ok(PreparedSqlStatement { statement })
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = prepare_sql_statement(statement, E::MODEL.name())?;
    compile_sql_global_aggregate_command_from_prepared::<E>(prepared, consistency)
}

// Lower one already-prepared SQL statement into the constrained global
// aggregate command envelope so callers that already parsed and routed the
// statement do not pay the parser again.
pub(crate) fn compile_sql_global_aggregate_command_from_prepared<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    Ok(bind_lowered_sql_global_aggregate_command::<E>(
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    ))
}

#[inline(never)]
fn prepare_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<SqlStatement, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(SqlStatement::Select(prepare_select_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Delete(statement) => Ok(SqlStatement::Delete(prepare_delete_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Explain(statement) => Ok(SqlStatement::Explain(prepare_explain_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Describe(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::Describe(statement))
        }
        SqlStatement::ShowIndexes(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowIndexes(statement))
        }
        SqlStatement::ShowColumns(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowColumns(statement))
        }
        SqlStatement::ShowEntities(statement) => Ok(SqlStatement::ShowEntities(statement)),
    }
}

fn prepare_explain_statement(
    statement: SqlExplainStatement,
    expected_entity: &'static str,
) -> Result<SqlExplainStatement, SqlLoweringError> {
    let target = match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            SqlExplainTarget::Select(prepare_select_statement(select_statement, expected_entity)?)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            SqlExplainTarget::Delete(prepare_delete_statement(delete_statement, expected_entity)?)
        }
    };

    Ok(SqlExplainStatement {
        mode: statement.mode,
        statement: target,
    })
}

fn prepare_select_statement(
    statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    Ok(normalize_select_statement_to_expected_entity(
        statement,
        expected_entity,
    ))
}

fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> SqlSelectStatement {
    // Re-scope parsed identifiers onto the resolved entity surface after the
    // caller has already established entity ownership for this statement.
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());
    statement.having = normalize_having_clauses(statement.having, entity_scope.as_slice());

    statement
}

fn prepare_delete_statement(
    mut statement: SqlDeleteStatement,
    expected_entity: &'static str,
) -> Result<SqlDeleteStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());

    Ok(statement)
}

#[inline(never)]
fn lower_prepared_statement(
    statement: SqlStatement,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Select(lower_select_shape(statement, primary_key_field)?),
        ))),
        SqlStatement::Delete(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Delete(lower_delete_shape(statement)),
        ))),
        SqlStatement::Explain(statement) => lower_explain_prepared(statement, primary_key_field),
        SqlStatement::Describe(_) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::DescribeEntity)),
        SqlStatement::ShowIndexes(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowIndexesEntity))
        }
        SqlStatement::ShowColumns(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowColumnsEntity))
        }
        SqlStatement::ShowEntities(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowEntities))
        }
    }
}

fn lower_explain_prepared(
    statement: SqlExplainStatement,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    let mode = statement.mode;

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select_prepared(select_statement, mode, primary_key_field)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
                mode,
                query: LoweredSqlQuery::Delete(lower_delete_shape(delete_statement)),
            }))
        }
    }
}

fn lower_explain_select_prepared(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match lower_select_shape(statement.clone(), primary_key_field) {
        Ok(query) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
            mode,
            query: LoweredSqlQuery::Select(query),
        })),
        Err(SqlLoweringError::UnsupportedSelectProjection) => {
            let command = lower_global_aggregate_select_shape(statement)?;

            Ok(LoweredSqlCommand(
                LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command },
            ))
        }
        Err(err) => Err(err),
    }
}

fn lower_global_aggregate_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSqlGlobalAggregateCommand, SqlLoweringError> {
    let SqlSelectStatement {
        projection,
        predicate,
        distinct,
        group_by,
        having,
        order_by,
        limit,
        offset,
        entity: _,
    } = statement;

    if distinct {
        return Err(SqlLoweringError::unsupported_select_distinct());
    }
    if !group_by.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }
    if !having.is_empty() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let terminal = lower_global_aggregate_terminal(projection)?;

    Ok(LoweredSqlGlobalAggregateCommand {
        query: LoweredBaseQueryShape {
            predicate,
            order_by,
            limit,
            offset,
        },
        terminal,
    })
}

///
/// ResolvedHavingClause
///
/// Pre-resolved HAVING clause shape after SQL projection aggregate index
/// resolution. This keeps SQL shape analysis entity-agnostic before typed
/// query binding.
///
#[derive(Clone, Debug)]
enum ResolvedHavingClause {
    GroupField {
        field: String,
        op: crate::db::predicate::CompareOp,
        value: crate::value::Value,
    },
    Aggregate {
        aggregate_index: usize,
        op: crate::db::predicate::CompareOp,
        value: crate::value::Value,
    },
}

///
/// LoweredSelectShape
///
/// Entity-agnostic lowered SQL SELECT shape prepared for typed `Query<E>`
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSelectShape {
    scalar_projection_fields: Option<Vec<String>>,
    grouped_projection_aggregates: Vec<SqlAggregateCall>,
    group_by_fields: Vec<String>,
    distinct: bool,
    having: Vec<ResolvedHavingClause>,
    predicate: Option<Predicate>,
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl LoweredSelectShape {
    // Report whether this lowered select shape carries grouped execution state.
    const fn has_grouping(&self) -> bool {
        !self.group_by_fields.is_empty()
    }
}

///
/// LoweredBaseQueryShape
///
/// Generic-free filter/order/window query modifiers shared by delete and
/// global-aggregate SQL lowering.
/// This keeps common SQL query-shape lowering shared before typed query
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredBaseQueryShape {
    predicate: Option<Predicate>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[inline(never)]
fn lower_select_shape(
    statement: SqlSelectStatement,
    primary_key_field: &str,
) -> Result<LoweredSelectShape, SqlLoweringError> {
    let SqlSelectStatement {
        projection,
        predicate,
        distinct,
        group_by,
        having,
        order_by,
        limit,
        offset,
        entity: _,
    } = statement;
    let projection_for_having = projection.clone();

    // Phase 1: resolve scalar/grouped projection shape.
    let (scalar_projection_fields, grouped_projection_aggregates) = if group_by.is_empty() {
        let scalar_projection_fields =
            lower_scalar_projection_fields(projection, distinct, primary_key_field)?;
        (scalar_projection_fields, Vec::new())
    } else {
        if distinct {
            return Err(SqlLoweringError::unsupported_select_distinct());
        }
        let grouped_projection_aggregates =
            grouped_projection_aggregate_calls(&projection, group_by.as_slice())?;
        (None, grouped_projection_aggregates)
    };

    // Phase 2: resolve HAVING symbols against grouped projection authority.
    let having = lower_having_clauses(
        having,
        &projection_for_having,
        group_by.as_slice(),
        grouped_projection_aggregates.as_slice(),
    )?;

    Ok(LoweredSelectShape {
        scalar_projection_fields,
        grouped_projection_aggregates,
        group_by_fields: group_by,
        distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    })
}

fn lower_scalar_projection_fields(
    projection: SqlProjection,
    distinct: bool,
    primary_key_field: &str,
) -> Result<Option<Vec<String>>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        if distinct {
            return Ok(None);
        }

        return Ok(None);
    };

    let has_aggregate = items
        .iter()
        .any(|item| matches!(item, SqlSelectItem::Aggregate(_)));
    if has_aggregate {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    let fields = items
        .into_iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Ok(field),
            SqlSelectItem::Aggregate(_) | SqlSelectItem::TextFunction(_) => {
                Err(SqlLoweringError::unsupported_select_projection())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    validate_scalar_distinct_projection(distinct, fields.as_slice(), primary_key_field)?;

    Ok(Some(fields))
}

fn validate_scalar_distinct_projection(
    distinct: bool,
    projection_fields: &[String],
    primary_key_field: &str,
) -> Result<(), SqlLoweringError> {
    if !distinct {
        return Ok(());
    }

    if projection_fields.is_empty() {
        return Ok(());
    }

    let has_primary_key_field = projection_fields
        .iter()
        .any(|field| field == primary_key_field);
    if !has_primary_key_field {
        return Err(SqlLoweringError::unsupported_select_distinct());
    }

    Ok(())
}

fn lower_having_clauses(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<Vec<ResolvedHavingClause>, SqlLoweringError> {
    if having_clauses.is_empty() {
        return Ok(Vec::new());
    }
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let projection_aggregates = grouped_projection_aggregate_calls(projection, group_by_fields)
        .map_err(|_| SqlLoweringError::unsupported_select_having())?;
    if projection_aggregates.as_slice() != grouped_projection_aggregates {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let mut lowered = Vec::with_capacity(having_clauses.len());
    for clause in having_clauses {
        match clause.symbol {
            SqlHavingSymbol::Field(field) => lowered.push(ResolvedHavingClause::GroupField {
                field,
                op: clause.op,
                value: clause.value,
            }),
            SqlHavingSymbol::Aggregate(aggregate) => {
                let aggregate_index =
                    resolve_having_aggregate_index(&aggregate, grouped_projection_aggregates)?;
                lowered.push(ResolvedHavingClause::Aggregate {
                    aggregate_index,
                    op: clause.op,
                    value: clause.value,
                });
            }
        }
    }

    Ok(lowered)
}

#[inline(never)]
pub(in crate::db) fn apply_lowered_select_shape(
    mut query: StructuralQuery,
    lowered: LoweredSelectShape,
) -> Result<StructuralQuery, SqlLoweringError> {
    let LoweredSelectShape {
        scalar_projection_fields,
        grouped_projection_aggregates,
        group_by_fields,
        distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    } = lowered;

    // Phase 1: apply grouped declaration semantics.
    for field in group_by_fields {
        query = query.group_by(field)?;
    }

    // Phase 2: apply scalar DISTINCT and projection contracts.
    if distinct {
        query = query.distinct();
    }
    if let Some(fields) = scalar_projection_fields {
        query = query.select_fields(fields);
    }
    for aggregate in grouped_projection_aggregates {
        query = query.aggregate(lower_aggregate_call(aggregate)?);
    }

    // Phase 3: bind resolved HAVING clauses against grouped terminals.
    for clause in having {
        match clause {
            ResolvedHavingClause::GroupField { field, op, value } => {
                query = query.having_group(field, op, value)?;
            }
            ResolvedHavingClause::Aggregate {
                aggregate_index,
                op,
                value,
            } => {
                query = query.having_aggregate(aggregate_index, op, value)?;
            }
        }
    }

    // Phase 4: attach the shared filter/order/page tail through the base-query lane.
    Ok(apply_lowered_base_query_shape(
        query,
        LoweredBaseQueryShape {
            predicate,
            order_by,
            limit,
            offset,
        },
    ))
}

fn apply_lowered_base_query_shape(
    mut query: StructuralQuery,
    lowered: LoweredBaseQueryShape,
) -> StructuralQuery {
    if let Some(predicate) = lowered.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms_structural(query, lowered.order_by);
    if let Some(limit) = lowered.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = lowered.offset {
        query = query.offset(offset);
    }

    query
}

pub(in crate::db) fn bind_lowered_sql_query_structural(
    model: &'static crate::model::entity::EntityModel,
    lowered: LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    match lowered {
        LoweredSqlQuery::Select(select) => {
            apply_lowered_select_shape(StructuralQuery::new(model, consistency), select)
        }
        LoweredSqlQuery::Delete(delete) => Ok(bind_lowered_sql_delete_query_structural(
            model,
            delete,
            consistency,
        )),
    }
}

pub(in crate::db) fn bind_lowered_sql_delete_query_structural(
    model: &'static crate::model::entity::EntityModel,
    delete: LoweredBaseQueryShape,
    consistency: MissingRowPolicy,
) -> StructuralQuery {
    apply_lowered_base_query_shape(StructuralQuery::new(model, consistency).delete(), delete)
}

pub(in crate::db) fn bind_lowered_sql_query<E: EntityKind>(
    lowered: LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let structural = bind_lowered_sql_query_structural(E::MODEL, lowered, consistency)?;

    Ok(Query::from_inner(structural))
}

fn bind_lowered_sql_global_aggregate_command<E: EntityKind>(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> SqlGlobalAggregateCommand<E> {
    SqlGlobalAggregateCommand {
        query: Query::from_inner(apply_lowered_base_query_shape(
            StructuralQuery::new(E::MODEL, consistency),
            lowered.query,
        )),
        terminal: lowered.terminal,
    }
}

fn bind_lowered_sql_global_aggregate_command_structural(
    model: &'static crate::model::entity::EntityModel,
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> SqlGlobalAggregateCommandCore {
    SqlGlobalAggregateCommandCore {
        query: apply_lowered_base_query_shape(
            StructuralQuery::new(model, consistency),
            lowered.query,
        ),
        terminal: lowered.terminal,
    }
}

fn lower_global_aggregate_terminal(
    projection: SqlProjection,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };
    if items.len() != 1 {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    let Some(SqlSelectItem::Aggregate(aggregate)) = items.into_iter().next() else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    match lower_sql_aggregate_shape(aggregate)? {
        LoweredSqlAggregateShape::CountRows => Ok(SqlGlobalAggregateTerminal::CountRows),
        LoweredSqlAggregateShape::CountField(field) => {
            Ok(SqlGlobalAggregateTerminal::CountField(field))
        }
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
        } => Ok(SqlGlobalAggregateTerminal::SumField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
        } => Ok(SqlGlobalAggregateTerminal::AvgField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Min,
            field,
        } => Ok(SqlGlobalAggregateTerminal::MinField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Max,
            field,
        } => Ok(SqlGlobalAggregateTerminal::MaxField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Count,
            ..
        } => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

fn lower_sql_aggregate_shape(
    call: SqlAggregateCall,
) -> Result<LoweredSqlAggregateShape, SqlLoweringError> {
    match (call.kind, call.field) {
        (SqlAggregateKind::Count, None) => Ok(LoweredSqlAggregateShape::CountRows),
        (SqlAggregateKind::Count, Some(field)) => Ok(LoweredSqlAggregateShape::CountField(field)),
        (
            kind @ (SqlAggregateKind::Sum
            | SqlAggregateKind::Avg
            | SqlAggregateKind::Min
            | SqlAggregateKind::Max),
            Some(field),
        ) => Ok(LoweredSqlAggregateShape::FieldTarget { kind, field }),
        _ => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

fn grouped_projection_aggregate_calls(
    projection: &SqlProjection,
    group_by_fields: &[String],
) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_group_by());
    };

    let mut projected_group_fields = Vec::<String>::new();
    let mut aggregate_calls = Vec::<SqlAggregateCall>::new();
    let mut seen_aggregate = false;

    for item in items {
        match item {
            SqlSelectItem::Field(field) => {
                // Keep grouped projection deterministic and mappable to grouped
                // response contracts: group keys must be declared first.
                if seen_aggregate {
                    return Err(SqlLoweringError::unsupported_select_group_by());
                }
                projected_group_fields.push(field.clone());
            }
            SqlSelectItem::Aggregate(aggregate) => {
                seen_aggregate = true;
                aggregate_calls.push(aggregate.clone());
            }
            SqlSelectItem::TextFunction(_) => {
                return Err(SqlLoweringError::unsupported_select_group_by());
            }
        }
    }

    if aggregate_calls.is_empty() || projected_group_fields.as_slice() != group_by_fields {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    Ok(aggregate_calls)
}

fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<crate::db::query::builder::AggregateExpr, SqlLoweringError> {
    match lower_sql_aggregate_shape(call)? {
        LoweredSqlAggregateShape::CountRows => Ok(count()),
        LoweredSqlAggregateShape::CountField(field) => Ok(count_by(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
        } => Ok(sum(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
        } => Ok(avg(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Min,
            field,
        } => Ok(min_by(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Max,
            field,
        } => Ok(max_by(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Count,
            ..
        } => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

fn resolve_having_aggregate_index(
    target: &SqlAggregateCall,
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<usize, SqlLoweringError> {
    let mut matched = grouped_projection_aggregates
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate)| (aggregate == target).then_some(index));
    let Some(index) = matched.next() else {
        return Err(SqlLoweringError::unsupported_select_having());
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(index)
}

fn lower_delete_shape(statement: SqlDeleteStatement) -> LoweredBaseQueryShape {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        entity: _,
    } = statement;

    LoweredBaseQueryShape {
        predicate,
        order_by,
        limit,
        offset: None,
    }
}

fn apply_order_terms_structural(
    mut query: StructuralQuery,
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
) -> StructuralQuery {
    for term in order_by {
        query = match term.direction {
            SqlOrderDirection::Asc => query.order_by(term.field),
            SqlOrderDirection::Desc => query.order_by_desc(term.field),
        };
    }

    query
}

fn normalize_having_clauses(
    clauses: Vec<SqlHavingClause>,
    entity_scope: &[String],
) -> Vec<SqlHavingClause> {
    clauses
        .into_iter()
        .map(|clause| SqlHavingClause {
            symbol: normalize_having_symbol(clause.symbol, entity_scope),
            op: clause.op,
            value: clause.value,
        })
        .collect()
}

fn normalize_having_symbol(symbol: SqlHavingSymbol, entity_scope: &[String]) -> SqlHavingSymbol {
    match symbol {
        SqlHavingSymbol::Field(field) => {
            SqlHavingSymbol::Field(normalize_identifier_to_scope(field, entity_scope))
        }
        SqlHavingSymbol::Aggregate(aggregate) => SqlHavingSymbol::Aggregate(
            normalize_aggregate_call_identifiers(aggregate, entity_scope),
        ),
    }
}

fn normalize_aggregate_call_identifiers(
    aggregate: SqlAggregateCall,
    entity_scope: &[String],
) -> SqlAggregateCall {
    SqlAggregateCall {
        kind: aggregate.kind,
        field: aggregate
            .field
            .map(|field| normalize_identifier_to_scope(field, entity_scope)),
    }
}

// Build one identifier scope used for reducing SQL-qualified field references
// (`entity.field`, `schema.entity.field`) into canonical planner field names.
fn sql_entity_scope_candidates(sql_entity: &str, expected_entity: &'static str) -> Vec<String> {
    let mut out = Vec::new();
    out.push(sql_entity.to_string());
    out.push(expected_entity.to_string());

    if let Some(last) = identifier_last_segment(sql_entity) {
        out.push(last.to_string());
    }
    if let Some(last) = identifier_last_segment(expected_entity) {
        out.push(last.to_string());
    }

    out
}

fn normalize_projection_identifiers(
    projection: SqlProjection,
    entity_scope: &[String],
) -> SqlProjection {
    match projection {
        SqlProjection::All => SqlProjection::All,
        SqlProjection::Items(items) => SqlProjection::Items(
            items
                .into_iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => {
                        SqlSelectItem::Field(normalize_identifier(field, entity_scope))
                    }
                    SqlSelectItem::Aggregate(aggregate) => {
                        SqlSelectItem::Aggregate(SqlAggregateCall {
                            kind: aggregate.kind,
                            field: aggregate
                                .field
                                .map(|field| normalize_identifier(field, entity_scope)),
                        })
                    }
                    SqlSelectItem::TextFunction(SqlTextFunctionCall {
                        function,
                        field,
                        literal,
                        literal2,
                        literal3,
                    }) => SqlSelectItem::TextFunction(SqlTextFunctionCall {
                        function,
                        field: normalize_identifier(field, entity_scope),
                        literal,
                        literal2,
                        literal3,
                    }),
                })
                .collect(),
        ),
    }
}

fn normalize_order_terms(
    terms: Vec<crate::db::sql::parser::SqlOrderTerm>,
    entity_scope: &[String],
) -> Vec<crate::db::sql::parser::SqlOrderTerm> {
    terms
        .into_iter()
        .map(|term| crate::db::sql::parser::SqlOrderTerm {
            field: normalize_order_term_identifier(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
}

fn normalize_order_term_identifier(identifier: String, entity_scope: &[String]) -> String {
    let Some(expression) = ExpressionOrderTerm::parse(identifier.as_str()) else {
        return normalize_identifier(identifier, entity_scope);
    };
    let normalized_field = normalize_identifier(expression.field().to_string(), entity_scope);

    expression.canonical_text_with_field(normalized_field.as_str())
}

fn normalize_identifier_list(fields: Vec<String>, entity_scope: &[String]) -> Vec<String> {
    fields
        .into_iter()
        .map(|field| normalize_identifier(field, entity_scope))
        .collect()
}

// SQL lowering only adapts identifier qualification (`entity.field` -> `field`)
// and delegates predicate-tree traversal ownership to `db::predicate`.
fn adapt_predicate_identifiers_to_scope(
    predicate: Predicate,
    entity_scope: &[String],
) -> Predicate {
    rewrite_field_identifiers(predicate, |field| normalize_identifier(field, entity_scope))
}

fn normalize_identifier(identifier: String, entity_scope: &[String]) -> String {
    normalize_identifier_to_scope(identifier, entity_scope)
}

fn ensure_entity_matches_expected(
    sql_entity: &str,
    expected_entity: &'static str,
) -> Result<(), SqlLoweringError> {
    if identifiers_tail_match(sql_entity, expected_entity) {
        return Ok(());
    }

    Err(SqlLoweringError::entity_mismatch(
        sql_entity,
        expected_entity,
    ))
}
