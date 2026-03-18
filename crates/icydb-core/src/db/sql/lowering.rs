//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate, rewrite_field_identifiers},
        query::{
            builder::aggregate::{avg, count, count_by, max_by, min_by, sum},
            intent::{Query, QueryError},
        },
        sql::identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlExplainMode,
            SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol,
            SqlOrderDirection, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement,
            parse_sql,
        },
    },
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// SqlCommand
///
/// Lowered SQL command for one entity-typed query surface.
///
/// `Query` contains executable load/delete intent.
/// `Explain` wraps one executable query intent plus requested explain mode.
/// `ExplainGlobalAggregate` wraps one constrained global aggregate SQL command
/// plus requested explain mode.
///

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
        "unsupported SQL SELECT projection in this release; executable forms are SELECT *, direct field lists, or constrained grouped aggregate projection shapes"
    )]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT in this release")]
    UnsupportedSelectDistinct,

    #[error("unsupported SQL GROUP BY projection shape in this release")]
    UnsupportedSelectGroupBy,

    #[error("unsupported SQL HAVING shape in this release")]
    UnsupportedSelectHaving,
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

/// Parse and lower one SQL statement into canonical query intent for `E`.
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = parse_sql(sql)?;
    compile_sql_command_from_statement::<E>(statement, consistency)
}

/// Lower one parsed SQL statement into canonical query intent for `E`.
pub(crate) fn compile_sql_command_from_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let prepared = prepare_sql_statement(statement, E::MODEL.entity_name())?;
    compile_sql_command_from_prepared_statement::<E>(prepared, consistency)
}

/// Lower one prepared SQL statement into canonical query intent for `E`.
pub(crate) fn compile_sql_command_from_prepared_statement<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    lower_prepared_statement::<E>(prepared.statement, consistency)
}

/// Prepare one parsed SQL statement for one expected entity route.
pub(crate) fn prepare_sql_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<PreparedSqlStatement, SqlLoweringError> {
    let statement = prepare_statement(statement, expected_entity)?;

    Ok(PreparedSqlStatement { statement })
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
pub(crate) fn compile_sql_global_aggregate_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = parse_sql(sql)?;
    let prepared = prepare_sql_statement(statement, E::MODEL.entity_name())?;
    compile_sql_global_aggregate_command_from_prepared::<E>(prepared, consistency)
}

fn compile_sql_global_aggregate_command_from_prepared<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };

    lower_global_aggregate_select_prepared::<E>(statement, consistency)
}

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
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());
    statement.having = normalize_having_clauses(statement.having, entity_scope.as_slice());

    Ok(statement)
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

fn lower_prepared_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(SqlCommand::Query(lower_select_prepared::<E>(
            statement,
            consistency,
        )?)),
        SqlStatement::Delete(statement) => Ok(SqlCommand::Query(lower_delete_prepared::<E>(
            statement,
            consistency,
        ))),
        SqlStatement::Explain(statement) => lower_explain_prepared::<E>(statement, consistency),
        SqlStatement::Describe(_) => Ok(SqlCommand::DescribeEntity),
        SqlStatement::ShowIndexes(_) => Ok(SqlCommand::ShowIndexesEntity),
        SqlStatement::ShowColumns(_) => Ok(SqlCommand::ShowColumnsEntity),
        SqlStatement::ShowEntities(_) => Ok(SqlCommand::ShowEntities),
    }
}

fn lower_explain_prepared<E: EntityKind>(
    statement: SqlExplainStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let mode = statement.mode;

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select_prepared::<E>(select_statement, mode, consistency)
        }
        SqlExplainTarget::Delete(delete_statement) => Ok(SqlCommand::Explain {
            mode,
            query: lower_delete_prepared::<E>(delete_statement, consistency),
        }),
    }
}

fn lower_explain_select_prepared<E: EntityKind>(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match lower_select_prepared::<E>(statement.clone(), consistency) {
        Ok(query) => Ok(SqlCommand::Explain { mode, query }),
        Err(SqlLoweringError::UnsupportedSelectProjection) => {
            let command = lower_global_aggregate_select_prepared::<E>(statement, consistency)?;

            Ok(SqlCommand::ExplainGlobalAggregate { mode, command })
        }
        Err(err) => Err(err),
    }
}

fn lower_global_aggregate_select_prepared<E: EntityKind>(
    statement: SqlSelectStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
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
        return Err(SqlLoweringError::UnsupportedSelectDistinct);
    }
    if !group_by.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }
    if !having.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectHaving);
    }

    let terminal = lower_global_aggregate_terminal(projection)?;

    // Phase 1: lower base scalar query shape for terminal execution.
    let mut query = Query::new(consistency);
    if let Some(predicate) = predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, order_by);

    // Phase 2: preserve effective window semantics for aggregate terminals.
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    if let Some(offset) = offset {
        query = query.offset(offset);
    }

    Ok(SqlGlobalAggregateCommand { query, terminal })
}

fn lower_select_prepared<E: EntityKind>(
    statement: SqlSelectStatement,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let lowered = lower_select_shape(statement, E::MODEL.primary_key.name)?;

    apply_lowered_select_shape(Query::new(consistency), lowered)
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
struct LoweredSelectShape {
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
            return Err(SqlLoweringError::UnsupportedSelectDistinct);
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
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    }

    let fields = items
        .into_iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Ok(field),
            SqlSelectItem::Aggregate(_) => Err(SqlLoweringError::UnsupportedSelectProjection),
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
        return Err(SqlLoweringError::UnsupportedSelectDistinct);
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
        return Err(SqlLoweringError::UnsupportedSelectHaving);
    }

    let projection_aggregates = grouped_projection_aggregate_calls(projection, group_by_fields)
        .map_err(|_| SqlLoweringError::UnsupportedSelectHaving)?;
    if projection_aggregates.as_slice() != grouped_projection_aggregates {
        return Err(SqlLoweringError::UnsupportedSelectHaving);
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

fn apply_lowered_select_shape<E: EntityKind>(
    mut query: Query<E>,
    lowered: LoweredSelectShape,
) -> Result<Query<E>, SqlLoweringError> {
    // Phase 1: apply grouped declaration semantics.
    for field in lowered.group_by_fields {
        query = query.group_by(field)?;
    }

    // Phase 2: apply scalar DISTINCT and projection contracts.
    if lowered.distinct {
        query = query.distinct();
    }
    if let Some(fields) = lowered.scalar_projection_fields {
        query = query.select_fields(fields);
    }
    for aggregate in lowered.grouped_projection_aggregates {
        query = query.aggregate(lower_aggregate_call(aggregate)?);
    }

    // Phase 3: bind resolved HAVING clauses against grouped terminals.
    for clause in lowered.having {
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

    // Phase 4: attach filter/order/page semantics.
    if let Some(predicate) = lowered.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, lowered.order_by);
    if let Some(limit) = lowered.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = lowered.offset {
        query = query.offset(offset);
    }

    Ok(query)
}

fn lower_global_aggregate_terminal(
    projection: SqlProjection,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };
    if items.len() != 1 {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    }

    let Some(SqlSelectItem::Aggregate(aggregate)) = items.into_iter().next() else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };

    match (aggregate.kind, aggregate.field) {
        (SqlAggregateKind::Count, None) => Ok(SqlGlobalAggregateTerminal::CountRows),
        (SqlAggregateKind::Count, Some(field)) => Ok(SqlGlobalAggregateTerminal::CountField(field)),
        (SqlAggregateKind::Sum, Some(field)) => Ok(SqlGlobalAggregateTerminal::SumField(field)),
        (SqlAggregateKind::Avg, Some(field)) => Ok(SqlGlobalAggregateTerminal::AvgField(field)),
        (SqlAggregateKind::Min, Some(field)) => Ok(SqlGlobalAggregateTerminal::MinField(field)),
        (SqlAggregateKind::Max, Some(field)) => Ok(SqlGlobalAggregateTerminal::MaxField(field)),
        _ => Err(SqlLoweringError::UnsupportedSelectProjection),
    }
}

fn grouped_projection_aggregate_calls(
    projection: &SqlProjection,
    group_by_fields: &[String],
) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
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
                    return Err(SqlLoweringError::UnsupportedSelectGroupBy);
                }
                projected_group_fields.push(field.clone());
            }
            SqlSelectItem::Aggregate(aggregate) => {
                seen_aggregate = true;
                aggregate_calls.push(aggregate.clone());
            }
        }
    }

    if aggregate_calls.is_empty() || projected_group_fields.as_slice() != group_by_fields {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }

    Ok(aggregate_calls)
}

fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<crate::db::query::builder::AggregateExpr, SqlLoweringError> {
    match (call.kind, call.field) {
        (SqlAggregateKind::Count, None) => Ok(count()),
        (SqlAggregateKind::Count, Some(field)) => Ok(count_by(field)),
        (SqlAggregateKind::Sum, Some(field)) => Ok(sum(field)),
        (SqlAggregateKind::Avg, Some(field)) => Ok(avg(field)),
        (SqlAggregateKind::Min, Some(field)) => Ok(min_by(field)),
        (SqlAggregateKind::Max, Some(field)) => Ok(max_by(field)),
        _ => Err(SqlLoweringError::UnsupportedSelectProjection),
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
        return Err(SqlLoweringError::UnsupportedSelectHaving);
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::UnsupportedSelectHaving);
    }

    Ok(index)
}

fn lower_delete_prepared<E: EntityKind>(
    statement: SqlDeleteStatement,
    consistency: MissingRowPolicy,
) -> Query<E> {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        entity: _,
    } = statement;

    let mut query = Query::new(consistency).delete();
    if let Some(predicate) = predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, order_by);
    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query
}

fn apply_order_terms<E: EntityKind>(
    mut query: Query<E>,
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
) -> Query<E> {
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
            field: normalize_identifier(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
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

    Err(SqlLoweringError::EntityMismatch {
        sql_entity: sql_entity.to_string(),
        expected_entity,
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::ExecutablePlan,
            predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
            query::intent::Query,
            query::plan::{
                QueryMode,
                expr::{Expr, ProjectionField},
            },
            sql::{
                lowering::{
                    SqlCommand, SqlGlobalAggregateTerminal, SqlLoweringError, compile_sql_command,
                    compile_sql_global_aggregate_command,
                },
                parser::{SqlExplainMode, SqlParseError},
            },
        },
        model::field::FieldKind,
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct SqlLowerEntity {
        id: Ulid,
        name: String,
        age: u64,
    }

    crate::test_canister! {
        ident = SqlLowerCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = SqlLowerDataStore,
        canister = SqlLowerCanister,
    }

    crate::test_entity_schema! {
        ident = SqlLowerEntity,
        id = Ulid,
        entity_name = "SqlLowerEntity",
    entity_tag = crate::testing::SQL_LOWER_ENTITY_TAG,
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text),
            ("age", FieldKind::Uint),
        ],
        indexes = [],
        store = SqlLowerDataStore,
        canister = SqlLowerCanister,
    }

    #[test]
    fn compile_sql_command_select_star_lowers_to_load_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 10 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT * should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_select_distinct_star_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT * FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT DISTINCT * should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "SELECT DISTINCT * should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_select_distinct_with_pk_field_list_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT id, age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT DISTINCT with PK-projected field list should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "SELECT DISTINCT field-list including PK should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_rejects_select_distinct_without_pk_projection() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
    }

    #[test]
    fn compile_sql_command_delete_lowers_to_delete_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "DELETE FROM SqlLowerEntity WHERE age < 18 ORDER BY age LIMIT 3",
            MissingRowPolicy::Ignore,
        )
        .expect("DELETE should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Delete(_)));
    }

    #[test]
    fn compile_sql_command_describe_lowers_to_describe_entity_lane() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "DESCRIBE public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("DESCRIBE should lower");

        assert!(
            matches!(command, SqlCommand::DescribeEntity),
            "DESCRIBE should lower to dedicated describe command lane",
        );
    }

    #[test]
    fn compile_sql_command_describe_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "DESCRIBE DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("DESCRIBE entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_command_show_indexes_lowers_to_show_indexes_lane() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SHOW INDEXES public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SHOW INDEXES should lower");

        assert!(
            matches!(command, SqlCommand::ShowIndexesEntity),
            "SHOW INDEXES should lower to dedicated show-indexes command lane",
        );
    }

    #[test]
    fn compile_sql_command_show_indexes_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SHOW INDEXES DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("SHOW INDEXES entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_command_show_columns_lowers_to_show_columns_lane() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SHOW COLUMNS public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SHOW COLUMNS should lower");

        assert!(
            matches!(command, SqlCommand::ShowColumnsEntity),
            "SHOW COLUMNS should lower to dedicated show-columns command lane",
        );
    }

    #[test]
    fn compile_sql_command_show_columns_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SHOW COLUMNS DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("SHOW COLUMNS entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_command_show_entities_lowers_to_show_entities_lane() {
        let command =
            compile_sql_command::<SqlLowerEntity>("SHOW ENTITIES", MissingRowPolicy::Ignore)
                .expect("SHOW ENTITIES should lower");

        assert!(
            matches!(command, SqlCommand::ShowEntities),
            "SHOW ENTITIES should lower to dedicated show-entities command lane",
        );
    }

    #[test]
    fn compile_sql_command_show_tables_lowers_to_show_entities_lane() {
        let command =
            compile_sql_command::<SqlLowerEntity>("SHOW TABLES", MissingRowPolicy::Ignore)
                .expect("SHOW TABLES should lower");

        assert!(
            matches!(command, SqlCommand::ShowEntities),
            "SHOW TABLES should lower to dedicated show-entities command lane",
        );
    }

    #[test]
    fn compile_sql_command_explain_execution_wraps_lowered_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN EXECUTION SELECT * FROM SqlLowerEntity LIMIT 1",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN EXECUTION should lower");

        let SqlCommand::Explain { mode, query } = command else {
            panic!("expected lowered explain command");
        };

        assert_eq!(mode, SqlExplainMode::Execution);
        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_explain_select_distinct_star_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT DISTINCT * FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN SELECT DISTINCT * should lower");

        let SqlCommand::Explain { mode, query } = command else {
            panic!("expected lowered explain command");
        };
        assert_eq!(mode, SqlExplainMode::Plan);
        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "EXPLAIN SELECT DISTINCT * should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_explain_select_distinct_without_pk_projection_rejects() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT DISTINCT age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("EXPLAIN SELECT DISTINCT without PK projection should fail closed");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
    }

    #[test]
    fn compile_sql_command_explain_global_aggregate_lowers_to_dedicated_command() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN global aggregate SQL should lower");

        let SqlCommand::ExplainGlobalAggregate { mode, command } = command else {
            panic!("expected lowered explain global aggregate command");
        };

        assert_eq!(mode, SqlExplainMode::Plan);
        assert!(
            matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
            "global aggregate EXPLAIN should preserve aggregate terminal lowering",
        );
    }

    #[test]
    fn compile_sql_command_select_field_projection_lowers_to_scalar_field_selection() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("field-list projection should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        let projection = query
            .plan()
            .expect("field-list plan should build")
            .projection_spec();
        let field_names = projection
            .fields()
            .map(|field| match field {
                ProjectionField::Scalar {
                    expr: Expr::Field(field),
                    alias: None,
                } => field.as_str().to_string(),
                other @ ProjectionField::Scalar { .. } => {
                    panic!(
                        "scalar field-list projection should lower to plain field exprs: {other:?}"
                    )
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
    }

    #[test]
    fn compile_sql_command_select_table_qualified_fields_parity_matches_unqualified_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT SqlLowerEntity.name, SqlLowerEntity.age \
             FROM SqlLowerEntity \
             WHERE SqlLowerEntity.age >= 21 \
             ORDER BY SqlLowerEntity.age DESC LIMIT 5 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified field-list SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .select_fields(["name", "age"])
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(5)
            .offset(1);

        assert_eq!(
            sql_query
                .plan()
                .expect("qualified SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("unqualified fluent plan should build")
                .into_inner(),
            "qualified SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
        );
    }

    #[test]
    fn compile_sql_command_qualified_nested_predicate_matches_unqualified_fluent_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity \
             WHERE (SqlLowerEntity.age >= 21 OR SqlLowerEntity.name = 'Ada') \
             AND NOT (SqlLowerEntity.name = 'Bob')",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified nested-predicate SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query =
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::And(vec![
                Predicate::Or(vec![
                    Predicate::Compare(ComparePredicate::with_coercion(
                        "age",
                        CompareOp::Gte,
                        Value::Int(21),
                        CoercionId::NumericWiden,
                    )),
                    Predicate::Compare(ComparePredicate::eq(
                        "name".to_string(),
                        Value::Text("Ada".to_string()),
                    )),
                ]),
                Predicate::Not(Box::new(Predicate::Compare(ComparePredicate::eq(
                    "name".to_string(),
                    Value::Text("Bob".to_string()),
                )))),
            ]));

        assert_eq!(
            sql_query
                .plan()
                .expect("qualified nested-predicate SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("unqualified fluent nested-predicate plan should build")
                .into_inner(),
            "qualified nested predicate identifiers should normalize to the same canonical planned intent as unqualified fluent predicates",
        );
    }

    #[test]
    fn compile_sql_command_lower_like_prefix_parity_matches_casefold_starts_with_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity WHERE LOWER(name) LIKE 'Al%'",
            MissingRowPolicy::Ignore,
        )
        .expect("LOWER(field) LIKE prefix SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            )),
        );

        assert_eq!(
            sql_query
                .plan()
                .expect("LOWER(field) LIKE SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("fluent text-casefold starts-with plan should build")
                .into_inner(),
            "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
        );
    }

    #[test]
    fn compile_sql_command_lower_like_non_prefix_pattern_rejects() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity WHERE LOWER(name) LIKE '%Al'",
            MissingRowPolicy::Ignore,
        )
        .expect_err("LOWER(field) LIKE non-prefix pattern should fail closed");

        assert!(matches!(
            err,
            SqlLoweringError::Parse(SqlParseError::UnsupportedFeature {
                feature: "LOWER(field) LIKE patterns beyond trailing '%' prefix form"
            })
        ));
    }

    #[test]
    fn compile_sql_command_select_schema_qualified_entity_lowers_to_load_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("schema-qualified entity SQL should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_rejects_global_aggregate_select_projection_in_current_slice() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("global aggregate projection should remain gated in this slice");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
    }

    #[test]
    fn compile_sql_command_rejects_mixed_scalar_and_aggregate_projection_in_current_slice() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("mixed scalar+aggregate projection should remain gated in this slice");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
    }

    #[test]
    fn compile_sql_command_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped aggregate projection should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };
        assert!(
            query.has_grouping(),
            "grouped aggregate SQL lowering should produce grouped query intent",
        );
    }

    #[test]
    fn compile_sql_command_select_grouped_qualified_identifiers_match_unqualified_intent() {
        let qualified_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT SqlLowerEntity.age, COUNT(*) \
             FROM public.SqlLowerEntity \
             WHERE SqlLowerEntity.age >= 21 \
             GROUP BY SqlLowerEntity.age \
             ORDER BY SqlLowerEntity.age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified grouped SQL query should lower");
        let unqualified_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("unqualified grouped SQL query should lower");

        let (SqlCommand::Query(qualified_query), SqlCommand::Query(unqualified_query)) =
            (qualified_command, unqualified_command)
        else {
            panic!("expected lowered grouped query commands");
        };

        assert_eq!(
            qualified_query
                .plan()
                .expect("qualified grouped SQL plan should build")
                .into_inner(),
            unqualified_query
                .plan()
                .expect("unqualified grouped SQL plan should build")
                .into_inner(),
            "qualified grouped SQL identifiers should normalize to the same canonical planned intent as unqualified grouped SQL",
        );
    }

    #[test]
    fn compile_sql_command_select_grouped_having_parity_matches_fluent_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             GROUP BY age \
             HAVING age >= 21 AND COUNT(*) > 1 \
             ORDER BY age DESC LIMIT 3",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped HAVING SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered grouped HAVING SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .group_by("age")
            .expect("fluent grouped query should accept grouped field")
            .aggregate(crate::db::count())
            .having_group("age", CompareOp::Gte, Value::Int(21))
            .expect("fluent grouped HAVING group-field clause should be accepted")
            .having_aggregate(0, CompareOp::Gt, Value::Int(1))
            .expect("fluent grouped HAVING aggregate clause should be accepted")
            .order_by_desc("age")
            .limit(3);

        assert_eq!(
            sql_query
                .plan()
                .expect("grouped HAVING SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("fluent grouped HAVING plan should build")
                .into_inner(),
            "grouped HAVING SQL lowering and fluent grouped HAVING query must produce identical normalized planned intent",
        );
    }

    #[test]
    fn compile_sql_command_select_grouped_having_is_null_parity_matches_fluent_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             GROUP BY age \
             HAVING age IS NOT NULL AND COUNT(*) IS NOT NULL \
             ORDER BY age DESC LIMIT 3",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped HAVING IS [NOT] NULL SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered grouped HAVING IS [NOT] NULL SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .group_by("age")
            .expect("fluent grouped query should accept grouped field")
            .aggregate(crate::db::count())
            .having_group("age", CompareOp::Ne, Value::Null)
            .expect("fluent grouped HAVING group-field IS NOT NULL should be accepted")
            .having_aggregate(0, CompareOp::Ne, Value::Null)
            .expect("fluent grouped HAVING aggregate IS NOT NULL should be accepted")
            .order_by_desc("age")
            .limit(3);

        assert_eq!(
            sql_query
                .plan()
                .expect("grouped HAVING IS [NOT] NULL SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("fluent grouped HAVING IS [NOT] NULL plan should build")
                .into_inner(),
            "grouped HAVING IS [NOT] NULL SQL lowering and fluent grouped HAVING query must produce identical normalized planned intent",
        );
    }

    #[test]
    fn compile_sql_command_select_having_without_group_by_rejects() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity HAVING COUNT(*) > 1",
            MissingRowPolicy::Ignore,
        )
        .expect_err("HAVING without GROUP BY should fail closed");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectHaving));
    }

    #[test]
    fn compile_sql_command_select_grouped_aggregate_parity_matches_query_and_executable_identity() {
        // Phase 1: lower equivalent grouped SQL and fluent grouped intents.
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 3 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped aggregate SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered grouped SQL query command");
        };
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .group_by("age")
            .expect("fluent grouped query should accept grouped field")
            .aggregate(crate::db::count())
            .order_by_desc("age")
            .limit(3)
            .offset(1);

        // Phase 2: assert canonical planned identity + fingerprint parity.
        let sql_compiled = sql_query.plan().expect("grouped SQL plan should build");
        let fluent_compiled = fluent_query
            .plan()
            .expect("fluent grouped plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "grouped SQL lowering and fluent grouped query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_query
                .plan_hash_hex()
                .expect("grouped SQL plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent grouped plan hash should build"),
            "equivalent grouped SQL and fluent grouped queries must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable =
            ExecutablePlan::from(sql_query.plan().expect("grouped SQL executable plan"));
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent grouped executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("grouped SQL execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent grouped execution strategy"),
            "equivalent grouped SQL and fluent grouped queries must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("grouped SQL execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent grouped execution ordering"),
            "equivalent grouped SQL and fluent grouped queries must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_command_select_field_projection_parity_matches_query_and_executable_identity() {
        // Phase 1: lower equivalent SQL and fluent field-list intents.
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, age FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 5 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("field-list SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .select_fields(["name", "age"])
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(5)
            .offset(1);

        // Phase 2: assert canonical planned identity + fingerprint parity.
        let sql_compiled = sql_query.plan().expect("SQL plan should build");
        let fluent_compiled = fluent_query.plan().expect("fluent plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "SQL field-list lowering and fluent field-list query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_query
                .plan_hash_hex()
                .expect("SQL field-list plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent field-list plan hash should build"),
            "equivalent SQL and fluent field-list projections must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable = ExecutablePlan::from(sql_query.plan().expect("SQL executable plan"));
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("SQL execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent execution strategy"),
            "equivalent SQL and fluent field-list projections must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("SQL execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent execution ordering"),
            "equivalent SQL and fluent field-list projections must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_command_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_global_aggregate_command_count_star_lowers() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT COUNT(*) FROM SqlLowerEntity WHERE age >= 21",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate count SQL should lower");

        assert!(
            matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
            "COUNT(*) should lower to global count terminal",
        );
        assert!(
            !command.query().has_grouping(),
            "global aggregate SQL command should lower to scalar base query shape",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_count_sum_avg_min_max_lower() {
        let count_by_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT COUNT(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("COUNT(field) SQL should lower");
        let sum_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SUM(field) SQL should lower");
        let avg_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT AVG(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("AVG(field) SQL should lower");
        let min_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT MIN(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("MIN(field) SQL should lower");
        let max_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT MAX(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("MAX(field) SQL should lower");

        assert!(
            matches!(
                count_by_command.terminal(),
                SqlGlobalAggregateTerminal::CountField(field) if field == "age"
            ),
            "COUNT(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                sum_command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "SUM(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                avg_command.terminal(),
                SqlGlobalAggregateTerminal::AvgField(field) if field == "age"
            ),
            "AVG(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                min_command.terminal(),
                SqlGlobalAggregateTerminal::MinField(field) if field == "age"
            ),
            "MIN(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                max_command.terminal(),
                SqlGlobalAggregateTerminal::MaxField(field) if field == "age"
            ),
            "MAX(field) should preserve field target in lowered terminal",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_qualified_field_lowers_to_unqualified_terminal() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(SqlLowerEntity.age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified global aggregate field SQL should lower");

        assert!(
            matches!(
                command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "qualified aggregate target fields should normalize to canonical unqualified field names",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_preserves_base_query_window_semantics() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate SQL command should lower");
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(2)
            .offset(1);

        assert_eq!(
            command
                .query()
                .plan()
                .expect("SQL global aggregate base query plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("fluent base query plan should build")
                .into_inner(),
            "global aggregate SQL lowering should preserve scalar base query predicate/order/window semantics",
        );
        assert_eq!(
            command
                .query()
                .plan_hash_hex()
                .expect("SQL global aggregate base query plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent base query plan hash should build"),
            "global aggregate SQL lowering should preserve deterministic base query fingerprint semantics",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_parity_matches_fluent_query_and_executable_identity() {
        // Phase 1: lower equivalent global aggregate SQL and fluent scalar base query intent.
        let sql_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 3 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate SQL should lower");
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(3)
            .offset(1);

        // Phase 2: assert aggregate-terminal contract and canonical planned identity + fingerprint parity.
        assert!(
            matches!(
                sql_command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "global aggregate SQL SUM terminal should preserve canonical target field",
        );
        let sql_compiled = sql_command
            .query()
            .plan()
            .expect("global aggregate SQL base query plan should build");
        let fluent_compiled = fluent_query
            .plan()
            .expect("fluent scalar base query plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "global aggregate SQL base query lowering and fluent scalar query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_command
                .query()
                .plan_hash_hex()
                .expect("global aggregate SQL base query plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent scalar base query plan hash should build"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable = ExecutablePlan::from(
            sql_command
                .query()
                .plan()
                .expect("global aggregate SQL base executable plan"),
        );
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent scalar executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("global aggregate SQL base execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent scalar execution strategy"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("global aggregate SQL base execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent scalar execution ordering"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_rejects_unsupported_shapes() {
        for sql in [
            "SELECT age FROM SqlLowerEntity",
            "SELECT COUNT(*), SUM(age) FROM SqlLowerEntity",
            "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        ] {
            let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
                sql,
                MissingRowPolicy::Ignore,
            )
            .expect_err("unsupported global aggregate SQL shape should fail closed");

            assert!(
                matches!(
                    err,
                    SqlLoweringError::UnsupportedSelectProjection
                        | SqlLoweringError::UnsupportedSelectGroupBy
                ),
                "unsupported global aggregate SQL shape should remain lowering-gated: {sql}",
            );
        }
    }
}
