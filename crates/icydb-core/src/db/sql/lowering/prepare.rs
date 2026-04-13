use crate::db::QueryError;
use crate::db::sql::lowering::{
    LoweredSqlCommand, LoweredSqlCommandInner, LoweredSqlQuery, PreparedSqlStatement,
    SqlLoweringError,
    aggregate::lower_global_aggregate_select_shape,
    normalize::{
        adapt_predicate_identifiers_to_scope, ensure_entity_matches_expected,
        normalize_order_terms, normalize_select_statement_to_expected_entity,
        sql_entity_scope_candidates,
    },
    select::{lower_delete_shape, lower_select_shape},
};
use crate::db::sql::parser::{
    SqlDeleteStatement, SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlInsertSource,
    SqlInsertStatement, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement,
};

/// Prepare one parsed SQL statement for one expected entity route.
#[inline(never)]
pub(crate) fn prepare_sql_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<PreparedSqlStatement, SqlLoweringError> {
    let statement = prepare_statement(statement, expected_entity)?;

    Ok(PreparedSqlStatement { statement })
}

/// Lower one prepared SQL statement into one shared generic-free command shape.
#[inline(never)]
pub(crate) fn lower_sql_command_from_prepared_statement(
    prepared: PreparedSqlStatement,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    lower_prepared_statement(prepared.statement, primary_key_field)
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
        SqlStatement::Insert(statement) => Ok(SqlStatement::Insert(prepare_insert_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Update(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::Update(statement))
        }
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

    normalize_select_statement_to_expected_entity(statement, expected_entity)
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

fn prepare_insert_statement(
    mut statement: SqlInsertStatement,
    expected_entity: &'static str,
) -> Result<SqlInsertStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    if let SqlInsertSource::Select(select) = statement.source {
        statement.source = SqlInsertSource::Select(Box::new(prepare_insert_select_source(
            *select,
            expected_entity,
        )?));
    }

    Ok(statement)
}

fn prepare_insert_select_source(
    statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    let statement = prepare_select_statement(statement, expected_entity)?;

    if !statement.group_by.is_empty() || !statement.having.is_empty() {
        return Err(QueryError::unsupported_query(
            "SQL INSERT SELECT requires scalar SELECT source in this release",
        )
        .into());
    }

    if let SqlProjection::Items(items) = &statement.projection {
        for item in items {
            if matches!(item, SqlSelectItem::Aggregate(_)) {
                return Err(QueryError::unsupported_query(
                    "SQL INSERT SELECT does not support aggregate source projection in this release",
                )
                .into());
            }
        }
    }

    Ok(statement)
}

#[inline(never)]
fn lower_prepared_statement(
    statement: SqlStatement,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Select(lower_select_shape(statement)?),
        ))),
        SqlStatement::Delete(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Delete(lower_delete_shape(statement)),
        ))),
        SqlStatement::Insert(_) | SqlStatement::Update(_) => {
            Err(SqlLoweringError::unexpected_query_lane_statement())
        }
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
    _primary_key_field: &str,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match lower_select_shape(statement.clone()) {
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
