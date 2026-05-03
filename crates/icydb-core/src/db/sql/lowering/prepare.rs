use crate::{
    db::{
        MissingRowPolicy, QueryError,
        query::intent::StructuralQuery,
        schema::SchemaInfo,
        sql::{
            lowering::{
                LoweredDeleteShape, LoweredSelectShape, LoweredSqlCommand, LoweredSqlCommandInner,
                LoweredSqlQuery, PreparedSqlStatement, SqlLoweringError,
                aggregate::lower_global_aggregate_select_shape,
                bind_lowered_sql_select_query_structural_with_schema,
                normalize::{
                    ensure_entity_matches_expected, normalize_delete_statement_to_expected_entity,
                    normalize_select_statement_to_expected_entity,
                    normalize_update_statement_to_expected_entity,
                },
                select::{lower_delete_shape, lower_delete_statement_shape, lower_select_shape},
            },
            parser::{
                SqlAggregateCall, SqlDeleteStatement, SqlExplainMode, SqlExplainStatement,
                SqlExplainTarget, SqlExpr, SqlInsertSource, SqlInsertStatement, SqlOrderTerm,
                SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement, SqlUpdateStatement,
            },
        },
    },
    model::entity::EntityModel,
};

/// Prepare one parsed SQL statement for one expected entity route.
#[inline(never)]
pub(crate) fn prepare_sql_statement(
    statement: &SqlStatement,
    expected_entity: &'static str,
) -> Result<PreparedSqlStatement, SqlLoweringError> {
    let statement = prepare_statement(statement, expected_entity)?;
    validate_prepared_statement_parameters(&statement)?;

    Ok(PreparedSqlStatement { statement })
}

// Reject placeholders at the prepared-input boundary until a real binding
// contract exists. This keeps future parameter support focused on replacing
// this owner instead of hunting for lower-level expression failures.
fn validate_prepared_statement_parameters(
    statement: &SqlStatement,
) -> Result<(), SqlLoweringError> {
    let Some(index) = first_statement_parameter_index(statement) else {
        return Ok(());
    };

    Err(SqlLoweringError::unsupported_parameter_placement(
        Some(index),
        "SQL parameter binding is not supported in this release",
    ))
}

// Find the first placeholder index in one normalized statement so prepare can
// report unsupported parameters before lowering builds execution artifacts.
fn first_statement_parameter_index(statement: &SqlStatement) -> Option<usize> {
    match statement {
        SqlStatement::Select(statement) => first_select_parameter_index(statement),
        SqlStatement::Delete(statement) => first_delete_parameter_index(statement),
        SqlStatement::Insert(statement) => first_insert_parameter_index(statement),
        SqlStatement::Update(statement) => first_update_parameter_index(statement),
        SqlStatement::Explain(statement) => first_explain_parameter_index(statement),
        SqlStatement::Describe(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowEntities(_) => None,
    }
}

// Scan one SELECT statement in clause order so unsupported parameter errors
// point at the first placeholder a user wrote in the executable surface.
fn first_select_parameter_index(statement: &SqlSelectStatement) -> Option<usize> {
    first_projection_parameter_index(&statement.projection)
        .or_else(|| {
            statement
                .predicate
                .as_ref()
                .and_then(first_expr_parameter_index)
        })
        .or_else(|| statement.having.iter().find_map(first_expr_parameter_index))
        .or_else(|| first_order_terms_parameter_index(statement.order_by.as_slice()))
}

// Scan one DELETE statement for unsupported placeholders in executable
// predicate and ordering clauses.
fn first_delete_parameter_index(statement: &SqlDeleteStatement) -> Option<usize> {
    statement
        .predicate
        .as_ref()
        .and_then(first_expr_parameter_index)
        .or_else(|| first_order_terms_parameter_index(statement.order_by.as_slice()))
}

// INSERT only admits placeholders through its prepared SELECT source today;
// literal VALUES are already parsed as concrete runtime values.
fn first_insert_parameter_index(statement: &SqlInsertStatement) -> Option<usize> {
    match &statement.source {
        SqlInsertSource::Values(_) => None,
        SqlInsertSource::Select(select) => first_select_parameter_index(select),
    }
}

// Scan one UPDATE statement for unsupported placeholders in its predicate and
// ordering clauses. SET values are concrete parsed `Value` payloads today.
fn first_update_parameter_index(statement: &SqlUpdateStatement) -> Option<usize> {
    statement
        .predicate
        .as_ref()
        .and_then(first_expr_parameter_index)
        .or_else(|| first_order_terms_parameter_index(statement.order_by.as_slice()))
}

// EXPLAIN wraps an executable reduced-SQL statement and therefore inherits the
// same parameter admission contract as its target.
fn first_explain_parameter_index(statement: &SqlExplainStatement) -> Option<usize> {
    match &statement.statement {
        SqlExplainTarget::Select(select) => first_select_parameter_index(select),
        SqlExplainTarget::Delete(delete) => first_delete_parameter_index(delete),
    }
}

// Scan projection items for placeholders in expression and aggregate inputs.
fn first_projection_parameter_index(projection: &SqlProjection) -> Option<usize> {
    let SqlProjection::Items(items) = projection else {
        return None;
    };

    items.iter().find_map(first_select_item_parameter_index)
}

// Scan one SELECT item, preserving the parser-owned distinction between field,
// aggregate, and general expression projection items.
fn first_select_item_parameter_index(item: &SqlSelectItem) -> Option<usize> {
    match item {
        SqlSelectItem::Field(_) => None,
        SqlSelectItem::Aggregate(aggregate) => first_aggregate_parameter_index(aggregate),
        SqlSelectItem::Expr(expr) => first_expr_parameter_index(expr),
    }
}

// Scan one aggregate call for placeholders in its input or FILTER expression.
fn first_aggregate_parameter_index(aggregate: &SqlAggregateCall) -> Option<usize> {
    aggregate
        .input
        .as_deref()
        .and_then(first_expr_parameter_index)
        .or_else(|| {
            aggregate
                .filter_expr
                .as_deref()
                .and_then(first_expr_parameter_index)
        })
}

// Scan ORDER BY expression terms for unsupported placeholders.
fn first_order_terms_parameter_index(order_by: &[SqlOrderTerm]) -> Option<usize> {
    order_by
        .iter()
        .find_map(|term| first_expr_parameter_index(&term.field))
}

// Use the parser-owned expression traversal so parameter detection stays on the
// same tree shape as aggregate and CASE validation.
fn first_expr_parameter_index(expr: &SqlExpr) -> Option<usize> {
    let mut parameter = None;
    expr.for_each_tree_expr(&mut |expr| {
        if parameter.is_none()
            && let SqlExpr::Param { index } = expr
        {
            parameter = Some(*index);
        }
    });

    parameter
}

/// Lower one prepared SQL statement into one shared generic-free command shape.
#[inline(never)]
pub(crate) fn lower_sql_command_from_prepared_statement(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    lower_prepared_statement(prepared.statement, model)
}

/// Lower one prepared SQL statement and return its SELECT query artifact.
#[inline(never)]
pub(crate) fn lower_prepared_sql_select_statement(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
) -> Result<LoweredSelectShape, SqlLoweringError> {
    let lowered = lower_sql_command_from_prepared_statement(prepared, model)?;
    let Some(select) = lowered.into_select_query() else {
        return Err(QueryError::prepared_sql_select_lane_mismatch().into());
    };

    Ok(select)
}

/// Lower one prepared SQL DELETE statement into its execution-ready artifact.
#[inline(never)]
pub(crate) fn lower_prepared_sql_delete_statement(
    prepared: PreparedSqlStatement,
) -> Result<LoweredDeleteShape, SqlLoweringError> {
    let SqlStatement::Delete(statement) = prepared.into_statement() else {
        return Err(QueryError::prepared_sql_delete_lane_mismatch().into());
    };

    lower_delete_statement_shape(statement)
}

/// Bind one prepared SQL SELECT through an explicit schema projection.
///
/// Write-side `INSERT ... SELECT` uses this accepted-schema-aware route so
/// source predicates follow the same top-level field authority as cached
/// query-surface SELECT compilation.
#[inline(never)]
pub(in crate::db) fn bind_prepared_sql_select_statement_structural_with_schema(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    let select = lower_prepared_sql_select_statement(prepared, model)?;

    bind_lowered_sql_select_query_structural_with_schema(model, select, consistency, schema)
}

/// Extract one normalized prepared SQL INSERT statement.
pub(crate) fn extract_prepared_sql_insert_statement(
    prepared: PreparedSqlStatement,
) -> Result<SqlInsertStatement, SqlLoweringError> {
    let SqlStatement::Insert(statement) = prepared.into_statement() else {
        return Err(QueryError::prepared_sql_insert_lane_mismatch().into());
    };

    Ok(statement)
}

/// Extract one normalized prepared SQL INSERT SELECT source statement.
pub(crate) fn extract_prepared_sql_insert_select_source(
    prepared: PreparedSqlStatement,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    let statement = extract_prepared_sql_insert_statement(prepared)?;
    let SqlInsertSource::Select(select) = statement.source else {
        return Err(QueryError::prepared_sql_insert_select_source_mismatch().into());
    };

    Ok(*select)
}

/// Extract one normalized prepared SQL UPDATE statement.
pub(crate) fn extract_prepared_sql_update_statement(
    prepared: PreparedSqlStatement,
) -> Result<SqlUpdateStatement, SqlLoweringError> {
    let SqlStatement::Update(statement) = prepared.into_statement() else {
        return Err(QueryError::prepared_sql_update_lane_mismatch().into());
    };

    Ok(statement)
}

#[inline(never)]
fn prepare_statement(
    statement: &SqlStatement,
    expected_entity: &'static str,
) -> Result<SqlStatement, SqlLoweringError> {
    // The compile boundary borrows the parsed statement, but preparation
    // returns an owned normalized statement. Clone only the selected statement
    // variant here so callers avoid a top-level clone before routing is known.
    match statement {
        SqlStatement::Select(statement) => Ok(SqlStatement::Select(prepare_select_statement(
            statement.clone(),
            expected_entity,
        )?)),
        SqlStatement::Delete(statement) => Ok(SqlStatement::Delete(prepare_delete_statement(
            statement.clone(),
            expected_entity,
        )?)),
        SqlStatement::Insert(statement) => Ok(SqlStatement::Insert(prepare_insert_statement(
            statement.clone(),
            expected_entity,
        )?)),
        SqlStatement::Update(statement) => Ok(SqlStatement::Update(prepare_update_statement(
            statement.clone(),
            expected_entity,
        )?)),
        SqlStatement::Explain(statement) => Ok(SqlStatement::Explain(prepare_explain_statement(
            statement.clone(),
            expected_entity,
        )?)),
        SqlStatement::Describe(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::Describe(statement.clone()))
        }
        SqlStatement::ShowIndexes(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowIndexes(statement.clone()))
        }
        SqlStatement::ShowColumns(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowColumns(statement.clone()))
        }
        SqlStatement::ShowEntities(statement) => Ok(SqlStatement::ShowEntities(statement.clone())),
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
        verbose: statement.verbose,
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
    statement: SqlDeleteStatement,
    expected_entity: &'static str,
) -> Result<SqlDeleteStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    Ok(normalize_delete_statement_to_expected_entity(
        statement,
        expected_entity,
    ))
}

fn prepare_update_statement(
    statement: SqlUpdateStatement,
    expected_entity: &'static str,
) -> Result<SqlUpdateStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    Ok(normalize_update_statement_to_expected_entity(
        statement,
        expected_entity,
    ))
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

// Normalize one SQL INSERT SELECT source and keep it on the scalar query lane.
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
            if item.contains_aggregate() {
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
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Select(lower_select_shape(statement, model)?),
        ))),
        SqlStatement::Delete(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Delete(lower_delete_shape(statement)?),
        ))),
        SqlStatement::Insert(_) | SqlStatement::Update(_) => {
            Err(SqlLoweringError::unexpected_query_lane_statement())
        }
        SqlStatement::Explain(statement) => lower_explain_prepared(statement, model),
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
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    let mode = statement.mode;
    let verbose = statement.verbose;

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select_prepared(select_statement, mode, verbose, model)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
                mode,
                verbose,
                query: LoweredSqlQuery::Delete(lower_delete_shape(delete_statement)?),
            }))
        }
    }
}

fn lower_explain_select_prepared(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    verbose: bool,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    if SqlStatement::Select(statement.clone()).is_global_aggregate_lane_shape() {
        let command = lower_global_aggregate_select_shape(statement)?;

        return Ok(LoweredSqlCommand(
            LoweredSqlCommandInner::ExplainGlobalAggregate {
                mode,
                verbose,
                command,
            },
        ));
    }

    match lower_select_shape(statement.clone(), model) {
        Ok(query) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
            mode,
            verbose,
            query: LoweredSqlQuery::Select(query),
        })),
        Err(SqlLoweringError::UnsupportedSelectProjection) => {
            let command = lower_global_aggregate_select_shape(statement)?;

            Ok(LoweredSqlCommand(
                LoweredSqlCommandInner::ExplainGlobalAggregate {
                    mode,
                    verbose,
                    command,
                },
            ))
        }
        Err(err) => Err(err),
    }
}
