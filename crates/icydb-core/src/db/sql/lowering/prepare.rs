use crate::db::QueryError;
use crate::db::sql::lowering::{
    LoweredSqlCommand, LoweredSqlCommandInner, LoweredSqlQuery, PreparedSqlParameterContract,
    PreparedSqlParameterTypeFamily, PreparedSqlStatement, SqlLoweringError,
    aggregate::{is_sql_global_aggregate_statement, lower_global_aggregate_select_shape},
    expr::{SqlExprPhase, lower_sql_binary_op, lower_sql_expr, lower_sql_scalar_function},
    normalize::{
        adapt_sql_predicate_identifiers_to_scope, ensure_entity_matches_expected,
        normalize_order_terms, normalize_select_statement_to_expected_entity,
        sql_entity_scope_candidates,
    },
    select::{lower_delete_shape, lower_select_shape, select_item_contains_aggregate},
};
use crate::db::sql::parser::{
    SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlExplainMode, SqlExplainStatement,
    SqlExplainTarget, SqlExpr, SqlInsertSource, SqlInsertStatement, SqlProjection,
    SqlScalarFunction, SqlSelectItem, SqlSelectStatement, SqlStatement,
};
use crate::db::{
    query::plan::expr::{
        ExprCoarseTypeFamily, binary_operand_coarse_family, coarse_family_for_field_kind,
        coarse_family_for_literal, function_arg_coarse_family, function_result_coarse_family,
        infer_expr_coarse_family,
    },
    schema::SchemaInfo,
};
use crate::model::entity::EntityModel;
use crate::model::field::FieldKind;
use crate::types::{Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Timestamp};
use crate::value::Value;

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
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    lower_prepared_statement(prepared.statement, model)
}

pub(in crate::db) fn collect_prepared_statement_parameter_contracts(
    statement: &SqlStatement,
    model: &'static EntityModel,
) -> Result<Vec<PreparedSqlParameterContract>, SqlLoweringError> {
    let SqlStatement::Select(statement) = statement else {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            "parameterized prepare currently supports SQL SELECT query shapes only",
        ));
    };

    let mut contracts = Vec::new();

    reject_params_in_projection(statement.projection.clone())?;
    for order_term in &statement.order_by {
        reject_params_in_expr(&order_term.field, "ORDER BY")?;
    }
    if let Some(predicate) = &statement.predicate {
        collect_where_param_contracts(predicate, model, &mut contracts)?;
    }
    for having_expr in &statement.having {
        collect_having_param_contracts(having_expr, model, &mut contracts)?;
    }

    Ok(contracts)
}

pub(in crate::db) fn bind_prepared_statement_literals(
    statement: &SqlStatement,
    bindings: &[Value],
) -> Result<SqlStatement, QueryError> {
    match statement {
        SqlStatement::Select(select) => Ok(SqlStatement::Select(SqlSelectStatement {
            projection: bind_projection_literals(&select.projection, bindings)?,
            projection_aliases: select.projection_aliases.clone(),
            entity: select.entity.clone(),
            predicate: select
                .predicate
                .as_ref()
                .map(|expr| bind_sql_expr_literals(expr, bindings))
                .transpose()?,
            distinct: select.distinct,
            group_by: select.group_by.clone(),
            having: select
                .having
                .iter()
                .map(|expr| bind_sql_expr_literals(expr, bindings))
                .collect::<Result<Vec<_>, _>>()?,
            order_by: select
                .order_by
                .iter()
                .map(|term| {
                    Ok(crate::db::sql::parser::SqlOrderTerm {
                        field: bind_sql_expr_literals(&term.field, bindings)?,
                        direction: term.direction,
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
            limit: select.limit,
            offset: select.offset,
        })),
        _ => Err(QueryError::unsupported_query(
            "prepared SQL binding currently supports SELECT query shapes only",
        )),
    }
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
    statement.predicate = statement.predicate.map(|predicate| {
        adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice())
    });
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
            if select_item_contains_aggregate(item) {
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

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select_prepared(select_statement, mode, model)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
                mode,
                query: LoweredSqlQuery::Delete(lower_delete_shape(delete_statement)?),
            }))
        }
    }
}

fn lower_explain_select_prepared(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    if is_sql_global_aggregate_statement(&SqlStatement::Select(statement.clone())) {
        let command = lower_global_aggregate_select_shape(statement)?;

        return Ok(LoweredSqlCommand(
            LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command },
        ));
    }

    match lower_select_shape(statement.clone(), model) {
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

fn reject_params_in_projection(projection: SqlProjection) -> Result<(), SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(());
    };

    for item in items {
        match item {
            SqlSelectItem::Field(_) => {}
            SqlSelectItem::Aggregate(aggregate) => {
                if let Some(input) = aggregate.input.as_deref() {
                    reject_params_in_expr(input, "SELECT aggregate input")?;
                }
                if let Some(filter) = aggregate.filter_expr.as_deref() {
                    reject_params_in_expr(filter, "SELECT aggregate FILTER")?;
                }
            }
            SqlSelectItem::Expr(expr) => {
                reject_params_in_expr(&expr, "SELECT projection")?;
            }
        }
    }

    Ok(())
}

fn reject_params_in_expr(expr: &SqlExpr, clause: &str) -> Result<(), SqlLoweringError> {
    match expr {
        SqlExpr::Param { index } => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            format!("parameterized {clause} is not supported in 0.98 v1"),
        )),
        SqlExpr::Field(_) | SqlExpr::Literal(_) => Ok(()),
        SqlExpr::Aggregate(aggregate) => {
            if let Some(input) = aggregate.input.as_deref() {
                reject_params_in_expr(input, clause)?;
            }
            if let Some(filter) = aggregate.filter_expr.as_deref() {
                reject_params_in_expr(filter, clause)?;
            }

            Ok(())
        }
        SqlExpr::Membership { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => reject_params_in_expr(expr, clause),
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                reject_params_in_expr(arg, clause)?;
            }

            Ok(())
        }
        SqlExpr::Binary { left, right, .. } => {
            reject_params_in_expr(left, clause)?;
            reject_params_in_expr(right, clause)
        }
        SqlExpr::Case { arms, else_expr } => {
            for arm in arms {
                reject_params_in_expr(&arm.condition, clause)?;
                reject_params_in_expr(&arm.result, clause)?;
            }
            if let Some(else_expr) = else_expr {
                reject_params_in_expr(else_expr, clause)?;
            }

            Ok(())
        }
    }
}

fn collect_where_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Literal(_) => Ok(()),
        SqlExpr::Param { index } => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            "bare WHERE parameter is not supported in 0.98 v1",
        )),
        SqlExpr::Aggregate(_) => Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            "WHERE does not admit aggregate parameter contracts",
        )),
        SqlExpr::Membership { expr, .. } => {
            if contains_param(expr) {
                let expected = infer_membership_value_family(expr, model)?;
                collect_where_value_param_contracts(expr, model, Some(expected), contracts)?;
            }

            Ok(())
        }
        SqlExpr::NullTest { expr, .. } => {
            if contains_param(expr) {
                collect_where_value_param_contracts(expr, model, None, contracts)?;
            }

            Ok(())
        }
        SqlExpr::FunctionCall { function: _, args } => {
            if args.iter().any(contains_param) {
                let expected = infer_where_bool_expr_param_family(expr, model)?;
                collect_where_value_param_contracts(expr, model, Some(expected), contracts)?;
            }

            Ok(())
        }
        SqlExpr::Unary { expr, .. } => collect_where_param_contracts(expr, model, contracts),
        SqlExpr::Case { .. } => {
            if contains_param(expr) {
                collect_where_value_param_contracts(
                    expr,
                    model,
                    Some(PreparedSqlParameterTypeFamily::Bool),
                    contracts,
                )?;
            }

            Ok(())
        }
        SqlExpr::Binary { op, left, right } => match op {
            crate::db::sql::parser::SqlExprBinaryOp::And
            | crate::db::sql::parser::SqlExprBinaryOp::Or => {
                collect_where_param_contracts(left, model, contracts)?;
                collect_where_param_contracts(right, model, contracts)
            }
            crate::db::sql::parser::SqlExprBinaryOp::Eq
            | crate::db::sql::parser::SqlExprBinaryOp::Ne
            | crate::db::sql::parser::SqlExprBinaryOp::Lt
            | crate::db::sql::parser::SqlExprBinaryOp::Lte
            | crate::db::sql::parser::SqlExprBinaryOp::Gt
            | crate::db::sql::parser::SqlExprBinaryOp::Gte => {
                if matches!(left.as_ref(), SqlExpr::Field(_) | SqlExpr::Aggregate(_))
                    && matches!(right.as_ref(), SqlExpr::Param { .. })
                {
                    collect_compare_param_contract(left, right, model, contracts, "WHERE")
                } else if contains_param(left) || contains_param(right) {
                    collect_where_compare_param_contracts(left, right, model, contracts)
                } else {
                    Ok(())
                }
            }
            crate::db::sql::parser::SqlExprBinaryOp::Add
            | crate::db::sql::parser::SqlExprBinaryOp::Sub
            | crate::db::sql::parser::SqlExprBinaryOp::Mul
            | crate::db::sql::parser::SqlExprBinaryOp::Div => {
                if contains_param(left) || contains_param(right) {
                    collect_where_value_param_contracts(expr, model, None, contracts)?;
                }

                Ok(())
            }
        },
    }
}

const fn prepared_family_from_expr_coarse_family(
    family: ExprCoarseTypeFamily,
) -> PreparedSqlParameterTypeFamily {
    match family {
        ExprCoarseTypeFamily::Bool => PreparedSqlParameterTypeFamily::Bool,
        ExprCoarseTypeFamily::Numeric => PreparedSqlParameterTypeFamily::Numeric,
        ExprCoarseTypeFamily::Text => PreparedSqlParameterTypeFamily::Text,
    }
}

fn infer_sql_expr_prepared_family(
    expr: &SqlExpr,
    phase: SqlExprPhase,
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    let lowered = lower_sql_expr(expr, phase)?;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let family = infer_expr_coarse_family(&lowered, schema)
        .map_err(|error| SqlLoweringError::from(QueryError::from(error)))?;

    Ok(family.map(prepared_family_from_expr_coarse_family))
}

// Collect coarse prepared parameter contracts for one general expression-owned
// SQL `WHERE` value subtree. This widens fallback prepared execution to the
// shared expression admission surface without widening any template-local
// predicate/access ownership.
fn collect_where_value_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    if !contains_param(expr) {
        return resolve_non_param_where_family(expr, model, expected);
    }

    match expr {
        SqlExpr::Field(field) => field_compare_type_family(model, field),
        SqlExpr::Literal(value) => value_type_family(value).or(expected).ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                None,
                "NULL-only expression parameter positions are not supported in prepared SQL",
            )
        }),
        SqlExpr::Param { index } => {
            let expected = expected.ok_or_else(|| {
                SqlLoweringError::unsupported_parameter_placement(
                    Some(*index),
                    "prepared SQL could not infer a parameter contract for the expression-owned WHERE position",
                )
            })?;
            contracts.push(PreparedSqlParameterContract::new(
                *index, expected, true, None,
            ));

            Ok(expected)
        }
        SqlExpr::Aggregate(_) => Err(SqlLoweringError::unsupported_parameter_placement(
            extract_first_param_index(expr),
            "WHERE does not admit aggregate parameter contracts",
        )),
        SqlExpr::Membership { expr, .. } => {
            collect_where_membership_param_contracts(expr, model, expected, contracts)
        }
        SqlExpr::NullTest { expr, .. } => {
            collect_where_value_param_contracts(expr, model, None, contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExpr::Unary { expr, .. } => {
            collect_where_value_param_contracts(
                expr,
                model,
                Some(PreparedSqlParameterTypeFamily::Bool),
                contracts,
            )?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExpr::Binary { op, left, right } => {
            collect_where_binary_param_contracts(*op, left, right, model, contracts)
        }
        SqlExpr::FunctionCall { function, args } => collect_where_function_param_contracts(
            *function,
            args.as_slice(),
            model,
            expected,
            contracts,
        ),
        SqlExpr::Case { arms, else_expr } => collect_where_case_param_contracts(
            arms,
            else_expr.as_deref(),
            model,
            expected,
            contracts,
        ),
    }
}

// Resolve one non-parameter value subtree by deriving its family from the
// shared planner-owned expression typing model and validating any required
// outer family constraint against that derived result.
fn resolve_non_param_where_family(
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let inferred = infer_sql_expr_prepared_family(expr, SqlExprPhase::PreAggregate, model)?;
    let family = inferred.or(expected).ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            None,
            "NULL-only expression parameter positions are not supported in prepared SQL",
        )
    })?;
    if let Some(expected) = expected
        && family != expected
    {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            extract_first_param_index(expr),
            format!(
                "prepared SQL inferred {family:?} for one expression-owned WHERE subtree where {expected:?} was required"
            ),
        ));
    }

    Ok(family)
}

// Collect one expression-owned membership subtree under the boolean WHERE
// surface while inferring one target family from either the outer requirement
// or the admitted membership target itself.
fn collect_where_membership_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let expected = expected.or_else(|| infer_membership_value_family(expr, model).ok());
    collect_where_value_param_contracts(expr, model, expected, contracts)?;

    Ok(PreparedSqlParameterTypeFamily::Bool)
}

// Collect one binary WHERE subtree by splitting the boolean/compare/numeric
// families explicitly and reusing the same child traversal helper for the
// shared-operand cases.
fn collect_where_binary_param_contracts(
    op: crate::db::sql::parser::SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    match op {
        crate::db::sql::parser::SqlExprBinaryOp::Or
        | crate::db::sql::parser::SqlExprBinaryOp::And => {
            let expected_operand = binary_operand_coarse_family(lower_sql_binary_op(op))
                .map(prepared_family_from_expr_coarse_family)
                .expect("boolean SQL binary operators should keep one shared coarse family");
            collect_where_value_children([left, right], model, Some(expected_operand), contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        crate::db::sql::parser::SqlExprBinaryOp::Eq
        | crate::db::sql::parser::SqlExprBinaryOp::Ne
        | crate::db::sql::parser::SqlExprBinaryOp::Lt
        | crate::db::sql::parser::SqlExprBinaryOp::Lte
        | crate::db::sql::parser::SqlExprBinaryOp::Gt
        | crate::db::sql::parser::SqlExprBinaryOp::Gte => {
            collect_where_compare_param_contracts(left, right, model, contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        crate::db::sql::parser::SqlExprBinaryOp::Add
        | crate::db::sql::parser::SqlExprBinaryOp::Sub
        | crate::db::sql::parser::SqlExprBinaryOp::Mul
        | crate::db::sql::parser::SqlExprBinaryOp::Div => {
            let expected_operand = binary_operand_coarse_family(lower_sql_binary_op(op))
                .map(prepared_family_from_expr_coarse_family)
                .expect("numeric SQL binary operators should keep one shared coarse family");
            collect_where_value_children([left, right], model, Some(expected_operand), contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Numeric)
        }
    }
}

// Collect one searched CASE subtree by forcing boolean condition contracts and
// one unified result family across all branches before descending into each
// parameter-carrying child.
fn collect_where_case_param_contracts(
    arms: &[crate::db::sql::parser::SqlCaseArm],
    else_expr: Option<&SqlExpr>,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    collect_where_value_children(
        arms.iter().map(|arm| &arm.condition),
        model,
        Some(PreparedSqlParameterTypeFamily::Bool),
        contracts,
    )?;
    let branch_family = expected
        .or_else(|| infer_case_result_family(arms, else_expr, model).ok())
        .ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                arms.iter()
                    .find_map(|arm| extract_first_param_index(&arm.result))
                    .or_else(|| else_expr.and_then(extract_first_param_index)),
                "prepared SQL could not infer one CASE result contract for the expression-owned WHERE position",
            )
        })?;
    collect_where_value_children(
        arms.iter().map(|arm| &arm.result),
        model,
        Some(branch_family),
        contracts,
    )?;
    if let Some(else_expr) = else_expr {
        collect_where_value_param_contracts(else_expr, model, Some(branch_family), contracts)?;
    }

    Ok(branch_family)
}

// Collect one shared expected contract family across multiple expression
// children while preserving the same fallback-only traversal rules used by the
// single-expression helper above.
fn collect_where_value_children<'a>(
    exprs: impl IntoIterator<Item = &'a SqlExpr>,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    for expr in exprs {
        collect_where_value_param_contracts(expr, model, expected, contracts)?;
    }

    Ok(())
}

// Infer coarse compare-side parameter contracts for one expression-owned SQL
// `WHERE` compare. This keeps prepared fallback on the shared expression seam
// while still letting direct right-hand field compares keep their older
// template-capable contract metadata.
fn collect_where_compare_param_contracts(
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    let right_hint = infer_where_value_family_hint(right, model)?;
    let left_family = collect_where_value_param_contracts(left, model, right_hint, contracts)?;
    let left_hint = infer_where_value_family_hint(left, model)?;
    let expected_right = left_hint.or(Some(left_family));
    let right_family =
        collect_where_value_param_contracts(right, model, expected_right, contracts)?;

    if left_family != right_family {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            extract_first_param_index_from_pair(left, right),
            format!(
                "prepared SQL could not unify compare operand contracts for the expression-owned WHERE position ({left_family:?} vs {right_family:?})"
            ),
        ));
    }

    Ok(())
}

// Infer one function-owned coarse parameter contract family and collect any
// parameter slots nested under that function according to the existing shared
// scalar-expression signature.
fn collect_where_function_param_contracts(
    function: SqlScalarFunction,
    args: &[SqlExpr],
    model: &'static EntityModel,
    expected_result: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let function = lower_sql_scalar_function(function);
    let shared_result_family =
        function_result_coarse_family(function).map(prepared_family_from_expr_coarse_family);
    let inferred_result_family = match shared_result_family {
        Some(family) => family,
        None => infer_dynamic_function_result_family(function, args, model, expected_result)?,
    };
    let result_family = expected_result
        .map(|expected| {
            if expected == inferred_result_family {
                Ok(expected)
            } else {
                Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index_from_args(args),
                    format!(
                        "prepared SQL inferred {inferred_result_family:?} for one function-owned WHERE subtree where {expected:?} was required"
                    ),
                ))
            }
        })
        .transpose()?
        .unwrap_or(inferred_result_family);

    for (index, arg) in args.iter().enumerate() {
        let expected = function_arg_coarse_family(function, index)
            .map(prepared_family_from_expr_coarse_family)
            .or(match function {
                crate::db::query::plan::expr::Function::Coalesce
                | crate::db::query::plan::expr::Function::NullIf => Some(result_family),
                _ => None,
            });
        collect_where_value_param_contracts(arg, model, expected, contracts)?;
    }

    Ok(result_family)
}

// Infer one dynamic result-family contract for fallback-only functions whose
// result family depends on their argument family rather than a fixed planner
// signature.
fn infer_dynamic_function_result_family(
    function: crate::db::query::plan::expr::Function,
    args: &[SqlExpr],
    model: &'static EntityModel,
    expected_result: Option<PreparedSqlParameterTypeFamily>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let dynamic_family = match function {
        crate::db::query::plan::expr::Function::Coalesce => {
            if let Some(expected) = expected_result {
                Some(expected)
            } else {
                infer_coalesce_param_family(args, model)?
            }
        }
        crate::db::query::plan::expr::Function::NullIf => {
            if let Some(expected) = expected_result {
                Some(expected)
            } else {
                infer_nullif_param_family(args, model)?
            }
        }
        _ => {
            return Err(SqlLoweringError::unsupported_parameter_placement(
                extract_first_param_index_from_args(args),
                "prepared SQL function family is outside the aligned fallback typing surface",
            ));
        }
    };

    dynamic_family.ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            extract_first_param_index_from_args(args),
            match function {
                crate::db::query::plan::expr::Function::Coalesce => {
                    "prepared SQL could not infer one COALESCE contract for the expression-owned WHERE position"
                }
                crate::db::query::plan::expr::Function::NullIf => {
                    "prepared SQL could not infer one NULLIF contract for the expression-owned WHERE position"
                }
                _ => unreachable!("dynamic function error message is only defined for supported functions"),
            },
        )
    })
}

// Infer one coarse family hint for one non-parameter SQL value subtree. This
// is only a hint source for neighboring parameter slots, not an admissibility
// gate, so it intentionally returns `None` whenever the family cannot be read
// from the existing static subtree alone.
fn infer_where_value_family_hint(
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    if contains_param(expr) {
        return Ok(None);
    }

    infer_sql_expr_prepared_family(expr, SqlExprPhase::PreAggregate, model)
}

// Infer the coarse family shared by literal membership values so parameterized
// left-hand `IN (...)` targets can stay on prepared fallback without assuming
// one template shape.
fn infer_membership_value_family(
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    infer_where_value_family_hint(expr, model)?.ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            extract_first_param_index(expr),
            "prepared SQL could not infer one IN target contract for the expression-owned WHERE position",
        )
    })
}

// Infer one shared coarse family across one expression set by deriving every
// available family hint from planner-aligned subtree typing and then forcing
// those hints to agree.
fn infer_common_param_family<'a>(
    exprs: impl IntoIterator<Item = &'a SqlExpr>,
    model: &'static EntityModel,
    missing_message: &'static str,
    conflict_label: &'static str,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    let mut family = None;
    for expr in exprs {
        let Some(next) = infer_where_value_family_hint(expr, model)? else {
            continue;
        };
        match family {
            None => family = Some(next),
            Some(current) if current == next => {}
            Some(current) => {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    format!(
                        "prepared SQL could not unify {conflict_label} contracts ({current:?} vs {next:?})"
                    ),
                ));
            }
        }
    }

    if family.is_none() {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            missing_message,
        ));
    }

    Ok(family)
}

// Infer the shared coarse family for one `COALESCE(...)` argument list when a
// prepared fallback query needs parameter contracts but templates are no
// longer allowed to own the enclosing expression semantics.
fn infer_coalesce_param_family(
    args: &[SqlExpr],
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    infer_common_param_family(
        args.iter(),
        model,
        "prepared SQL could not infer one COALESCE contract for the expression-owned WHERE position",
        "COALESCE argument",
    )
}

// Infer the shared coarse family for one `NULLIF(left, right)` pair when a
// prepared fallback query needs parameter contracts for the surrounding
// expression-owned `WHERE` shape.
fn infer_nullif_param_family(
    args: &[SqlExpr],
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    infer_coalesce_param_family(args, model)
}

// Infer the coarse result family for one searched `CASE` value expression so
// parameterized branches can stay on prepared fallback without forcing one
// predicate/access template shape.
fn infer_case_result_family(
    arms: &[crate::db::sql::parser::SqlCaseArm],
    else_expr: Option<&SqlExpr>,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let result_exprs = arms.iter().map(|arm| &arm.result).chain(else_expr);
    infer_common_param_family(
        result_exprs,
        model,
        "prepared SQL could not infer one CASE result contract for the expression-owned WHERE position",
        "CASE result",
    )?
    .ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            None,
            "prepared SQL could not infer one CASE result contract for the expression-owned WHERE position",
        )
    })
}

// Infer the coarse boolean-family result of one top-level SQL `WHERE` subtree.
// This keeps expression-owned prepared fallback contracts explicit without
// letting non-boolean subtrees masquerade as standalone predicates.
fn infer_where_bool_expr_param_family(
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    if !contains_param(expr) {
        let family = infer_where_value_family_hint(expr, model)?.ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                extract_first_param_index(expr),
                "prepared SQL could not infer one boolean contract for the expression-owned WHERE position",
            )
        })?;
        if family != PreparedSqlParameterTypeFamily::Bool {
            return Err(SqlLoweringError::unsupported_parameter_placement(
                extract_first_param_index(expr),
                "prepared SQL WHERE parameters must stay on one boolean predicate surface",
            ));
        }

        return Ok(family);
    }

    let family = match expr {
        SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Case { .. } => PreparedSqlParameterTypeFamily::Bool,
        SqlExpr::Binary { op, .. } => match op {
            crate::db::sql::parser::SqlExprBinaryOp::Or
            | crate::db::sql::parser::SqlExprBinaryOp::And
            | crate::db::sql::parser::SqlExprBinaryOp::Eq
            | crate::db::sql::parser::SqlExprBinaryOp::Ne
            | crate::db::sql::parser::SqlExprBinaryOp::Lt
            | crate::db::sql::parser::SqlExprBinaryOp::Lte
            | crate::db::sql::parser::SqlExprBinaryOp::Gt
            | crate::db::sql::parser::SqlExprBinaryOp::Gte => PreparedSqlParameterTypeFamily::Bool,
            crate::db::sql::parser::SqlExprBinaryOp::Add
            | crate::db::sql::parser::SqlExprBinaryOp::Sub
            | crate::db::sql::parser::SqlExprBinaryOp::Mul
            | crate::db::sql::parser::SqlExprBinaryOp::Div => {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "prepared SQL WHERE parameters must stay on one boolean predicate surface",
                ));
            }
        },
        SqlExpr::Field(_) | SqlExpr::Literal(_) | SqlExpr::Param { .. } | SqlExpr::Aggregate(_) => {
            return Err(SqlLoweringError::unsupported_parameter_placement(
                extract_first_param_index(expr),
                "prepared SQL WHERE parameters must stay on one boolean predicate surface",
            ));
        }
    };

    Ok(family)
}

// Map one SQL literal onto the coarse prepared bind family used by v1
// parameter validation. `NULL` stays unresolved here and must inherit one
// family from surrounding context instead of inventing one locally.
fn value_type_family(value: &Value) -> Option<PreparedSqlParameterTypeFamily> {
    coarse_family_for_literal(value).map(prepared_family_from_expr_coarse_family)
}

fn collect_having_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Literal(_) | SqlExpr::Aggregate(_) => Ok(()),
        SqlExpr::Param { index } => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            "bare HAVING parameter is not supported in 0.98 v1",
        )),
        SqlExpr::Unary { expr, .. } => collect_having_param_contracts(expr, model, contracts),
        SqlExpr::NullTest { expr, .. } => {
            if contains_param(expr) {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "NULL tests over parameter slots are not supported in 0.98 v1",
                ));
            }

            Ok(())
        }
        SqlExpr::FunctionCall { .. } | SqlExpr::Membership { .. } | SqlExpr::Case { .. } => {
            if contains_param(expr) {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "only compare-style HAVING parameter positions are supported in 0.98 v1",
                ));
            }

            Ok(())
        }
        SqlExpr::Binary { op, left, right } => match op {
            crate::db::sql::parser::SqlExprBinaryOp::And
            | crate::db::sql::parser::SqlExprBinaryOp::Or => {
                collect_having_param_contracts(left, model, contracts)?;
                collect_having_param_contracts(right, model, contracts)
            }
            crate::db::sql::parser::SqlExprBinaryOp::Eq
            | crate::db::sql::parser::SqlExprBinaryOp::Ne
            | crate::db::sql::parser::SqlExprBinaryOp::Lt
            | crate::db::sql::parser::SqlExprBinaryOp::Lte
            | crate::db::sql::parser::SqlExprBinaryOp::Gt
            | crate::db::sql::parser::SqlExprBinaryOp::Gte => {
                collect_compare_param_contract(left, right, model, contracts, "HAVING")
            }
            crate::db::sql::parser::SqlExprBinaryOp::Add
            | crate::db::sql::parser::SqlExprBinaryOp::Sub
            | crate::db::sql::parser::SqlExprBinaryOp::Mul
            | crate::db::sql::parser::SqlExprBinaryOp::Div => {
                if contains_param(left) || contains_param(right) {
                    return Err(SqlLoweringError::unsupported_parameter_placement(
                        extract_first_param_index(expr),
                        "arithmetic parameter expressions are not supported in 0.98 v1",
                    ));
                }

                Ok(())
            }
        },
    }
}

fn collect_compare_param_contract(
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
    clause: &str,
) -> Result<(), SqlLoweringError> {
    match (left, right) {
        (SqlExpr::Field(field), SqlExpr::Param { index }) => {
            let field_kind = model
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field)
                .map(crate::model::field::FieldModel::kind)
                .ok_or_else(|| SqlLoweringError::unknown_field(field.clone()))?;
            contracts.push(PreparedSqlParameterContract::new(
                *index,
                field_kind_type_family(field_kind)?,
                true,
                template_binding_for_field_kind(field_kind, *index),
            ));

            Ok(())
        }
        (SqlExpr::Aggregate(aggregate), SqlExpr::Param { index }) => {
            contracts.push(PreparedSqlParameterContract::new(
                *index,
                aggregate_compare_type_family(aggregate, model)?,
                true,
                template_binding_for_aggregate_compare(aggregate, model, *index),
            ));

            Ok(())
        }
        (_, SqlExpr::Param { index }) => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            format!(
                "only field-compare and aggregate-compare {clause} parameter positions are supported in 0.98 v1"
            ),
        )),
        _ => {
            if contains_param(left) || contains_param(right) {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index_from_pair(left, right),
                    format!(
                        "only right-hand compare parameters are supported in {clause} for 0.98 v1"
                    ),
                ));
            }

            Ok(())
        }
    }
}

fn field_compare_type_family(
    model: &'static EntityModel,
    field: &str,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let field_kind = model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(crate::model::field::FieldModel::kind)
        .ok_or_else(|| SqlLoweringError::unknown_field(field.to_string()))?;

    field_kind_type_family(field_kind)
}

fn template_binding_for_aggregate_compare(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
    index: usize,
) -> Option<Value> {
    let Ok(index) = u64::try_from(index) else {
        return None;
    };

    match aggregate.kind {
        SqlAggregateKind::Count | SqlAggregateKind::Sum | SqlAggregateKind::Avg => {
            Some(Value::Uint(u64::MAX.saturating_sub(index)))
        }
        SqlAggregateKind::Min | SqlAggregateKind::Max => {
            let Some(SqlExpr::Field(field)) = aggregate.input.as_deref() else {
                return None;
            };
            let field_kind = model
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field)
                .map(crate::model::field::FieldModel::kind)?;

            template_binding_for_field_kind(field_kind, usize::try_from(index).ok()?)
        }
    }
}

fn aggregate_compare_type_family(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    match aggregate.kind {
        SqlAggregateKind::Count | SqlAggregateKind::Sum | SqlAggregateKind::Avg => {
            Ok(PreparedSqlParameterTypeFamily::Numeric)
        }
        SqlAggregateKind::Min | SqlAggregateKind::Max => {
            let Some(input) = aggregate.input.as_deref() else {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    None,
                    "target-less MIN/MAX parameter contracts are not supported in 0.98 v1",
                ));
            };
            let SqlExpr::Field(field) = input else {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(input),
                    "expression-backed MIN/MAX parameter contracts are not supported in 0.98 v1",
                ));
            };

            field_compare_type_family(model, field)
        }
    }
}

fn field_kind_type_family(
    field_kind: FieldKind,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    coarse_family_for_field_kind(&field_kind)
        .map(prepared_family_from_expr_coarse_family)
        .ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                None,
                "field kind is outside the initial 0.98 v1 prepared compare-family surface",
            )
        })
}

fn template_binding_for_field_kind(field_kind: FieldKind, index: usize) -> Option<Value> {
    let index_u64 = u64::try_from(index).ok()?;

    match field_kind {
        FieldKind::Int => {
            let offset = i64::try_from(index_u64).ok()?;

            Some(Value::Int(i64::MAX.saturating_sub(offset)))
        }
        FieldKind::Int128 => Some(Value::Int128(Int128::from(
            i128::MAX - i128::from(index_u64),
        ))),
        FieldKind::IntBig => Some(Value::IntBig(Int::from(
            i32::MAX.saturating_sub(i32::try_from(index_u64).unwrap_or(i32::MAX)),
        ))),
        FieldKind::Uint => Some(Value::Uint(u64::MAX.saturating_sub(index_u64))),
        FieldKind::Uint128 => Some(Value::Uint128(Nat128::from(
            u128::MAX - u128::from(index_u64),
        ))),
        FieldKind::UintBig => Some(Value::UintBig(Nat::from(
            u64::MAX.saturating_sub(index_u64),
        ))),
        FieldKind::Float32 => {
            let offset = f32::from(u16::try_from(index_u64.min(1_000)).ok()?);

            Float32::try_new(f32::MAX - offset).map(Value::Float32)
        }
        FieldKind::Float64 => {
            let offset = f64::from(u16::try_from(index_u64.min(1_000)).ok()?);

            Float64::try_new(f64::MAX - offset).map(Value::Float64)
        }
        FieldKind::Decimal { scale } => Some(Value::Decimal(Decimal::from_i128_with_scale(
            i128::MAX - i128::from(index_u64),
            scale,
        ))),
        FieldKind::Duration => Some(Value::Duration(Duration::from_millis(
            u64::MAX.saturating_sub(index_u64),
        ))),
        FieldKind::Timestamp => {
            let offset = i64::try_from(index_u64).ok()?;

            Some(Value::Timestamp(Timestamp::from_millis(i64::MAX - offset)))
        }
        FieldKind::Text => Some(Value::Text(format!(
            "__icydb_prepared_param_text_{index_u64}__"
        ))),
        FieldKind::Relation { key_kind, .. } => template_binding_for_field_kind(*key_kind, index),
        FieldKind::Bool
        | FieldKind::Enum { .. }
        | FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => None,
    }
}

fn bind_projection_literals(
    projection: &SqlProjection,
    bindings: &[Value],
) -> Result<SqlProjection, QueryError> {
    match projection {
        SqlProjection::All => Ok(SqlProjection::All),
        SqlProjection::Items(items) => Ok(SqlProjection::Items(
            items
                .iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => Ok(SqlSelectItem::Field(field.clone())),
                    SqlSelectItem::Aggregate(aggregate) => Ok(SqlSelectItem::Aggregate(
                        bind_sql_aggregate_literals(aggregate, bindings)?,
                    )),
                    SqlSelectItem::Expr(expr) => {
                        Ok(SqlSelectItem::Expr(bind_sql_expr_literals(expr, bindings)?))
                    }
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
        )),
    }
}

fn bind_sql_aggregate_literals(
    aggregate: &SqlAggregateCall,
    bindings: &[Value],
) -> Result<SqlAggregateCall, QueryError> {
    Ok(SqlAggregateCall {
        kind: aggregate.kind,
        input: aggregate
            .input
            .as_ref()
            .map(|input| bind_sql_expr_literals(input, bindings).map(Box::new))
            .transpose()?,
        filter_expr: aggregate
            .filter_expr
            .as_ref()
            .map(|filter| bind_sql_expr_literals(filter, bindings).map(Box::new))
            .transpose()?,
        distinct: aggregate.distinct,
    })
}

fn bind_sql_expr_literals(expr: &SqlExpr, bindings: &[Value]) -> Result<SqlExpr, QueryError> {
    match expr {
        SqlExpr::Field(field) => Ok(SqlExpr::Field(field.clone())),
        SqlExpr::Aggregate(aggregate) => Ok(SqlExpr::Aggregate(bind_sql_aggregate_literals(
            aggregate, bindings,
        )?)),
        SqlExpr::Literal(value) => Ok(SqlExpr::Literal(value.clone())),
        SqlExpr::Param { index } => {
            let value = bindings.get(*index).ok_or_else(|| {
                QueryError::unsupported_query(format!(
                    "missing prepared SQL binding at index={index}",
                ))
            })?;

            Ok(SqlExpr::Literal(value.clone()))
        }
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => Ok(SqlExpr::Membership {
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
            values: values.clone(),
            negated: *negated,
        }),
        SqlExpr::NullTest { expr, negated } => Ok(SqlExpr::NullTest {
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
            negated: *negated,
        }),
        SqlExpr::FunctionCall { function, args } => Ok(SqlExpr::FunctionCall {
            function: *function,
            args: args
                .iter()
                .map(|arg| bind_sql_expr_literals(arg, bindings))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        SqlExpr::Unary { op, expr } => Ok(SqlExpr::Unary {
            op: *op,
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
        }),
        SqlExpr::Binary { op, left, right } => Ok(SqlExpr::Binary {
            op: *op,
            left: Box::new(bind_sql_expr_literals(left, bindings)?),
            right: Box::new(bind_sql_expr_literals(right, bindings)?),
        }),
        SqlExpr::Case { arms, else_expr } => Ok(SqlExpr::Case {
            arms: arms
                .iter()
                .map(|arm| {
                    Ok(crate::db::sql::parser::SqlCaseArm {
                        condition: bind_sql_expr_literals(&arm.condition, bindings)?,
                        result: bind_sql_expr_literals(&arm.result, bindings)?,
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| bind_sql_expr_literals(else_expr, bindings).map(Box::new))
                .transpose()?,
        }),
    }
}

fn contains_param(expr: &SqlExpr) -> bool {
    extract_first_param_index(expr).is_some()
}

fn extract_first_param_index(expr: &SqlExpr) -> Option<usize> {
    match expr {
        SqlExpr::Param { index } => Some(*index),
        SqlExpr::Field(_) | SqlExpr::Literal(_) => None,
        SqlExpr::Aggregate(aggregate) => aggregate
            .input
            .as_deref()
            .and_then(extract_first_param_index)
            .or_else(|| {
                aggregate
                    .filter_expr
                    .as_deref()
                    .and_then(extract_first_param_index)
            }),
        SqlExpr::Membership { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => extract_first_param_index(expr),
        SqlExpr::FunctionCall { args, .. } => extract_first_param_index_from_args(args.as_slice()),
        SqlExpr::Binary { left, right, .. } => {
            extract_first_param_index(left).or_else(|| extract_first_param_index(right))
        }
        SqlExpr::Case { arms, else_expr } => arms
            .iter()
            .find_map(|arm| {
                extract_first_param_index(&arm.condition)
                    .or_else(|| extract_first_param_index(&arm.result))
            })
            .or_else(|| else_expr.as_deref().and_then(extract_first_param_index)),
    }
}

fn extract_first_param_index_from_args(args: &[SqlExpr]) -> Option<usize> {
    args.iter().find_map(extract_first_param_index)
}

fn extract_first_param_index_from_pair(left: &SqlExpr, right: &SqlExpr) -> Option<usize> {
    extract_first_param_index(left).or_else(|| extract_first_param_index(right))
}
