use crate::db::QueryError;
use crate::db::sql::lowering::{
    LoweredSqlCommand, LoweredSqlCommandInner, LoweredSqlQuery, PreparedSqlParameterContract,
    PreparedSqlParameterTypeFamily, PreparedSqlStatement, SqlLoweringError,
    aggregate::{is_sql_global_aggregate_statement, lower_global_aggregate_select_shape},
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
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "parameterized IN predicates are not supported in 0.98 v1",
                ));
            }

            Ok(())
        }
        SqlExpr::NullTest { expr, .. } => {
            if contains_param(expr) {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "NULL tests over parameter slots are not supported in 0.98 v1",
                ));
            }

            Ok(())
        }
        SqlExpr::FunctionCall { function, args } => {
            if args.iter().any(contains_param) {
                let label = match function {
                    SqlScalarFunction::StartsWith => {
                        "parameterized STARTS WITH predicates are not supported in 0.98 v1"
                    }
                    _ => "parameterized function-call predicates are not supported in 0.98 v1",
                };
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index_from_args(args.as_slice()),
                    label,
                ));
            }

            Ok(())
        }
        SqlExpr::Unary { expr, .. } => collect_where_param_contracts(expr, model, contracts),
        SqlExpr::Case { .. } => {
            if contains_param(expr) {
                return Err(SqlLoweringError::unsupported_parameter_placement(
                    extract_first_param_index(expr),
                    "CASE expressions with parameters are not supported in 0.98 v1",
                ));
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
                collect_compare_param_contract(left, right, model, contracts, "WHERE")
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
    match field_kind {
        FieldKind::Bool => Ok(PreparedSqlParameterTypeFamily::Bool),
        FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Timestamp => Ok(PreparedSqlParameterTypeFamily::Numeric),
        FieldKind::Text | FieldKind::Enum { .. } => Ok(PreparedSqlParameterTypeFamily::Text),
        FieldKind::Relation { key_kind, .. } => field_kind_type_family(*key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            "field kind is outside the initial 0.98 v1 prepared compare-family surface",
        )),
    }
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
