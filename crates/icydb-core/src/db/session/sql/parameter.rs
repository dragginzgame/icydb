#![allow(dead_code)]

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::EntityAuthority,
        session::sql::SqlCompiledCommandCacheKey,
        sql::{
            lowering::{PreparedSqlStatement, SqlLoweringError},
            parser::{
                SqlAggregateCall, SqlAggregateKind, SqlExpr, SqlProjection, SqlScalarFunction,
                SqlSelectItem, SqlStatement,
            },
        },
    },
    model::{entity::EntityModel, field::FieldKind},
    traits::{CanisterKind, EntityValue},
    value::Value,
};

///
/// PreparedSqlParameterTypeFamily
///
/// Stable bind-time type family for one prepared SQL parameter slot.
/// This keeps v1 validation coarse and deterministic while the prepared SQL
/// surface remains restricted to compare-family value-insensitive positions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PreparedSqlParameterTypeFamily {
    Numeric,
    Text,
    Bool,
}

///
/// PreparedSqlParameterContract
///
/// Frozen bind contract for one prepared SQL parameter slot.
/// The contract is inferred once during prepare and reused unchanged for every
/// execution of the prepared SQL query shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PreparedSqlParameterContract {
    index: usize,
    type_family: PreparedSqlParameterTypeFamily,
    null_allowed: bool,
}

impl PreparedSqlParameterContract {
    #[must_use]
    pub(in crate::db) const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub(in crate::db) const fn type_family(&self) -> PreparedSqlParameterTypeFamily {
        self.type_family
    }

    #[must_use]
    pub(in crate::db) const fn null_allowed(&self) -> bool {
        self.null_allowed
    }
}

///
/// PreparedSqlQuery
///
/// Session-owned prepared reduced-SQL query shape for v1 parameter binding.
/// This keeps parsing, normalization, and parameter-contract collection stable
/// across repeated executions while still reusing the existing bound SQL
/// execution path after literal substitution.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PreparedSqlQuery {
    source_sql: String,
    statement: PreparedSqlStatement,
    parameter_contracts: Vec<PreparedSqlParameterContract>,
}

impl PreparedSqlQuery {
    #[must_use]
    pub(in crate::db) fn source_sql(&self) -> &str {
        &self.source_sql
    }

    #[must_use]
    pub(in crate::db) fn parameter_contracts(&self) -> &[PreparedSqlParameterContract] {
        self.parameter_contracts.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn parameter_count(&self) -> usize {
        self.parameter_contracts.len()
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Prepare one parameterized reduced-SQL query shape for repeated execution.
    pub(in crate::db) fn prepare_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<PreparedSqlQuery, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = crate::db::session::sql::parse_sql_statement_with_attribution(sql)
            .map(|(statement, _)| statement)?;
        Self::ensure_sql_query_statement_supported(&statement)?;

        let authority = EntityAuthority::for_type::<E>();
        let prepared = Self::prepare_sql_statement_for_authority(&statement, authority)?;
        let parameter_contracts =
            collect_parameter_contracts_for_query(prepared.statement(), authority.model())
                .map_err(QueryError::from_sql_lowering_error)?;

        Ok(PreparedSqlQuery {
            source_sql: sql.to_string(),
            statement: prepared,
            parameter_contracts,
        })
    }

    /// Execute one prepared reduced-SQL query with one validated binding vector.
    pub(in crate::db) fn execute_prepared_sql_query<E>(
        &self,
        prepared: &PreparedSqlQuery,
        bindings: &[Value],
    ) -> Result<crate::db::session::sql::SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        validate_parameter_bindings(prepared.parameter_contracts(), bindings)?;

        let bound_statement =
            bind_prepared_statement_literals(prepared.statement.statement(), bindings)?;
        let authority = EntityAuthority::for_type::<E>();
        let compiled_cache_key =
            SqlCompiledCommandCacheKey::query_for_entity::<E>(prepared.source_sql());
        let compiled = Self::compile_sql_statement_for_authority(
            &bound_statement,
            authority,
            compiled_cache_key,
        )?
        .0;

        self.execute_compiled_sql::<E>(&compiled)
    }
}

fn collect_parameter_contracts_for_query(
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
    for having in &statement.having {
        collect_having_param_contracts(having, model, &mut contracts)?;
    }

    Ok(contracts)
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
        SqlExpr::Membership { expr, .. } => reject_params_in_expr(expr, clause),
        SqlExpr::NullTest { expr, .. } => reject_params_in_expr(expr, clause),
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                reject_params_in_expr(arg, clause)?;
            }

            Ok(())
        }
        SqlExpr::Unary { expr, .. } => reject_params_in_expr(expr, clause),
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
            contracts.push(PreparedSqlParameterContract {
                index: *index,
                type_family: field_compare_type_family(model, field)?,
                null_allowed: true,
            });

            Ok(())
        }
        (SqlExpr::Aggregate(aggregate), SqlExpr::Param { index }) => {
            contracts.push(PreparedSqlParameterContract {
                index: *index,
                type_family: aggregate_compare_type_family(aggregate, model)?,
                null_allowed: true,
            });

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

fn validate_parameter_bindings(
    contracts: &[PreparedSqlParameterContract],
    bindings: &[Value],
) -> Result<(), QueryError> {
    if bindings.len() != contracts.len() {
        return Err(QueryError::unsupported_query(format!(
            "prepared SQL expected {} bindings, found {}",
            contracts.len(),
            bindings.len(),
        )));
    }

    for contract in contracts {
        let binding = bindings.get(contract.index()).ok_or_else(|| {
            QueryError::unsupported_query(format!(
                "missing prepared SQL binding at index={}",
                contract.index(),
            ))
        })?;
        if !binding_matches_contract(binding, contract) {
            return Err(QueryError::unsupported_query(format!(
                "prepared SQL binding at index={} does not match the required {:?} contract",
                contract.index(),
                contract.type_family(),
            )));
        }
    }

    Ok(())
}

fn binding_matches_contract(value: &Value, contract: &PreparedSqlParameterContract) -> bool {
    if matches!(value, Value::Null) {
        return contract.null_allowed();
    }

    match contract.type_family() {
        PreparedSqlParameterTypeFamily::Numeric => matches!(
            value,
            Value::Int(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Uint(_)
                | Value::Uint128(_)
                | Value::UintBig(_)
                | Value::Float32(_)
                | Value::Float64(_)
                | Value::Decimal(_)
                | Value::Duration(_)
                | Value::Timestamp(_)
        ),
        PreparedSqlParameterTypeFamily::Text => {
            matches!(value, Value::Text(_) | Value::Enum(_))
        }
        PreparedSqlParameterTypeFamily::Bool => matches!(value, Value::Bool(_)),
    }
}

fn bind_prepared_statement_literals(
    statement: &SqlStatement,
    bindings: &[Value],
) -> Result<SqlStatement, QueryError> {
    match statement {
        SqlStatement::Select(select) => Ok(SqlStatement::Select(
            crate::db::sql::parser::SqlSelectStatement {
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
            },
        )),
        _ => Err(QueryError::unsupported_query(
            "prepared SQL binding currently supports SELECT query shapes only",
        )),
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
