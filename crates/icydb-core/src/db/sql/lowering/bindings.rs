//! Responsibility: admit typed WHERE operands before SQL boolean simplification.
//! Does not own: casts, expression types, query execution, or command caching.
//! Boundary: substitutes into an execution-owned copy of the existing SQL AST.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        QueryError,
        predicate::{Predicate, normalize_enum_literals},
        query::{
            plan::{
                PlanError,
                expr::{
                    Expr, compile_bool_compare_expr, infer_expr_type,
                    scalar_where_truth_condition_is_admitted,
                },
            },
            predicate::validate_predicate,
        },
        schema::{SchemaInfo, ValidateError},
        sql::{
            lowering::{
                PreparedSqlStatement,
                ast_depth::validate_sql_statement_ast_depth,
                expr::{SqlExprPhase, lower_sql_expr},
                prepare::{
                    first_order_terms_parameter_index, first_projection_parameter_index,
                    prepare_statement,
                },
            },
            parser::{SqlExpr, SqlExprBinaryOp, SqlMembershipValue, SqlStatement},
        },
    },
    value::{InputValue, PublicValue, Value},
};
use icydb_diagnostic_code::{QueryFieldRole, SqlLoweringCode};

const MAX_BINDINGS: usize = 64;
const MAX_BINDING_BYTES: u64 = 64 * 1024;

/// Admit placement, exact arity and scalar payloads without cloning caller data.
pub(crate) fn validate_sql_bindings(
    statement: &SqlStatement,
    bindings: &[InputValue],
) -> Result<(), QueryError> {
    if bindings.len() > MAX_BINDINGS {
        return Err(binding_error(SqlLoweringCode::BindingLimit));
    }
    validate_sql_statement_ast_depth(statement).map_err(QueryError::from_sql_lowering_error)?;
    let SqlStatement::Select(select) = statement else {
        return if bindings.is_empty() {
            Ok(())
        } else {
            Err(binding_error(SqlLoweringCode::ParameterPlacement))
        };
    };
    let forbidden = first_projection_parameter_index(&select.projection)
        .or_else(|| first_order_terms_parameter_index(&select.order_by))
        .or_else(|| {
            select.having.iter().find_map(|expr| {
                let mut first = None;
                expr.for_each_parameter(&mut |index| {
                    first.get_or_insert(index);
                });
                first
            })
        });
    if forbidden.is_some() {
        return Err(binding_error(SqlLoweringCode::ParameterPlacement));
    }
    let mut count = 0;
    if let Some(expr) = &select.predicate {
        // BETWEEN may repeat an AST operand. Lexical indices, not tree occurrence
        // counts, define the original input vector.
        expr.for_each_parameter(&mut |index| {
            count = count.max(index.saturating_add(1));
        });
    }
    if count != bindings.len() {
        return Err(binding_error(SqlLoweringCode::BindingCount));
    }
    let mut bytes = 0_u64;
    for input in bindings {
        bytes = bytes
            .checked_add(scalar_payload_bytes(input.as_public())?)
            .filter(|bytes| *bytes <= MAX_BINDING_BYTES)
            .ok_or_else(|| binding_error(SqlLoweringCode::BindingLimit))?;
    }
    Ok(())
}

// Logical scalar payload, not retained capacity or caller construction/decoder work.
// Big-integer magnitude length is queried without first allocating digits/encoding.
fn scalar_payload_bytes(value: &PublicValue) -> Result<u64, QueryError> {
    Ok(match value {
        PublicValue::List(_) | PublicValue::Map(_) | PublicValue::Enum(_) => {
            return Err(binding_error(SqlLoweringCode::BindingFamily));
        }
        PublicValue::Blob(value) => value.len() as u64,
        PublicValue::Text(value) => value.len() as u64,
        PublicValue::IntBig(value) => value.magnitude_bits().div_ceil(8).saturating_add(1),
        PublicValue::NatBig(value) => value.magnitude_bits().div_ceil(8),
        PublicValue::Account(value) => {
            value.owner().as_slice().len() as u64
                + 1
                + if value.subaccount().is_some() { 32 } else { 0 }
        }
        PublicValue::Principal(value) => value.as_slice().len() as u64,
        PublicValue::Subaccount(_) | PublicValue::U256(_) => 32,
        PublicValue::Int128(_)
        | PublicValue::Nat128(_)
        | PublicValue::Decimal(_)
        | PublicValue::Ulid(_) => 16,
        PublicValue::Int64(_)
        | PublicValue::Nat64(_)
        | PublicValue::Float64(_)
        | PublicValue::Duration(_)
        | PublicValue::Timestamp(_) => 8,
        PublicValue::Float32(_) | PublicValue::Date(_) => 4,
        PublicValue::Bool(_) => 1,
        PublicValue::Null | PublicValue::Unit => 0,
    })
}

fn binding_error(code: SqlLoweringCode) -> QueryError {
    QueryError::sql_lowering(code)
}

/// Normalize identifiers, then validate every bound context before destructive folds.
/// Query ingress must first call `validate_sql_bindings` on these same inputs.
pub(crate) fn prepare_bound_sql_statement(
    statement: &SqlStatement,
    entity: &str,
    schema: &SchemaInfo,
    bindings: &[InputValue],
) -> Result<PreparedSqlStatement, QueryError> {
    let mut statement =
        prepare_statement(statement, entity).map_err(QueryError::from_sql_lowering_error)?;
    let SqlStatement::Select(select) = &mut statement else {
        return Err(binding_error(SqlLoweringCode::ParameterPlacement));
    };
    if let Some(expr) = &mut select.predicate {
        bind_expr(expr, schema, bindings, true)?;
        let lowered = lower_sql_expr(expr, SqlExprPhase::Where)
            .map_err(QueryError::from_sql_lowering_error)?;
        if !scalar_where_truth_condition_is_admitted(&lowered) {
            return Err(binding_error(SqlLoweringCode::WhereExpressionShape));
        }
    }
    Ok(PreparedSqlStatement { statement })
}

fn operand(index: usize, bindings: &[InputValue]) -> Result<Value, QueryError> {
    bindings
        .get(index)
        .ok_or_else(|| binding_error(SqlLoweringCode::BindingCount))?
        .clone()
        .try_into_runtime_non_enum()
        .ok_or_else(|| binding_error(SqlLoweringCode::BindingFamily))
}

// Returns whether the original subtree carried any slot. Never short-circuit
// sibling traversal: even values in an unreachable branch require admission.
// `infer_here` is false only when an enclosing typed expression will infer this
// complete subtree before folding. Comparison normalization still runs here.
fn bind_expr(
    expr: &mut SqlExpr,
    schema: &SchemaInfo,
    bindings: &[InputValue],
    infer_here: bool,
) -> Result<bool, QueryError> {
    let bound = match expr {
        SqlExpr::Param { index } => {
            *expr = SqlExpr::Literal(operand(*index, bindings)?);
            return Ok(true);
        }
        SqlExpr::Field(_) | SqlExpr::FieldPath { .. } | SqlExpr::Literal(_) => false,
        SqlExpr::Aggregate(_) => return Err(binding_error(SqlLoweringCode::ParameterPlacement)),
        SqlExpr::Binary { op, left, right } => {
            let boolean = matches!(op, SqlExprBinaryOp::And | SqlExprBinaryOp::Or);
            let left_bound = bind_expr(left, schema, bindings, infer_here && boolean)?;
            let right_bound = bind_expr(right, schema, bindings, infer_here && boolean)?;
            let bound = left_bound | right_bound;
            let comparison = matches!(
                op,
                SqlExprBinaryOp::Eq
                    | SqlExprBinaryOp::Ne
                    | SqlExprBinaryOp::Lt
                    | SqlExprBinaryOp::Lte
                    | SqlExprBinaryOp::Gt
                    | SqlExprBinaryOp::Gte
            );
            if bound && !boolean && (infer_here || comparison) {
                admit_compare_or_expression(expr, schema, infer_here)?;
            }
            return Ok(bound);
        }
        SqlExpr::Membership { expr, values, .. } => {
            let target_bound = bind_expr(expr, schema, bindings, infer_here)?;
            let mut bound = target_bound;
            for value in values {
                let value_bound = matches!(value, SqlMembershipValue::Param { .. });
                if let SqlMembershipValue::Param { index } = value {
                    *value = SqlMembershipValue::Literal(operand(*index, bindings)?);
                }
                bound |= value_bound;
                if target_bound || value_bound {
                    let SqlMembershipValue::Literal(value) = value else {
                        return Err(QueryError::invariant());
                    };
                    let mut compare = SqlExpr::Binary {
                        op: SqlExprBinaryOp::Eq,
                        left: expr.clone(),
                        right: Box::new(SqlExpr::Literal(value.clone())),
                    };
                    admit_compare_or_expression(&mut compare, schema, true)?;
                    if let SqlExpr::Binary { right, .. } = compare
                        && let SqlExpr::Literal(normalized) = *right
                    {
                        *value = normalized;
                    }
                }
            }
            return Ok(bound);
        }
        // Boolean wrappers use the shared WHERE truth-shape owner above. Broad
        // expression inference would narrow maintained predicate/null semantics.
        SqlExpr::Unary { expr, .. } => return bind_expr(expr, schema, bindings, infer_here),
        SqlExpr::NullTest { expr, .. } | SqlExpr::Like { expr, .. } => {
            bind_expr(expr, schema, bindings, false)?
        }
        SqlExpr::FunctionCall { args, .. } => {
            let mut bound = false;
            for arg in args {
                bound |= bind_expr(arg, schema, bindings, false)?;
            }
            bound
        }
        SqlExpr::Case { arms, else_expr } => {
            let mut bound = false;
            for arm in arms {
                bound |= bind_expr(&mut arm.condition, schema, bindings, false)?;
                bound |= bind_expr(&mut arm.result, schema, bindings, false)?;
            }
            if let Some(expr) = else_expr {
                bound |= bind_expr(expr, schema, bindings, false)?;
            }
            bound
        }
    };
    if bound && infer_here {
        admit_compare_or_expression(expr, schema, true)?;
    }
    Ok(bound)
}

// The planner's existing leaf compiler chooses coercion. Its query normalizer
// and validator own meaning; inference owns expressions outside that leaf subset.
fn admit_compare_or_expression(
    expr: &mut SqlExpr,
    schema: &SchemaInfo,
    infer_here: bool,
) -> Result<(), QueryError> {
    let lowered =
        lower_sql_expr(expr, SqlExprPhase::Where).map_err(QueryError::from_sql_lowering_error)?;
    if let Expr::Binary { op, left, right } = &lowered
        && let Some(predicate) = compile_bool_compare_expr(*op, left, right)
    {
        let predicate = normalize_enum_literals(schema, &predicate).map_err(query_operand_error)?;
        if let Predicate::Compare(compare) = &predicate
            && matches!(compare.value(), Value::Null)
        {
            validate_predicate(
                schema,
                &Predicate::IsNull {
                    field: compare.field().to_string(),
                },
            )
            .map_err(query_operand_error)?;
        } else {
            validate_predicate(schema, &predicate).map_err(query_operand_error)?;
        }
        if let Predicate::Compare(compare) = predicate
            && let SqlExpr::Binary { left, right, .. } = expr
        {
            if matches!(right.as_ref(), SqlExpr::Literal(_)) {
                **right = SqlExpr::Literal(compare.value);
            } else if matches!(left.as_ref(), SqlExpr::Literal(_)) {
                **left = SqlExpr::Literal(compare.value);
            }
        }
        return Ok(());
    }
    if infer_here {
        infer_expr_type(&lowered, schema).map_err(|error| {
            QueryError::from(error.attach_query_field(QueryFieldRole::Predicate))
        })?;
    }
    Ok(())
}

fn query_operand_error(error: ValidateError) -> QueryError {
    PlanError::from(error)
        .attach_query_field(QueryFieldRole::Predicate)
        .into()
}
