//! Responsibility: admit typed WHERE operands before SQL boolean simplification.
//! Does not own: casts, expression types, query execution, or command caching.
//! Boundary: validates bound semantic operands without mutating SQL syntax.

#[cfg(test)]
mod tests;

use crate::db::query::preparation::PreparationWork;
use crate::{
    db::{
        QueryError,
        predicate::{Predicate, normalize_enum_literals},
        query::{
            plan::{
                PlanError,
                expr::{
                    BinaryOp, Expr, compile_bool_compare_expr, infer_expr_type,
                    scalar_where_truth_condition_is_admitted,
                },
            },
            predicate::validate_predicate,
        },
        schema::{SchemaInfo, ValidateError},
        sql::{
            input::validate_sql_statement_input,
            lowering::{
                expr::{charge_storage, lower_bound_where},
                prepare::{first_order_terms_parameter_index, first_projection_parameter_index},
            },
            parser::{SqlExpr, SqlStatement},
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
    validate_sql_statement_input(statement, &[])?;
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
    let mut payloads = Vec::with_capacity(bindings.len());
    for input in bindings {
        let (scalar_bytes, payload) = scalar_input_sizes(input.as_public())?;
        bytes = bytes
            .checked_add(scalar_bytes)
            .filter(|bytes| *bytes <= MAX_BINDING_BYTES)
            .ok_or_else(|| binding_error(SqlLoweringCode::BindingLimit))?;
        payloads.push(payload);
    }
    if !payloads.is_empty() {
        validate_sql_statement_input(statement, &payloads)?;
    }
    Ok(())
}

// Return (binding representation bytes, shared variable payload bytes).
// Both caps inspect the same admitted scalar without encoding or cloning it.
fn scalar_input_sizes(value: &PublicValue) -> Result<(u64, usize), QueryError> {
    let fixed_bytes = match value {
        PublicValue::List(_) | PublicValue::Map(_) | PublicValue::Enum(_) => {
            return Err(binding_error(SqlLoweringCode::BindingFamily));
        }
        PublicValue::Blob(value) => return Ok((value.len() as u64, value.len())),
        PublicValue::Text(value) => return Ok((value.len() as u64, value.len())),
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
    };
    Ok((fixed_bytes, 0))
}

fn binding_error(code: SqlLoweringCode) -> QueryError {
    QueryError::sql_lowering(code)
}

pub(in crate::db::sql::lowering) fn bind_prepared_where(
    expr: &SqlExpr,
    schema: &SchemaInfo,
    bindings: &[InputValue],
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let lowered = lower_bound_where(expr, schema, bindings, work)
        .map_err(QueryError::from_sql_lowering_error)?;
    if !scalar_where_truth_condition_is_admitted(&lowered) {
        return Err(binding_error(SqlLoweringCode::WhereExpressionShape));
    }
    Ok(lowered)
}

pub(in crate::db::sql::lowering) fn operand(
    index: usize,
    bindings: &[InputValue],
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    let input = bindings
        .get(index)
        .ok_or_else(|| binding_error(SqlLoweringCode::BindingCount))?;
    // Ingress admits only scalars. Charge their variable backing before cloning;
    // fixed representations need no separately allocated payload.
    let (_, payload) = scalar_input_sizes(input.as_public())?;
    work.charge(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::TemporaryBytes,
        payload as u64,
    )?;
    work.charge(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        payload as u64,
    )?;
    work.charge(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
        1,
    )?;
    input
        .clone()
        .try_into_runtime_non_enum()
        .ok_or_else(|| binding_error(SqlLoweringCode::BindingFamily))
}

pub(in crate::db::sql::lowering) fn admit_bound_membership_item(
    target: &Expr,
    value: &Value,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let right = Expr::Literal(work.copy_value(value)?);
    if !admit_lowered_compare(BinaryOp::Eq, target, &right, schema, work)? {
        charge_storage::<Expr>(2, work).map_err(QueryError::from_sql_lowering_error)?;
        let compare = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(work.copy_expr(target)?),
            right: Box::new(right),
        };
        infer_expr_type(&compare, schema, work)
            .map_err(|error| error.attach_query_field(QueryFieldRole::Predicate))?;
    }
    Ok(())
}

pub(in crate::db::sql::lowering) fn admit_bound_expression(
    lowered: &Expr,
    schema: &SchemaInfo,
    infer_here: bool,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if let Expr::Binary { op, left, right } = lowered
        && admit_lowered_compare(*op, left, right, schema, work)?
    {
        return Ok(());
    }
    if infer_here {
        infer_expr_type(lowered, schema, work)
            .map_err(|error| error.attach_query_field(QueryFieldRole::Predicate))?;
    }
    Ok(())
}

// Admit comparison operands without writing canonical values back into syntax.
// Final schema-bound lowering owns the executable representation.
fn admit_lowered_compare(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<bool, QueryError> {
    let Some(predicate) = compile_bool_compare_expr(op, left, right, work)? else {
        return Ok(false);
    };
    let predicate =
        normalize_enum_literals(schema, &predicate, work).map_err(|error| match error {
            QueryError::Validate(error) => query_operand_error(*error),
            error => error,
        })?;
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
    Ok(true)
}

fn query_operand_error(error: ValidateError) -> QueryError {
    PlanError::from(error)
        .attach_query_field(QueryFieldRole::Predicate)
        .into()
}
