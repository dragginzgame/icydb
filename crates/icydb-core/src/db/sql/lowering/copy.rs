//! Copy admitted SQL operands with charges at construction, not a sizing prewalk.

use crate::db::{
    query::preparation::PreparationWork,
    sql::{
        lowering::{
            SqlLoweringError,
            expr::{charge_storage, copy_text},
        },
        parser::{SqlAggregateCall, SqlCaseArm, SqlExpr, SqlMembershipValue, SqlSelectItem},
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Preserve the select-item expression shape while charging its retained copy.
pub(in crate::db::sql::lowering) fn copy_select_item_expr(
    item: &SqlSelectItem,
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    match item {
        SqlSelectItem::Expr(expr) => copy_sql_expr(expr, work),
        SqlSelectItem::Aggregate(aggregate) => {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            Ok(SqlExpr::Aggregate(copy_aggregate(aggregate, work)?))
        }
        SqlSelectItem::Field(field) => {
            work.charge(Resource::PredicateExpressionSteps, 1 + field.len() as u64)?;
            let Some((root, tail)) = field.split_once('.') else {
                return Ok(SqlExpr::Field(copy_text(field, work)?));
            };
            let (root, segments) = copy_field_path_parts(root, tail, work)?;
            Ok(SqlExpr::FieldPath { root, segments })
        }
    }
}

/// Construct dotted path backing shared by owned normalization and alias copies.
pub(in crate::db::sql::lowering) fn copy_field_path_parts(
    root: &str,
    tail: &str,
    work: &PreparationWork<'_>,
) -> Result<(String, Vec<String>), SqlLoweringError> {
    // Count separators without allocating; retain empty path components.
    work.charge(Resource::PredicateExpressionSteps, tail.len() as u64)?;
    let count = tail.split('.').count();
    charge_storage::<String>(count, work)?;
    let mut segments = Vec::with_capacity(count);
    for segment in tail.split('.') {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        segments.push(copy_text(segment, work)?);
    }
    Ok((copy_text(root, work)?, segments))
}

fn copy_box(expr: &SqlExpr, work: &PreparationWork<'_>) -> Result<Box<SqlExpr>, SqlLoweringError> {
    charge_storage::<SqlExpr>(1, work)?;
    Ok(Box::new(copy_sql_expr(expr, work)?))
}

fn copy_aggregate(
    aggregate: &SqlAggregateCall,
    work: &PreparationWork<'_>,
) -> Result<SqlAggregateCall, SqlLoweringError> {
    Ok(SqlAggregateCall {
        kind: aggregate.kind,
        input: aggregate
            .input
            .as_deref()
            .map(|expr| copy_box(expr, work))
            .transpose()?,
        filter_expr: aggregate
            .filter_expr
            .as_deref()
            .map(|expr| copy_box(expr, work))
            .transpose()?,
        distinct: aggregate.distinct,
    })
}

// All recursive inputs have already passed SQL input admission. Failed partial
// trees use SqlExpr's maintained cleanup; no partial result escapes this owner.
fn copy_sql_expr(expr: &SqlExpr, work: &PreparationWork<'_>) -> Result<SqlExpr, SqlLoweringError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    Ok(match expr {
        SqlExpr::Field(field) => SqlExpr::Field(copy_text(field, work)?),
        SqlExpr::FieldPath { root, segments } => {
            charge_storage::<String>(segments.len(), work)?;
            let mut copied = Vec::with_capacity(segments.len());
            for segment in segments {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                copied.push(copy_text(segment, work)?);
            }
            SqlExpr::FieldPath {
                root: copy_text(root, work)?,
                segments: copied,
            }
        }
        SqlExpr::Literal(value) => SqlExpr::Literal(work.copy_value(value)?),
        SqlExpr::Param { index } => SqlExpr::Param { index: *index },
        SqlExpr::Aggregate(aggregate) => SqlExpr::Aggregate(copy_aggregate(aggregate, work)?),
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => {
            let expr = copy_box(expr, work)?;
            charge_storage::<SqlMembershipValue>(values.len(), work)?;
            let mut copied = Vec::with_capacity(values.len());
            for value in values {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                copied.push(match value {
                    SqlMembershipValue::Literal(value) => {
                        SqlMembershipValue::Literal(work.copy_value(value)?)
                    }
                    SqlMembershipValue::Param { index } => {
                        SqlMembershipValue::Param { index: *index }
                    }
                });
            }
            SqlExpr::Membership {
                expr,
                values: copied,
                negated: *negated,
            }
        }
        SqlExpr::NullTest { expr, negated } => SqlExpr::NullTest {
            expr: copy_box(expr, work)?,
            negated: *negated,
        },
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            casefold,
        } => SqlExpr::Like {
            expr: copy_box(expr, work)?,
            pattern: copy_text(pattern, work)?,
            negated: *negated,
            casefold: *casefold,
        },
        SqlExpr::Unary { expr, op } => SqlExpr::Unary {
            expr: copy_box(expr, work)?,
            op: *op,
        },
        SqlExpr::Binary { left, right, op } => SqlExpr::Binary {
            left: copy_box(left, work)?,
            right: copy_box(right, work)?,
            op: *op,
        },
        SqlExpr::FunctionCall { function, args } => {
            charge_storage::<SqlExpr>(args.len(), work)?;
            let mut copied = Vec::with_capacity(args.len());
            for arg in args {
                copied.push(copy_sql_expr(arg, work)?);
            }
            SqlExpr::FunctionCall {
                function: *function,
                args: copied,
            }
        }
        SqlExpr::Case { arms, else_expr } => {
            charge_storage::<SqlCaseArm>(arms.len(), work)?;
            let mut copied = Vec::with_capacity(arms.len());
            for arm in arms {
                copied.push(SqlCaseArm {
                    condition: copy_sql_expr(&arm.condition, work)?,
                    result: copy_sql_expr(&arm.result, work)?,
                });
            }
            SqlExpr::Case {
                arms: copied,
                else_expr: else_expr
                    .as_deref()
                    .map(|expr| copy_box(expr, work))
                    .transpose()?,
            }
        }
    })
}
