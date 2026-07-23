use crate::db::{
    sql::{
        lowering::SqlLoweringError,
        parser::{
            SqlAggregateCall, SqlDeleteStatement, SqlExpr, SqlInsertSource, SqlInsertStatement,
            SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement,
            SqlUpdateStatement,
        },
    },
    sql_shared::{MAX_SQL_EXPR_DEPTH, sql_expr_depth_limit_error},
};

/// Validate that every parsed SQL expression tree is shallow enough for the
/// recursive normalization and lowering passes that follow preparation.
pub(in crate::db::sql::lowering) fn validate_sql_statement_ast_depth(
    statement: &SqlStatement,
) -> Result<(), SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => validate_select_statement_depth(statement),
        SqlStatement::Delete(statement) => validate_delete_statement_depth(statement),
        SqlStatement::Insert(statement) => validate_insert_statement_depth(statement),
        SqlStatement::Update(statement) => validate_update_statement_depth(statement),
        #[cfg(feature = "sql-explain")]
        SqlStatement::Explain(statement) => match &statement.statement {
            crate::db::sql::parser::SqlExplainTarget::Select(select) => {
                validate_select_statement_depth(select)
            }
            crate::db::sql::parser::SqlExplainTarget::Delete(delete) => {
                validate_delete_statement_depth(delete)
            }
        },
        SqlStatement::Ddl(_)
        | SqlStatement::Describe(_)
        | SqlStatement::ShowConstraints(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => Ok(()),
    }
}

fn validate_select_statement_depth(statement: &SqlSelectStatement) -> Result<(), SqlLoweringError> {
    validate_projection_depth(&statement.projection)?;
    validate_optional_expr_depth(statement.predicate.as_ref())?;
    validate_exprs_depth(statement.having.as_slice())?;
    validate_order_terms_depth(statement.order_by.as_slice())
}

fn validate_delete_statement_depth(statement: &SqlDeleteStatement) -> Result<(), SqlLoweringError> {
    validate_optional_expr_depth(statement.predicate.as_ref())?;
    validate_order_terms_depth(statement.order_by.as_slice())
}

fn validate_insert_statement_depth(statement: &SqlInsertStatement) -> Result<(), SqlLoweringError> {
    match &statement.source {
        SqlInsertSource::Values(_) | SqlInsertSource::DefaultValues => Ok(()),
        SqlInsertSource::Select(select) => validate_select_statement_depth(select),
    }
}

fn validate_update_statement_depth(statement: &SqlUpdateStatement) -> Result<(), SqlLoweringError> {
    validate_optional_expr_depth(statement.predicate.as_ref())?;
    validate_order_terms_depth(statement.order_by.as_slice())
}

fn validate_projection_depth(projection: &SqlProjection) -> Result<(), SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(());
    };

    for item in items {
        match item {
            SqlSelectItem::Field(_) => {}
            SqlSelectItem::Aggregate(aggregate) => validate_aggregate_depth(aggregate, 1)?,
            SqlSelectItem::Expr(expr) => validate_expr_depth(expr, 1)?,
        }
    }

    Ok(())
}

fn validate_optional_expr_depth(expr: Option<&SqlExpr>) -> Result<(), SqlLoweringError> {
    if let Some(expr) = expr {
        validate_expr_depth(expr, 1)?;
    }

    Ok(())
}

fn validate_exprs_depth(exprs: &[SqlExpr]) -> Result<(), SqlLoweringError> {
    for expr in exprs {
        validate_expr_depth(expr, 1)?;
    }

    Ok(())
}

fn validate_order_terms_depth(terms: &[SqlOrderTerm]) -> Result<(), SqlLoweringError> {
    for term in terms {
        validate_expr_depth(&term.field, 1)?;
    }

    Ok(())
}

fn validate_aggregate_depth(
    aggregate: &SqlAggregateCall,
    root_depth: usize,
) -> Result<(), SqlLoweringError> {
    reject_if_over_depth(root_depth)?;

    let mut stack = Vec::new();
    push_aggregate_children(&mut stack, aggregate, root_depth.saturating_add(1));
    validate_expr_stack(stack)
}

fn validate_expr_depth(expr: &SqlExpr, root_depth: usize) -> Result<(), SqlLoweringError> {
    validate_expr_stack(vec![(expr, root_depth)])
}

fn validate_expr_stack(mut stack: Vec<(&SqlExpr, usize)>) -> Result<(), SqlLoweringError> {
    while let Some((expr, depth)) = stack.pop() {
        reject_if_over_depth(depth)?;
        let child_depth = depth.saturating_add(1);

        match expr {
            SqlExpr::Field(_)
            | SqlExpr::FieldPath { .. }
            | SqlExpr::Literal(_)
            | SqlExpr::Param { .. } => {}
            SqlExpr::Aggregate(aggregate) => {
                push_aggregate_children(&mut stack, aggregate, child_depth);
            }
            SqlExpr::Membership { expr, .. }
            | SqlExpr::NullTest { expr, .. }
            | SqlExpr::Like { expr, .. }
            | SqlExpr::Unary { expr, .. } => stack.push((expr, child_depth)),
            SqlExpr::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push((arg, child_depth));
                }
            }
            SqlExpr::Binary { left, right, .. } => {
                stack.push((right, child_depth));
                stack.push((left, child_depth));
            }
            SqlExpr::Case { arms, else_expr } => {
                for arm in arms {
                    stack.push((&arm.result, child_depth));
                    stack.push((&arm.condition, child_depth));
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    stack.push((else_expr, child_depth));
                }
            }
        }
    }

    Ok(())
}

fn push_aggregate_children<'a>(
    stack: &mut Vec<(&'a SqlExpr, usize)>,
    aggregate: &'a SqlAggregateCall,
    child_depth: usize,
) {
    if let Some(input) = aggregate.input.as_deref() {
        stack.push((input, child_depth));
    }
    if let Some(filter_expr) = aggregate.filter_expr.as_deref() {
        stack.push((filter_expr, child_depth));
    }
}

fn reject_if_over_depth(depth: usize) -> Result<(), SqlLoweringError> {
    if depth > MAX_SQL_EXPR_DEPTH {
        return Err(sql_expr_depth_limit_error().into());
    }

    Ok(())
}
