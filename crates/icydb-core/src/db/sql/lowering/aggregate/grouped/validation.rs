use crate::{
    db::{
        query::builder::AggregateExpr,
        schema::SchemaInfo,
        sql::{
            lowering::{
                SqlLoweringError,
                aggregate::{lower_aggregate_call, lowering::validate_model_bound_scalar_expr},
            },
            parser::{SqlAggregateCall, SqlExpr, SqlSelectItem},
        },
    },
    model::entity::EntityModel,
};

// Extend one unique aggregate-call list from one SQL expression while keeping
// first-seen SQL order stable for grouped reducer slot assignment.
pub(in crate::db::sql::lowering) fn extend_unique_sql_expr_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    expr: &SqlExpr,
) {
    expr.for_each_tree_aggregate(&mut |aggregate| {
        push_unique_sql_aggregate_call(aggregate_calls, aggregate.clone());
    });
}

// Extend one unique aggregate-call list from one SQL select item while keeping
// SQL item-order ownership local to shared aggregate collection helpers.
pub(in crate::db::sql::lowering) fn extend_unique_sql_select_item_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    item: &SqlSelectItem,
) {
    match item {
        SqlSelectItem::Field(_) => {}
        SqlSelectItem::Aggregate(aggregate) => {
            push_unique_sql_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlSelectItem::Expr(expr) => {
            extend_unique_sql_expr_aggregate_calls(aggregate_calls, expr);
        }
    }
}

pub(in crate::db::sql::lowering) fn resolve_having_aggregate_expr_index(
    target: &AggregateExpr,
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<usize, SqlLoweringError> {
    let mut matched =
        grouped_projection_aggregates
            .iter()
            .enumerate()
            .filter_map(|(index, aggregate)| {
                lower_aggregate_call(aggregate.clone())
                    .ok()
                    .filter(|current| current == target)
                    .map(|_| index)
            });
    let Some(index) = matched.next() else {
        return Err(SqlLoweringError::unsupported_select_having());
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(index)
}

// Keep grouped aggregate scalar-subexpression validation on one lowering seam
// so alias leakage inside FILTER or aggregate inputs fails as a user-facing
// SQL error before grouped execution reaches its scalar compiler invariant.
pub(in crate::db::sql::lowering::aggregate) fn validate_grouped_aggregate_scalar_subexpressions(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    aggregate: &AggregateExpr,
) -> Result<(), SqlLoweringError> {
    if let Some(input_expr) = aggregate.input_expr() {
        validate_model_bound_scalar_expr(
            model,
            schema,
            input_expr,
            SqlLoweringError::unsupported_aggregate_input_expressions,
        )?;
    }
    if let Some(filter_expr) = aggregate.filter_expr() {
        validate_model_bound_scalar_expr(
            model,
            schema,
            filter_expr,
            SqlLoweringError::unsupported_where_expression,
        )?;
    }

    Ok(())
}

// Keep aggregate extraction on one stable first-seen unique terminal order so
// repeated SQL aggregate leaves reuse the same reducer slot.
fn push_unique_sql_aggregate_call(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    aggregate: SqlAggregateCall,
) {
    if aggregate_calls.iter().all(|current| current != &aggregate) {
        aggregate_calls.push(aggregate);
    }
}
