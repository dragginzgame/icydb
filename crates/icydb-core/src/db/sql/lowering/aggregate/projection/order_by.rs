use crate::db::{
    query::plan::expr::Alias,
    sql::{
        lowering::{
            SqlLoweringError, analyze_lowered_expr, expr::SqlExprPhase,
            select::lower_select_item_expr,
        },
        parser::{SqlExpr, SqlOrderTerm, SqlProjection},
    },
};

// Drop singleton-result ORDER BY terms that target the global aggregate output
// row itself, while preserving base-row ordering used to shape the aggregate input window.
pub(in crate::db::sql::lowering::aggregate) fn strip_inert_global_aggregate_output_order_terms(
    order_by: Vec<SqlOrderTerm>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Vec<SqlOrderTerm>, SqlLoweringError> {
    let inert_targets =
        collect_global_aggregate_output_order_targets(projection, projection_aliases)?;

    Ok(order_by
        .into_iter()
        .filter(|term| !inert_targets.iter().any(|target| target == &term.field))
        .collect())
}

// Collect the canonical ORDER BY spellings that refer to the singleton global
// aggregate output row so the dedicated aggregate lane can ignore them instead
// of re-deriving them as base-row ordering.
fn collect_global_aggregate_output_order_targets(
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Vec<SqlExpr>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(Vec::new());
    };

    let mut targets = Vec::with_capacity(items.len());
    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let expr = lower_select_item_expr(item, SqlExprPhase::PostAggregate)?;
        let analysis = analyze_lowered_expr(&expr, None);
        if !analysis.contains_aggregate() || analysis.references_direct_fields() {
            continue;
        }

        targets.push(SqlExpr::from_select_item(item));
        if let Some(alias) = alias {
            targets.push(SqlExpr::Field(Alias::new(alias).as_str().to_string()));
        }
    }

    Ok(targets)
}
