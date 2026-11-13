use crate::db::query::preparation::PreparationWork;
use crate::db::{
    query::{
        intent::StructuralQuery,
        plan::{OrderSpec, OrderTerm},
    },
    sql::{
        lowering::{
            LoweredExprAnalysis, SqlLoweringError, analyze_lowered_expr,
            expr::{SqlExprPhase, lower_sql_expr},
        },
        parser::{SqlOrderDirection, SqlOrderTerm},
    },
};

///
/// LoweredSqlOrderTerm
///
/// Lowered ORDER BY term carried after SQL expression normalization.
///

#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) struct LoweredSqlOrderTerm {
    pub(in crate::db::sql::lowering) expr: crate::db::query::plan::expr::Expr,
    pub(in crate::db::sql::lowering) analysis: LoweredExprAnalysis,
    pub(in crate::db::sql::lowering) direction: SqlOrderDirection,
}

pub(super) fn lower_order_terms(
    order_by: Vec<SqlOrderTerm>,
    work: &PreparationWork<'_>,
) -> Result<Vec<LoweredSqlOrderTerm>, SqlLoweringError> {
    order_by
        .into_iter()
        .map(|term| lower_order_term(&term, work))
        .collect()
}

pub(super) fn apply_order_terms_structural(
    query: StructuralQuery,
    order_by: Vec<LoweredSqlOrderTerm>,
) -> StructuralQuery {
    if order_by.is_empty() {
        return query;
    }

    query.order_spec(OrderSpec {
        fields: order_by
            .into_iter()
            .map(|term| {
                OrderTerm::new(
                    term.expr,
                    match term.direction {
                        SqlOrderDirection::Asc => crate::db::query::plan::OrderDirection::Asc,
                        SqlOrderDirection::Desc => crate::db::query::plan::OrderDirection::Desc,
                    },
                )
            })
            .collect(),
    })
}

// ORDER BY lowering now carries only the lowered semantic expression through
// the SQL boundary so planner ordering stays expression-first too.
fn lower_order_term(
    term: &SqlOrderTerm,
    work: &PreparationWork<'_>,
) -> Result<LoweredSqlOrderTerm, SqlLoweringError> {
    let phase = if term.field.contains_aggregate() {
        SqlExprPhase::PostAggregate
    } else {
        SqlExprPhase::Scalar
    };
    let expr = lower_sql_expr(&term.field, phase, work)?;
    let analysis = analyze_lowered_expr(&expr);

    Ok(LoweredSqlOrderTerm {
        analysis,
        expr,
        direction: term.direction,
    })
}

#[cfg(test)]
mod tests;

crate::retained::retained_fields!(LoweredSqlOrderTerm {
    Self { expr, analysis, direction: _ } => [expr, analysis],
});
