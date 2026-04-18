use crate::db::{
    query::{
        builder::scalar_projection::render_scalar_projection_expr_sql_label,
        intent::StructuralQuery,
        plan::{OrderSpec, OrderTerm},
    },
    sql::{
        lowering::{
            SqlLoweringError,
            expr::{SqlExprPhase, lower_sql_expr},
        },
        parser::{SqlOrderDirection, SqlOrderTerm},
    },
};

///
/// LoweredSqlOrderTerm
///
/// Lowered ORDER BY term carried after SQL expression normalization has
/// already produced one canonical planner-compatible order label.
///

#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) struct LoweredSqlOrderTerm {
    pub(in crate::db::sql::lowering) field: String,
    pub(in crate::db::sql::lowering) expr: crate::db::query::plan::expr::Expr,
    pub(in crate::db::sql::lowering) direction: SqlOrderDirection,
}

pub(in crate::db::sql::lowering) fn lower_order_terms(
    order_by: Vec<SqlOrderTerm>,
) -> Result<Vec<LoweredSqlOrderTerm>, SqlLoweringError> {
    order_by.into_iter().map(lower_order_term).collect()
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
                    term.field,
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

// ORDER BY lowering freezes one canonical order label from the shared SQL
// expression seam so SQL alias normalization no longer needs to render text and
// reparse it just to preserve semantic identity.
fn lower_order_term(term: SqlOrderTerm) -> Result<LoweredSqlOrderTerm, SqlLoweringError> {
    let phase = if term.field.contains_aggregate() {
        SqlExprPhase::PostAggregate
    } else {
        SqlExprPhase::Scalar
    };
    let expr = lower_sql_expr(&term.field, phase)?;

    Ok(LoweredSqlOrderTerm {
        field: render_scalar_projection_expr_sql_label(&expr),
        expr,
        direction: term.direction,
    })
}
