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
fn lower_order_term(term: SqlOrderTerm) -> Result<LoweredSqlOrderTerm, SqlLoweringError> {
    let phase = if term.field.contains_aggregate() {
        SqlExprPhase::PostAggregate
    } else {
        SqlExprPhase::Scalar
    };
    let expr = lower_sql_expr(&term.field, phase)?;
    let analysis = analyze_lowered_expr(&expr, None);

    Ok(LoweredSqlOrderTerm {
        analysis,
        expr,
        direction: term.direction,
    })
}

#[cfg(test)]
mod tests {
    use crate::db::sql::parser::{SqlExpr, SqlOrderDirection, SqlOrderTerm, SqlScalarFunction};

    use super::lower_order_term;

    #[test]
    fn lowered_order_term_carries_expression_analysis_for_distinct_projection_proof() {
        let lowered = lower_order_term(SqlOrderTerm {
            field: SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args: vec![SqlExpr::Field("name".to_string())],
            },
            direction: SqlOrderDirection::Asc,
        })
        .expect("order term should lower");

        assert!(
            lowered.analysis.references_only_direct_fields(&["name"]),
            "order analysis should prove derived expressions over projected direct fields",
        );
        assert!(
            !lowered.analysis.references_only_direct_fields(&["age"]),
            "order analysis should reject hidden direct fields",
        );

        let field_path = lower_order_term(SqlOrderTerm {
            field: SqlExpr::FieldPath {
                root: "profile".to_string(),
                segments: vec!["name".to_string()],
            },
            direction: SqlOrderDirection::Asc,
        })
        .expect("field-path order term should lower");

        assert!(
            !field_path
                .analysis
                .references_only_direct_fields(&["profile"]),
            "field paths must not satisfy DISTINCT direct-field projection proof",
        );
    }
}
