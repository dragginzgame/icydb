use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::expr::{Expr, compile_scalar_projection_expr_with_schema},
    },
    schema::SchemaInfo,
    sql::lowering::{AnalyzedLoweredExpr, LoweredExprAnalysis, SqlLoweringError},
};

// Attach one optional normalized planner-owned filter expression to an
// aggregate expression so parser/lowering support can stay on the aggregate
// semantic boundary without reopening aggregate construction at callsites.
pub(in crate::db::sql::lowering::aggregate) fn apply_aggregate_filter_expr(
    aggregate: AggregateExpr,
    filter_expr: Option<Expr>,
) -> AggregateExpr {
    match filter_expr {
        Some(filter_expr) => aggregate.with_filter_expr(filter_expr),
        None => aggregate,
    }
}

// Validate one already-analyzed model-bound scalar expression while preserving
// first unknown-field diagnostics from the recorded lowered field-root order.
pub(in crate::db::sql::lowering::aggregate) fn validate_analyzed_schema_bound_scalar_expr(
    schema: &SchemaInfo,
    analyzed: &AnalyzedLoweredExpr,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    validate_schema_bound_scalar_expr_with_analysis(
        schema,
        analyzed.expr(),
        analyzed.analysis(),
        unsupported,
    )
}

fn validate_schema_bound_scalar_expr_with_analysis(
    schema: &SchemaInfo,
    expr: &Expr,
    analysis: &LoweredExprAnalysis,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    if let Some(field) = analysis.first_unknown_field_for_schema(schema) {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if compile_scalar_projection_expr_with_schema(schema, expr).is_none() {
        return Err(unsupported());
    }

    Ok(())
}
