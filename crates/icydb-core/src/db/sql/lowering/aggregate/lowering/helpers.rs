use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::expr::{Expr, compile_scalar_projection_expr_with_schema},
    },
    schema::SchemaInfo,
    sql::lowering::{
        AnalyzedLoweredExpr, LoweredExprAnalysis, SqlLoweringError, analyze_lowered_expr,
    },
};
use crate::model::entity::EntityModel;

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

// Validate one model-bound scalar expression while preserving the first
// unknown-field diagnostic before generic expression-family fallback.
pub(in crate::db::sql::lowering::aggregate) fn validate_model_bound_scalar_expr(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    expr: &Expr,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    let analysis = analyze_lowered_expr(expr, Some(model));

    validate_model_bound_scalar_expr_with_analysis(model, schema, expr, &analysis, unsupported)
}

// Validate one already-analyzed model-bound scalar expression while preserving
// first unknown-field diagnostics from the recorded lowered field-root order.
pub(in crate::db::sql::lowering::aggregate) fn validate_analyzed_model_bound_scalar_expr(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    analyzed: &AnalyzedLoweredExpr,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    validate_model_bound_scalar_expr_with_analysis(
        model,
        schema,
        analyzed.expr(),
        analyzed.analysis(),
        unsupported,
    )
}

fn validate_model_bound_scalar_expr_with_analysis(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    expr: &Expr,
    analysis: &LoweredExprAnalysis,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    if let Some(field) = analysis.first_unknown_field_for_model(model) {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if compile_scalar_projection_expr_with_schema(schema, expr).is_none() {
        return Err(unsupported());
    }

    Ok(())
}
