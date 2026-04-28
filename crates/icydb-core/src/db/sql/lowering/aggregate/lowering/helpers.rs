use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::expr::{Expr, compile_scalar_projection_expr},
    },
    sql::lowering::{SqlLoweringError, analyze_lowered_expr},
};
use crate::model::entity::EntityModel;

// Attach one optional normalized planner-owned filter expression to an
// aggregate expression so parser/lowering support can stay on the aggregate
// identity boundary without reopening aggregate construction at callsites.
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
    expr: &Expr,
    unsupported: impl FnOnce() -> SqlLoweringError,
) -> Result<(), SqlLoweringError> {
    if let Some(field) = analyze_lowered_expr(expr, Some(model)).first_unknown_field() {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if compile_scalar_projection_expr(model, expr).is_none() {
        return Err(unsupported());
    }

    Ok(())
}
